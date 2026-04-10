import os
import json
import logging
import asyncio
import httpx
import tempfile
from typing import Optional, List, Dict, Any, AsyncGenerator
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import uvicorn
import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from anthropic import Anthropic, APIError

# Import new modules for PDF pipeline
from parsers.pdf_parser import PDFParser
from parsers.reference_extractor import ReferenceExtractor
from validators.content_validator import ContentValidator
from services import import_service as import_svc
from scrapers import scrape_runner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "building_code")
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://embedding-service:8011")
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")
BACKEND_PORT = int(os.getenv("BACKEND_PORT", 8010))
PDF_UPLOAD_DIR = os.getenv("PDF_UPLOAD_DIR", "/tmp/pdf_uploads")

# LLM Configuration
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:30b")
OLLAMA_NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX", "8192"))  # KV cache size; Ollama's default (context_length) can be huge (256K) and blow up first-load
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")  # "ollama" or "claude"

# Database connection string
DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create upload directory if it doesn't exist
os.makedirs(PDF_UPLOAD_DIR, exist_ok=True)

# Global state
db_pool = None
scheduler = None
client = None
pdf_parser = None
reference_extractor = None
content_validator = None
current_llm_provider = LLM_PROVIDER
current_llm_model = OLLAMA_MODEL
# import_svc module used directly via import_svc.import_pdf()


# Pydantic Models
class HealthResponse(BaseModel):
    status: str
    database: str
    embedding_service: str
    claude_api: str
    llm_provider: str
    ollama: Optional[Dict[str, Any]] = None


class LLMStatusResponse(BaseModel):
    provider: str
    model: str
    claude_available: bool
    ollama_available: bool
    available_models: Optional[List[str]] = None


class LLMConfigRequest(BaseModel):
    provider: str  # "ollama" or "claude"
    model: Optional[str] = None


class EmbedRequest(BaseModel):
    text: str


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[int] = None
    code_book_id: Optional[int] = None
    use_claude: bool = False  # Default to Ollama, opt-in to Claude
    model: Optional[str] = None  # Override the default model


class SearchRequest(BaseModel):
    q: str
    code_book_id: Optional[int] = None
    chapter: Optional[str] = None
    has_amendment: Optional[bool] = None


class SearchResult(BaseModel):
    id: int
    title: str
    content: str
    code_book_id: int
    chapter: str
    section_number: str


class SectionDetail(BaseModel):
    id: int
    title: str
    content: str
    code_book_id: int
    chapter: str
    section_number: str
    depth: int
    path: str
    effective_date: Optional[str]
    superseded_date: Optional[str]
    amended: bool


class SectionReferences(BaseModel):
    internal_references: List[Dict[str, Any]]
    external_references: List[Dict[str, Any]]


class ImportStatus(BaseModel):
    source_id: int
    status: str
    last_run: Optional[str]
    next_run: Optional[str]
    records_imported: int
    records_failed: int


class QuarantineItem(BaseModel):
    id: int
    source_id: int
    validation_layer: int
    error_message: str
    raw_content: str
    created_at: str
    reviewed_at: Optional[str]


class ApprovalResponse(BaseModel):
    id: int
    status: str


class GraphNode(BaseModel):
    id: int
    section_number: str
    section_title: str
    full_text: str
    depth: int
    reference_type: str


class GraphResponse(BaseModel):
    source_section_id: int
    nodes: List[GraphNode]


class HybridSearchResult(BaseModel):
    id: int
    section_number: str
    section_title: str
    full_text: str
    relevance_score: float
    search_type: str  # "vector" or "keyword"


class ImportUploadResponse(BaseModel):
    filename: str
    status: str
    import_log_id: int


class ScrapeICCRequest(BaseModel):
    code_url: str
    code_book_id: int


class ScrapeResponse(BaseModel):
    source_id: int
    status: str
    message: str


class ChatMessage(BaseModel):
    role: str
    content: str


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up Building Code System backend...")

    global db_pool, scheduler, client, pdf_parser, reference_extractor, content_validator

    try:
        # Initialize database pool
        db_pool = await asyncpg.create_pool(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            min_size=5,
            max_size=20
        )
        logger.info("Database pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise

    # Initialize Claude client
    if CLAUDE_API_KEY and CLAUDE_API_KEY != "sk-ant-CHANGEME":
        client = Anthropic(api_key=CLAUDE_API_KEY)
        logger.info("Claude API client initialized")
    else:
        logger.warning("CLAUDE_API_KEY not set, Claude features will be unavailable")

    # Initialize PDF pipeline components
    try:
        pdf_parser = PDFParser()
        reference_extractor = ReferenceExtractor()
        content_validator = ContentValidator(db_pool=db_pool)
        logger.info("PDF pipeline components initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize PDF pipeline: {e}")

    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    scheduler.start()
    logger.info("APScheduler started")

    yield

    # Shutdown
    logger.info("Shutting down...")
    if db_pool:
        await db_pool.close()
    if scheduler:
        scheduler.shutdown()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Building Code Intelligence System Backend",
    version="1.0.0",
    lifespan=lifespan
)


# Helper functions
async def check_database() -> bool:
    """Check if database is accessible"""
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return False


async def check_embedding_service() -> bool:
    """Check if embedding service is accessible"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(f"{EMBEDDING_SERVICE_URL}/health")
            return response.status_code == 200
    except Exception as e:
        logger.error(f"Embedding service check failed: {e}")
        return False


async def check_claude_api() -> bool:
    """Check if Claude API is accessible"""
    if not client or not CLAUDE_API_KEY or CLAUDE_API_KEY == "sk-ant-CHANGEME":
        return False
    try:
        response = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=10,
            messages=[{"role": "user", "content": "ping"}],
            timeout=5.0
        )
        return True
    except Exception as e:
        logger.error(f"Claude API check failed: {e}")
        return False


async def check_ollama() -> Dict[str, Any]:
    """Check if Ollama is accessible and return available models"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as http_client:
            response = await http_client.get(f"{OLLAMA_URL}/api/tags")
            if response.status_code == 200:
                data = response.json()
                models = [m.get("name") for m in data.get("models", [])]
                return {
                    "available": True,
                    "models": models,
                    "url": OLLAMA_URL
                }
    except Exception as e:
        logger.error(f"Ollama check failed: {e}")

    return {
        "available": False,
        "models": [],
        "url": OLLAMA_URL
    }


async def get_embedding(text: str) -> Optional[List[float]]:
    """Get embedding from embedding service"""
    try:
        # Generous timeout: cold model warm-up can take ~30s after a container recreate
        async with httpx.AsyncClient(timeout=120.0) as http_client:
            response = await http_client.post(
                f"{EMBEDDING_SERVICE_URL}/embed",
                json={"text": text}
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("embedding")
    except Exception as e:
        logger.error(f"Error getting embedding: {e}")
    return None


async def retry_claude_call(messages: List[Dict], max_retries: int = 3,
                           exponential_base: float = 2.0) -> Optional[str]:
    """Call Claude API with retry logic and exponential backoff"""
    if not client:
        return None

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                messages=messages
            )
            return response.content[0].text
        except APIError as e:
            if attempt < max_retries - 1:
                wait_time = (exponential_base ** attempt)
                logger.warning(f"Claude API call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Claude API call failed after {max_retries} attempts: {e}")
                return None
        except Exception as e:
            logger.error(f"Unexpected error in Claude call: {e}")
            return None

    return None


async def call_ollama_stream(messages: List[Dict], model: str, system_prompt: str = "") -> AsyncGenerator[str, None]:
    """Stream responses from Ollama API"""
    # Prepend system prompt to first message if provided
    formatted_messages = messages.copy()
    if system_prompt and formatted_messages:
        if formatted_messages[0]["role"] == "user":
            formatted_messages[0]["content"] = f"{system_prompt}\n\n{formatted_messages[0]['content']}"

    for attempt in range(3):
        try:
            # Long timeout for first-time model loading (can take 2-3 min)
            timeout = httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0)
            async with httpx.AsyncClient(timeout=timeout) as http_client:
                async with http_client.stream(
                    "POST",
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": model,
                        "messages": formatted_messages,
                        "stream": True,
                        "options": {"num_ctx": OLLAMA_NUM_CTX},
                    }
                ) as response:
                    if response.status_code == 200:
                        async for line in response.aiter_lines():
                            if line:
                                try:
                                    chunk = json.loads(line)
                                    if "message" in chunk and "content" in chunk["message"]:
                                        yield chunk["message"]["content"]
                                except json.JSONDecodeError:
                                    logger.warning(f"Failed to parse Ollama response: {line}")
                        return
                    else:
                        logger.error(f"Ollama API error: {response.status_code}")
                        raise Exception(f"Ollama returned {response.status_code}")
        except Exception as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                logger.warning(f"Ollama call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Ollama call failed after 3 attempts: {e}")
                yield f"Error: Failed to get response from Ollama after 3 attempts"


async def call_claude_stream(messages: List[Dict], system_prompt: str = "") -> AsyncGenerator[str, None]:
    """Stream responses from Claude API"""
    if not client:
        yield "Error: Claude API not configured"
        return

    for attempt in range(3):
        try:
            response = client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                system=system_prompt if system_prompt else None,
                messages=messages,
                stream=True
            )

            for event in response:
                if hasattr(event, 'delta') and hasattr(event.delta, 'text'):
                    yield event.delta.text
            return
        except APIError as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                logger.warning(f"Claude API call failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Claude API call failed after 3 attempts: {e}")
                yield f"Error: Failed to get response from Claude after 3 attempts"
        except Exception as e:
            logger.error(f"Unexpected error in Claude stream: {e}")
            yield f"Error: {str(e)}"


async def call_llm(messages: List[Dict], system_prompt: str = "", stream: bool = True, use_claude: bool = False, model: Optional[str] = None) -> AsyncGenerator[str, None]:
    """
    Unified LLM call function that supports both Ollama and Claude API.
    If use_claude=True and Claude is configured, use Claude API.
    Otherwise, use Ollama API (default).
    """
    # Determine which provider to use
    use_claude_api = use_claude and client and CLAUDE_API_KEY and CLAUDE_API_KEY != "sk-ant-CHANGEME"

    if use_claude_api:
        logger.info("Using Claude API")
        async for chunk in call_claude_stream(messages, system_prompt):
            yield chunk
    else:
        logger.info(f"Using Ollama API with model {model or OLLAMA_MODEL}")
        selected_model = model or OLLAMA_MODEL
        async for chunk in call_ollama_stream(messages, selected_model, system_prompt):
            yield chunk


# API Endpoints
@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    db_ok = await check_database()
    embed_ok = await check_embedding_service()
    claude_ok = await check_claude_api()
    ollama_status = await check_ollama()

    return {
        "status": "healthy" if all([db_ok, embed_ok, ollama_status.get("available")]) else "degraded",
        "database": "ok" if db_ok else "error",
        "embedding_service": "ok" if embed_ok else "error",
        "claude_api": "ok" if claude_ok else "not configured",
        "llm_provider": current_llm_provider,
        "ollama": ollama_status
    }


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Chat endpoint with RAG pipeline and streaming response"""
    try:
        # Get embedding for the user's question
        question_embedding = await get_embedding(request.message)
        if not question_embedding:
            raise HTTPException(status_code=500, detail="Failed to embed question")

        # Run hybrid search (vector + full-text)
        # asyncpg needs vector as string for ::vector cast
        embedding_str = "[" + ",".join(str(x) for x in question_embedding) + "]"
        async with db_pool.acquire() as conn:
            # Vector similarity search
            vector_results = await conn.fetch(
                """SELECT id, section_number, section_title, full_text,
                          1 - (embedding <=> $1::vector) as similarity
                   FROM code_sections
                   WHERE code_book_id = $2
                   AND 1 - (embedding <=> $1::vector) > 0.5
                   ORDER BY similarity DESC
                   LIMIT 5""",
                embedding_str, request.code_book_id or 1
            )

            # Full-text search
            tsquery = " | ".join(request.message.split())
            keyword_results = await conn.fetch(
                """SELECT id, section_number, section_title, full_text,
                          ts_rank(to_tsvector('english', full_text),
                                  to_tsquery('english', $1)) as rank
                   FROM code_sections
                   WHERE code_book_id = $2
                   AND to_tsvector('english', full_text) @@ to_tsquery('english', $1)
                   ORDER BY rank DESC
                   LIMIT 5""",
                tsquery, request.code_book_id or 1
            )

            # Deduplicate and combine results
            seen_ids = set()
            all_results = []

            for row in vector_results:
                if row['id'] not in seen_ids:
                    all_results.append({
                        'id': row['id'],
                        'section_number': row['section_number'],
                        'section_title': row['section_title'],
                        'full_text': row['full_text'],
                        'score': row['similarity'],
                        'type': 'vector'
                    })
                    seen_ids.add(row['id'])

            for row in keyword_results:
                if row['id'] not in seen_ids:
                    all_results.append({
                        'id': row['id'],
                        'section_number': row['section_number'],
                        'section_title': row['section_title'],
                        'full_text': row['full_text'],
                        'score': row['rank'] or 0.0,
                        'type': 'keyword'
                    })
                    seen_ids.add(row['id'])

            # For top 5 results, get 2-hop reference traversal
            context_sections = []
            for result in all_results[:5]:
                section_id = result['id']
                context_sections.append(result)

                # Get 2-hop references
                ref_results = await conn.fetch(
                    """WITH RECURSIVE ref_chain AS (
                        SELECT cr.target_section_id, cr.reference_type, cr.reference_text, 1 as depth
                        FROM code_references cr
                        WHERE cr.source_section_id = $1 AND cr.target_section_id IS NOT NULL
                        UNION ALL
                        SELECT cr.target_section_id, cr.reference_type, cr.reference_text, rc.depth + 1
                        FROM code_references cr
                        JOIN ref_chain rc ON cr.source_section_id = rc.target_section_id
                        WHERE rc.depth < 2 AND cr.target_section_id IS NOT NULL
                    )
                    SELECT DISTINCT cs.id, cs.section_number, cs.section_title, cs.full_text, rc.depth
                    FROM ref_chain rc
                    JOIN code_sections cs ON cs.id = rc.target_section_id
                    LIMIT 10""",
                    section_id
                )

                for ref_row in ref_results:
                    if ref_row['id'] not in seen_ids:
                        context_sections.append({
                            'id': ref_row['id'],
                            'section_number': ref_row['section_number'],
                            'section_title': ref_row['section_title'],
                            'full_text': ref_row['full_text'],
                            'score': 0.0,
                            'type': 'reference'
                        })
                        seen_ids.add(ref_row['id'])

        # Build context window
        context_text = "RELEVANT CODE SECTIONS:\n\n"
        for section in context_sections:
            context_text += f"Section {section['section_number']}: {section['section_title']}\n"
            context_text += f"{section['full_text'][:500]}...\n\n"

        # System prompt for building code expert
        system_prompt = """You are a building code expert assistant. Answer using ONLY the provided code sections.
Always cite section numbers in your response. If a section references another section or standard, mention that reference.
If a California amendment applies, note it explicitly with the agency tag (e.g., [HCD], [SFM]).
If you're not sure or the provided sections don't contain the answer, say so — do not fabricate code references."""

        # Prepare messages for the LLM
        messages = [
            {"role": "user", "content": f"Question: {request.message}\n\n{context_text}"}
        ]

        async def generate():
            try:
                response_text = ""
                async for chunk in call_llm(
                    messages=messages,
                    system_prompt=system_prompt,
                    stream=True,
                    use_claude=request.use_claude,
                    model=request.model
                ):
                    response_text += chunk
                    yield chunk

                # Save chat message to database
                if response_text:
                    try:
                        async with db_pool.acquire() as conn:
                            # Get or create session
                            session_id = request.session_id
                            if session_id:
                                session_id = int(session_id)
                            else:
                                row = await conn.fetchrow(
                                    """INSERT INTO chat_sessions (title, created_at)
                                       VALUES ($1, $2) RETURNING id""",
                                    request.message[:100],
                                    datetime.utcnow()
                                )
                                session_id = row['id']

                            await conn.execute(
                                """INSERT INTO chat_messages (session_id, role, content, created_at)
                                   VALUES ($1, $2, $3, $4)""",
                                session_id, "user", request.message, datetime.utcnow()
                            )
                            await conn.execute(
                                """INSERT INTO chat_messages (session_id, role, content, created_at)
                                   VALUES ($1, $2, $3, $4)""",
                                session_id, "assistant", response_text, datetime.utcnow()
                            )
                    except Exception as save_err:
                        logger.warning(f"Failed to save chat history: {save_err}")
            except Exception as e:
                logger.error(f"Chat error: {e}")
                yield f"Error: {str(e)}"

        return StreamingResponse(generate(), media_type="text/event-stream")

    except Exception as e:
        logger.error(f"Chat pipeline error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sections/search")
async def search_sections(
    q: str = Query(...),
    code_book_id: Optional[int] = None,
    chapter: Optional[str] = None,
    has_amendment: Optional[bool] = None
) -> List[SearchResult]:
    """Search sections"""
    try:
        async with db_pool.acquire() as conn:
            query = "SELECT id, section_title, full_text, code_book_id, chapter, section_number FROM code_sections WHERE 1=1"
            params = []
            
            if q:
                query += " AND (section_title ILIKE $%d OR full_text ILIKE $%d)" % (len(params) + 1, len(params) + 2)
                params.extend([f"%{q}%", f"%{q}%"])
            
            if code_book_id:
                query += f" AND code_book_id = ${len(params) + 1}"
                params.append(code_book_id)
            
            if chapter:
                query += f" AND chapter = ${len(params) + 1}"
                params.append(chapter)
            
            query += " LIMIT 50"
            
            rows = await conn.fetch(query, *params)
            
            return [
                SearchResult(
                    id=row['id'],
                    title=row['section_title'] or '',
                    content=row['full_text'][:500] if row['full_text'] else '',
                    code_book_id=row['code_book_id'],
                    chapter=row['chapter'] or '',
                    section_number=row['section_number']
                )
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sections/hybrid-search")
async def hybrid_search(
    q: str = Query(...),
    code_book_id: Optional[int] = None,
    limit: int = Query(10, ge=1, le=100)
) -> List[HybridSearchResult]:
    """Hybrid search combining vector similarity and full-text search"""
    try:
        query_embedding = await get_embedding(q)
        if not query_embedding:
            raise HTTPException(status_code=500, detail="Failed to embed query")

        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
        async with db_pool.acquire() as conn:
            vector_results = await conn.fetch(
                """SELECT id, section_number, section_title, full_text,
                          1 - (embedding <=> $1::vector) as similarity
                   FROM code_sections
                   WHERE code_book_id = COALESCE($2, code_book_id)
                   AND embedding IS NOT NULL
                   ORDER BY embedding <=> $1::vector
                   LIMIT $3""",
                embedding_str, code_book_id, limit
            )

            tsquery = " | ".join(q.split())
            keyword_results = await conn.fetch(
                """SELECT id, section_number, section_title, full_text,
                          ts_rank(to_tsvector('english', full_text),
                                  to_tsquery('english', $1)) as rank
                   FROM code_sections
                   WHERE code_book_id = COALESCE($2, code_book_id)
                   AND to_tsvector('english', full_text) @@ to_tsquery('english', $1)
                   ORDER BY rank DESC
                   LIMIT $3""",
                tsquery, code_book_id, limit
            )

            seen_ids = {}
            results = []
            for row in vector_results:
                if row['id'] not in seen_ids:
                    results.append(HybridSearchResult(
                        id=row['id'],
                        section_number=row['section_number'],
                        section_title=row['section_title'] or '',
                        full_text=(row['full_text'] or '')[:500],
                        relevance_score=float(row['similarity']) if row['similarity'] else 0.0,
                        search_type="vector"
                    ))
                    seen_ids[row['id']] = True

            for row in keyword_results:
                if row['id'] not in seen_ids:
                    results.append(HybridSearchResult(
                        id=row['id'],
                        section_number=row['section_number'],
                        section_title=row['section_title'] or '',
                        full_text=(row['full_text'] or '')[:500],
                        relevance_score=float(row['rank'] or 0.0),
                        search_type="keyword"
                    ))
                    seen_ids[row['id']] = True

            return results[:limit]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Hybrid search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sections/{section_id}", response_model=SectionDetail)
async def get_section(section_id: int):
    """Get section details"""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT id, section_title, full_text, code_book_id, chapter, section_number,
                          depth, path, effective_date, superseded_date, has_ca_amendment
                   FROM code_sections WHERE id = $1""",
                section_id
            )

            if not row:
                raise HTTPException(status_code=404, detail="Section not found")

            return SectionDetail(
                id=row['id'],
                title=row['section_title'] or '',
                content=row['full_text'] or '',
                code_book_id=row['code_book_id'],
                chapter=row['chapter'] or '',
                section_number=row['section_number'],
                depth=row['depth'] or 0,
                path=row['path'] or '',
                effective_date=str(row['effective_date']) if row['effective_date'] else None,
                superseded_date=str(row['superseded_date']) if row['superseded_date'] else None,
                amended=row['has_ca_amendment'] or False
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching section: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sections/{section_id}/references", response_model=SectionReferences)
async def get_section_references(section_id: int):
    """Get section references"""
    try:
        async with db_pool.acquire() as conn:
            internal_refs = await conn.fetch(
                "SELECT target_section_id, reference_type, reference_text FROM code_references WHERE source_section_id = $1 AND target_section_id IS NOT NULL",
                section_id
            )

            external_refs = await conn.fetch(
                """SELECT es.standard_id, es.title, cr.external_standard_id
                   FROM code_references cr
                   JOIN external_standards es ON cr.external_standard_id = es.id
                   WHERE cr.source_section_id = $1""",
                section_id
            )

            return SectionReferences(
                internal_references=[dict(r) for r in internal_refs],
                external_references=[dict(r) for r in external_refs]
            )
    except Exception as e:
        logger.error(f"Error fetching references: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sections/{section_id}/graph", response_model=GraphResponse)
async def get_section_graph(section_id: int):
    """Get 2-hop reference traversal graph for a section"""
    try:
        async with db_pool.acquire() as conn:
            # Use recursive CTE to get 2-hop references
            rows = await conn.fetch(
                """WITH RECURSIVE ref_chain AS (
                    SELECT cr.target_section_id, cr.reference_type, cr.reference_text, 1 as depth
                    FROM code_references cr
                    WHERE cr.source_section_id = $1 AND cr.target_section_id IS NOT NULL
                    UNION ALL
                    SELECT cr.target_section_id, cr.reference_type, cr.reference_text, rc.depth + 1
                    FROM code_references cr
                    JOIN ref_chain rc ON cr.source_section_id = rc.target_section_id
                    WHERE rc.depth < 2 AND cr.target_section_id IS NOT NULL
                )
                SELECT DISTINCT cs.id, cs.section_number, cs.section_title, cs.full_text, rc.depth, rc.reference_type
                FROM ref_chain rc
                JOIN code_sections cs ON cs.id = rc.target_section_id""",
                section_id
            )

            nodes = [
                GraphNode(
                    id=row['id'],
                    section_number=row['section_number'],
                    section_title=row['section_title'],
                    full_text=row['full_text'],
                    depth=row['depth'],
                    reference_type=row['reference_type']
                )
                for row in rows
            ]

            return GraphResponse(source_section_id=section_id, nodes=nodes)
    except Exception as e:
        logger.error(f"Error fetching section graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/import/upload")
async def upload_pdf(
    file: UploadFile = File(...),
    code_book_id: int = Query(1, description="Code book ID to import into")
) -> ImportUploadResponse:
    """Handle PDF upload and start import pipeline"""
    try:
        if not pdf_parser:
            raise HTTPException(status_code=503, detail="PDF pipeline not available")

        # Read file contents
        contents = await file.read()

        # Save to temp directory
        temp_file_path = os.path.join(PDF_UPLOAD_DIR, file.filename or "upload.pdf")
        with open(temp_file_path, "wb") as f:
            f.write(contents)

        # Create import_logs record with 'running' status
        async with db_pool.acquire() as conn:
            import_log_id = await conn.fetchval(
                """INSERT INTO import_logs (source_id, status, imported_at)
                   VALUES ($1, $2, $3)
                   RETURNING id""",
                1, "running", datetime.utcnow()
            )

        # Start import in background
        asyncio.create_task(
            import_svc.import_pdf(
                file_path=temp_file_path,
                code_book_id=code_book_id,
                db_pool=db_pool,
                embedding_url=EMBEDDING_SERVICE_URL,
                source_id=1
            )
        )

        logger.info(f"PDF upload started: {file.filename} (import_log_id={import_log_id})")
        return ImportUploadResponse(
            filename=file.filename or "upload.pdf",
            status="processing",
            import_log_id=import_log_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/import/trigger/{source_id}")
async def trigger_import(source_id: int):
    """Trigger import for a source (PDF or web scrape)"""
    try:
        # Look up the import source
        async with db_pool.acquire() as conn:
            source = await conn.fetchrow(
                """SELECT id, source_type, source_url, code_book_id
                   FROM import_sources
                   WHERE id = $1""",
                source_id
            )

            if not source:
                raise HTTPException(status_code=404, detail=f"Source {source_id} not found")

            source_type = source['source_type']
            source_url = source['source_url']
            code_book_id = source['code_book_id']

        # Route based on source type
        if source_type == 'pdf_parse':
            raise HTTPException(
                status_code=400,
                detail="PDF imports must use the /api/import/upload endpoint"
            )

        elif source_type == 'web_scrape' and source_url and 'iccsafe.org' in source_url:
            # Trigger ICC scraper in background
            asyncio.create_task(
                scrape_runner.run_icc_import(
                    code_url=source_url,
                    code_book_id=code_book_id,
                    db_pool=db_pool,
                    embedding_url=EMBEDDING_SERVICE_URL,
                    source_id=source_id,
                )
            )
            logger.info(f"ICC scrape triggered for source {source_id}")
            return {"source_id": source_id, "status": "triggered", "type": "icc_scrape"}

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported source type: {source_type} or URL: {source_url}"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Trigger error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/import/scrape-icc")
async def scrape_icc_direct(request: ScrapeICCRequest) -> ScrapeResponse:
    """Trigger ICC scrape directly without needing an import_source record.

    Args:
        request: ScrapeICCRequest with code_url and code_book_id

    Returns:
        ScrapeResponse with source_id and status
    """
    try:
        # Create import source record
        async with db_pool.acquire() as conn:
            source_id = await conn.fetchval(
                '''INSERT INTO import_sources
                   (source_name, source_type, source_url, code_book_id, status)
                   VALUES ($1, $2, $3, $4, $5)
                   RETURNING id''',
                f"ICC {request.code_url}",
                'web_scrape',
                request.code_url,
                request.code_book_id,
                'queued',
            )

        # Trigger scraper in background
        asyncio.create_task(
            scrape_runner.run_icc_import(
                code_url=request.code_url,
                code_book_id=request.code_book_id,
                db_pool=db_pool,
                embedding_url=EMBEDDING_SERVICE_URL,
                source_id=source_id,
            )
        )

        logger.info(f"ICC scrape started directly: {request.code_url} (source_id={source_id})")
        return ScrapeResponse(
            source_id=source_id,
            status="processing",
            message=f"Scraping {request.code_url}",
        )

    except Exception as e:
        logger.error(f"ICC scrape error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/import/status")
async def get_import_status():
    """Get import sources status"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, source_name, source_url, source_type, status,
                          last_crawled, sections_imported, next_crawl_at
                   FROM import_sources
                   ORDER BY last_crawled DESC NULLS LAST"""
            )

            return [
                {
                    "id": row['id'],
                    "source_name": row['source_name'],
                    "source_url": row['source_url'],
                    "source_type": row['source_type'],
                    "status": row['status'],
                    "last_crawled": row['last_crawled'].isoformat() if row['last_crawled'] else None,
                    "sections_imported": row['sections_imported'],
                    "next_crawl_at": row['next_crawl_at'].isoformat() if row['next_crawl_at'] else None
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Import status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/quarantine")
async def get_quarantine() -> List[QuarantineItem]:
    """Get quarantined items"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, source_id, validation_layer, error_message, raw_content, created_at, reviewed_at
                   FROM content_quarantine ORDER BY created_at DESC LIMIT 50"""
            )
            
            return [
                QuarantineItem(
                    id=row['id'],
                    source_id=row['source_id'],
                    validation_layer=row['validation_layer'],
                    error_message=row['error_message'],
                    raw_content=row['raw_content'][:200],
                    created_at=row['created_at'].isoformat(),
                    reviewed_at=row['reviewed_at'].isoformat() if row['reviewed_at'] else None
                )
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Quarantine error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/quarantine/{item_id}/approve", response_model=ApprovalResponse)
async def approve_quarantine(item_id: int):
    """Approve quarantined item"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE content_quarantine SET reviewed_at = $1 WHERE id = $2",
                datetime.utcnow(), item_id
            )
        return ApprovalResponse(id=item_id, status="approved")
    except Exception as e:
        logger.error(f"Approval error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/quarantine/{item_id}/reject", response_model=ApprovalResponse)
async def reject_quarantine(item_id: int):
    """Reject quarantined item"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM content_quarantine WHERE id = $1",
                item_id
            )
        return ApprovalResponse(id=item_id, status="rejected")
    except Exception as e:
        logger.error(f"Rejection error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def get_stats():
    """Get system statistics for the dashboard"""
    try:
        async with db_pool.acquire() as conn:
            sections = await conn.fetchval("SELECT count(*) FROM code_sections")
            references = await conn.fetchval("SELECT count(*) FROM code_references")
            quarantined = await conn.fetchval(
                "SELECT count(*) FROM content_quarantine WHERE reviewed_at IS NULL"
            )
            code_books = await conn.fetchval("SELECT count(*) FROM code_books")
            standards = await conn.fetchval("SELECT count(*) FROM external_standards")
            topics = await conn.fetchval("SELECT count(*) FROM topics")
            return {
                "total_sections": sections,
                "total_references": references,
                "pending_quarantine": quarantined,
                "code_books": code_books,
                "external_standards": standards,
                "topics": topics
            }
    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-books")
async def list_code_books():
    """List all code books for dropdowns"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, code_name, abbreviation, part_number, status
                   FROM code_books ORDER BY part_number"""
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Code books error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/llm/status", response_model=LLMStatusResponse)
async def get_llm_status():
    """Get LLM provider status and configuration"""
    try:
        claude_available = await check_claude_api()
        ollama_status = await check_ollama()

        return {
            "provider": current_llm_provider,
            "model": current_llm_model,
            "claude_available": claude_available,
            "ollama_available": ollama_status.get("available", False),
            "available_models": ollama_status.get("models", [])
        }
    except Exception as e:
        logger.error(f"LLM status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/llm/config")
async def set_llm_config(request: LLMConfigRequest):
    """Configure LLM provider and model at runtime"""
    global current_llm_provider, current_llm_model

    try:
        # Validate provider
        if request.provider not in ["ollama", "claude"]:
            raise HTTPException(status_code=400, detail="Provider must be 'ollama' or 'claude'")

        # Validate Claude is available if switching to it
        if request.provider == "claude":
            if not client or not CLAUDE_API_KEY or CLAUDE_API_KEY == "sk-ant-CHANGEME":
                raise HTTPException(status_code=400, detail="Claude API not configured")

        # If Ollama provider, validate model if provided
        if request.provider == "ollama" and request.model:
            ollama_status = await check_ollama()
            if request.model not in ollama_status.get("models", []):
                raise HTTPException(status_code=400, detail=f"Model {request.model} not available in Ollama")

        # Update global configuration
        current_llm_provider = request.provider
        if request.model:
            current_llm_model = request.model
        elif request.provider == "ollama":
            current_llm_model = OLLAMA_MODEL  # Reset to default

        logger.info(f"LLM configuration updated: provider={current_llm_provider}, model={current_llm_model}")

        return {
            "provider": current_llm_provider,
            "model": current_llm_model,
            "status": "updated"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"LLM config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=BACKEND_PORT)
