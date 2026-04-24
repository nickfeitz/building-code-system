import os
import json
import hashlib
import logging
import asyncio
import httpx
import tempfile
import threading
from collections import OrderedDict
from typing import Optional, List, Dict, Any, AsyncGenerator
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Query, Request
from fastapi.responses import StreamingResponse, Response
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


def _spawn_import_task(coro, import_log_id: int, context: str):
    """Kick off an import coroutine with a done-callback.

    A plain ``asyncio.create_task`` will swallow an unhandled exception
    into the task object — the task dies, the exception ends up only in
    asyncio's default handler, and ``import_logs`` stays stuck on
    ``status='processing'`` forever. That produced silent failures until
    an operator noticed a row that never completed.

    This wrapper attaches a callback that:
      * logs the traceback with the import_log_id + context
      * flips the row to ``status='error', phase='crashed'`` with the
        exception message so the UI shows a red pill instead of a
        spinner that never ends.
    """
    task = asyncio.create_task(coro)

    def _on_done(t: asyncio.Task):
        if t.cancelled():
            logger.warning("import task cancelled: log_id=%s (%s)", import_log_id, context)
            asyncio.create_task(_mark_import_crashed(
                import_log_id, "Import task cancelled", "cancelled",
            ))
            return
        exc = t.exception()
        if exc is None:
            return
        logger.error(
            "import task crashed: log_id=%s (%s): %r",
            import_log_id, context, exc, exc_info=exc,
        )
        asyncio.create_task(_mark_import_crashed(
            import_log_id, f"{type(exc).__name__}: {exc}", "crashed",
        ))

    task.add_done_callback(_on_done)
    return task


async def _mark_import_crashed(import_log_id: int, message: str, phase: str):
    """Flip an import_logs row to status='error' after its worker died."""
    try:
        async with db_pool.acquire() as conn:
            # Only overwrite if the row is still in a non-terminal state —
            # if the worker already updated itself to completed/error, don't
            # stomp its reason with our generic "crashed" label.
            await conn.execute(
                """UPDATE import_logs
                      SET status = 'error', phase = $1,
                          error_message = COALESCE(error_message, $2),
                          stage_detail = 'Import worker exited unexpectedly',
                          completed_at = NOW(), updated_at = NOW()
                    WHERE id = $3
                      AND status NOT IN ('completed', 'error')""",
                phase, message[:500], import_log_id,
            )
    except Exception:
        logger.exception("failed to mark import_log %s as crashed", import_log_id)


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
    # Canonical naming shared with HybridSearchResult + the catalog payload
    # + the frontend's SectionSearchHit type. Previously this model used
    # `title`/`content` which silently caused the Browser panel to show
    # empty hit titles/bodies.
    section_title: str
    full_text: str
    code_book_id: int
    chapter: str
    section_number: str


class SectionDetail(BaseModel):
    id: int
    section_title: str
    full_text: str
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

    # Apply additive SQL migrations in backend/migrations/. Each file uses
    # IF NOT EXISTS guards so re-running is a no-op; run in lexical order so
    # later migrations can depend on earlier ones.
    try:
        mig_dir = os.path.join(os.path.dirname(__file__), "migrations")
        if os.path.isdir(mig_dir):
            files = sorted(f for f in os.listdir(mig_dir) if f.endswith(".sql"))
            async with db_pool.acquire() as conn:
                for fname in files:
                    path = os.path.join(mig_dir, fname)
                    try:
                        with open(path, "r", encoding="utf-8") as fh:
                            sql = fh.read()
                        if sql.strip():
                            await conn.execute(sql)
                            logger.info("Applied migration %s", fname)
                    except Exception as e:
                        logger.warning("Migration %s failed: %s", fname, e)
    except Exception as e:
        logger.warning("Migration runner error: %s", e)

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
            # Vector similarity search — exclude superseded versions so
            # replies don't cite outdated rows after a re-upload.
            vector_results = await conn.fetch(
                """SELECT id, section_number, section_title, full_text,
                          1 - (embedding <=> $1::vector) as similarity
                   FROM code_sections
                   WHERE code_book_id = $2
                   AND superseded_date IS NULL
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
                   AND superseded_date IS NULL
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
            # Default to current (non-superseded) versions only.
            query = ("SELECT id, section_title, full_text, code_book_id, chapter, "
                     "section_number FROM code_sections WHERE superseded_date IS NULL")
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
                    section_title=row['section_title'] or '',
                    full_text=row['full_text'][:500] if row['full_text'] else '',
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
                   AND superseded_date IS NULL
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
                   AND superseded_date IS NULL
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
                section_title=row['section_title'] or '',
                full_text=row['full_text'] or '',
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
    code_book_id: int = Query(1, description="Code book ID to import into"),
) -> ImportUploadResponse:
    """Handle PDF upload: stream to disk, persist to Postgres, kick off parsing.

    Streaming avoids loading the entire file into process memory (important
    for multi-hundred-MB code PDFs). The bytes are then stored in the
    code_book_pdfs table keyed by (code_book_id, sha256) so the original
    survives container restarts and can be retrieved via the download
    endpoint. If the same (book, sha256) is uploaded again, we reuse the
    existing row instead of duplicating.
    """
    try:
        if not pdf_parser:
            raise HTTPException(status_code=503, detail="PDF pipeline not available")

        # Verify the target book exists (and grab its name for logging)
        async with db_pool.acquire() as conn:
            book = await conn.fetchrow(
                "SELECT id, code_name FROM code_books WHERE id = $1",
                code_book_id,
            )
        if not book:
            raise HTTPException(
                status_code=404,
                detail=f"code_book {code_book_id} not found",
            )

        safe_name = os.path.basename(file.filename or "upload.pdf")
        temp_file_path = os.path.join(PDF_UPLOAD_DIR, safe_name)

        # Stream request body to disk in 1 MB chunks while computing SHA-256.
        # Avoids loading 1 GB into process memory.
        hasher = hashlib.sha256()
        size = 0
        CHUNK = 1024 * 1024
        with open(temp_file_path, "wb") as out:
            while True:
                chunk = await file.read(CHUNK)
                if not chunk:
                    break
                out.write(chunk)
                hasher.update(chunk)
                size += len(chunk)
        sha256 = hasher.hexdigest()

        logger.info(
            "Uploaded PDF on disk: name=%s size=%.1f MB sha256=%s book_id=%s",
            safe_name, size / (1024 * 1024), sha256, code_book_id,
        )

        # Persist the bytes to Postgres (dedup on (code_book_id, sha256)).
        # Read once more from disk into memory for the bytea param; this is
        # a single short-lived spike per upload.
        with open(temp_file_path, "rb") as f:
            pdf_bytes = f.read()

        async with db_pool.acquire() as conn:
            # Dedup guard 1: same (book, sha256) ⇒ refuse with 409.
            # The UX contract is "identical bytes = nothing to do"; the
            # bytes are already persisted and the sections are already
            # indexed. Return the existing pdf_id + counts so the client
            # can surface a friendly "already uploaded" message with a
            # link to Review.
            existing = await conn.fetchrow(
                """SELECT id, filename, uploaded_at
                     FROM code_book_pdfs
                    WHERE code_book_id = $1 AND sha256 = $2""",
                code_book_id, sha256,
            )
            if existing:
                current_sections = await conn.fetchval(
                    """SELECT count(*) FROM code_sections
                        WHERE code_book_id = $1 AND superseded_date IS NULL""",
                    code_book_id,
                )
                # Discard the temp file on disk — we're not going to parse.
                try:
                    os.remove(temp_file_path)
                except OSError:
                    pass
                raise HTTPException(
                    status_code=409,
                    detail={
                        "status": "duplicate",
                        "message": "Identical PDF already uploaded for this book.",
                        "pdf_id": existing["id"],
                        "filename": existing["filename"],
                        "uploaded_at": existing["uploaded_at"].isoformat()
                                      if existing["uploaded_at"] else None,
                        "current_sections": current_sections,
                        "code_book_id": code_book_id,
                        "code_name": book["code_name"],
                    },
                )

            # New bytes: this is treated as a new version of the book.
            # Mark any currently-live sections superseded so search/stats
            # stop returning them, then insert the fresh PDF + run parse.
            status_tag = await conn.execute(
                """UPDATE code_sections
                      SET superseded_date = CURRENT_DATE, updated_at = NOW()
                    WHERE code_book_id = $1 AND superseded_date IS NULL""",
                code_book_id,
            )
            # asyncpg returns "UPDATE N" for execute() of an UPDATE.
            try:
                superseded_n = int(status_tag.split()[-1])
            except (ValueError, IndexError):
                superseded_n = 0
            if superseded_n:
                logger.info(
                    "Superseded %s prior sections for code_book %s before new ingest",
                    superseded_n, code_book_id,
                )

            pdf_id = await conn.fetchval(
                """
                INSERT INTO code_book_pdfs
                    (code_book_id, filename, mime_type, sha256, size_bytes, content)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (code_book_id, sha256)
                    DO UPDATE SET uploaded_at = NOW(),
                                  filename = EXCLUDED.filename,
                                  size_bytes = EXCLUDED.size_bytes
                RETURNING id
                """,
                code_book_id, safe_name, file.content_type or "application/pdf",
                sha256, size, pdf_bytes,
            )

            # Dedicated import_sources row for PDF uploads per book.
            # Find-or-create so many uploads of the same book share a source.
            src = await conn.fetchrow(
                """SELECT id FROM import_sources
                   WHERE code_book_id = $1 AND source_type = 'pdf_parse'
                   ORDER BY id DESC LIMIT 1""",
                code_book_id,
            )
            if src:
                source_id = src["id"]
            else:
                source_id = await conn.fetchval(
                    """INSERT INTO import_sources
                           (source_name, source_type, code_book_id, status)
                       VALUES ($1, 'pdf_parse', $2, 'pending')
                       RETURNING id""",
                    f"PDF uploads: {book['code_name']}",
                    code_book_id,
                )

            # One canonical import_logs row that the background parser
            # will keep updating as it progresses.
            import_log_id = await conn.fetchval(
                """INSERT INTO import_logs
                       (source_id, code_book_id, pdf_id, filename,
                        status, phase, imported_at, updated_at)
                   VALUES ($1, $2, $3, $4, 'processing', 'queued', NOW(), NOW())
                   RETURNING id""",
                source_id, code_book_id, pdf_id, safe_name,
            )

        # Drop the local buffer before kicking off the parser so we don't
        # hold the full PDF in memory through the async task.
        del pdf_bytes

        # Start parse+index in background; pass the log_id so progress
        # lands on the same row the upload endpoint just created.
        _spawn_import_task(
            import_svc.import_pdf(
                file_path=temp_file_path,
                code_book_id=code_book_id,
                db_pool=db_pool,
                embedding_url=EMBEDDING_SERVICE_URL,
                source_id=source_id,
                import_log_id=import_log_id,
                source_pdf_id=pdf_id,
                ollama_url=OLLAMA_URL,
                ollama_model=OLLAMA_MODEL,
                ollama_num_ctx=OLLAMA_NUM_CTX,
            ),
            import_log_id=import_log_id,
            context=f"upload pdf_id={pdf_id} book={code_book_id}",
        )

        logger.info(
            "PDF upload persisted: pdf_id=%s book=%s import_log_id=%s size=%.1fMB",
            pdf_id, book["code_name"], import_log_id, size / (1024 * 1024),
        )
        return ImportUploadResponse(
            filename=safe_name,
            status="processing",
            import_log_id=import_log_id,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-books/{book_id}/pdfs")
async def list_book_pdfs(book_id: int):
    """List stored PDFs for a code_book (metadata only; no binary)."""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id, filename, mime_type, sha256, size_bytes,
                          uploaded_at, notes
                   FROM code_book_pdfs
                   WHERE code_book_id = $1
                   ORDER BY uploaded_at DESC""",
                book_id,
            )
            return [
                {
                    "id": r["id"],
                    "filename": r["filename"],
                    "mime_type": r["mime_type"],
                    "sha256": r["sha256"],
                    "size_bytes": r["size_bytes"],
                    "uploaded_at": r["uploaded_at"].isoformat() if r["uploaded_at"] else None,
                    "notes": r["notes"],
                }
                for r in rows
            ]
    except Exception as e:
        logger.error(f"List PDFs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-book-pdfs/{pdf_id}/content")
async def download_book_pdf(pdf_id: int):
    """Stream back a stored PDF by id."""
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT filename, mime_type, content
                   FROM code_book_pdfs WHERE id = $1""",
                pdf_id,
            )
        if not row:
            raise HTTPException(status_code=404, detail="PDF not found")
        return Response(
            content=bytes(row["content"]),
            media_type=row["mime_type"] or "application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{row["filename"] or f"{pdf_id}.pdf"}"',
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download PDF error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/code-book-pdfs/{pdf_id}/reindex")
async def reindex_pdf(pdf_id: int):
    """Re-parse an already-stored PDF without requiring a re-upload.

    Used when:
      - a prior parse was interrupted (e.g. backend restart mid-ingest)
        so the PDF exists but no sections were ever produced
      - the parser rules changed and you want to replay the file
      - the indexed content drifted for any other reason

    Pipeline: load bytes from code_book_pdfs, write to disk for the
    parser, supersede any currently-live sections for this book, create
    a fresh import_logs row, and fire import_svc.import_pdf in the
    background. The caller polls /api/imports/{log_id} to watch progress
    (same shape as a fresh upload).
    """
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT id, code_book_id, filename, content, size_bytes
                     FROM code_book_pdfs WHERE id = $1""",
                pdf_id,
            )
        if not row:
            raise HTTPException(status_code=404, detail=f"PDF {pdf_id} not found")

        code_book_id = row["code_book_id"]
        filename = row["filename"] or f"{pdf_id}.pdf"
        safe_name = os.path.basename(filename)

        # Materialise bytes to /tmp so the parser (which takes a path)
        # can read them. We drop the Python buffer before kicking off
        # the async task so the full file isn't held in RAM for the
        # lifetime of the parse.
        pdf_bytes = bytes(row["content"])
        temp_file_path = os.path.join(PDF_UPLOAD_DIR, safe_name)
        with open(temp_file_path, "wb") as f:
            f.write(pdf_bytes)
        del pdf_bytes

        async with db_pool.acquire() as conn:
            book = await conn.fetchrow(
                "SELECT id, code_name FROM code_books WHERE id = $1",
                code_book_id,
            )
            if not book:
                raise HTTPException(
                    status_code=404,
                    detail=f"code_book {code_book_id} not found",
                )

            # Supersede any current sections — same semantics as
            # uploading new bytes. If there's nothing current (common
            # for the interrupted-parse case) this is a no-op.
            status_tag = await conn.execute(
                """UPDATE code_sections
                      SET superseded_date = CURRENT_DATE, updated_at = NOW()
                    WHERE code_book_id = $1 AND superseded_date IS NULL""",
                code_book_id,
            )
            try:
                superseded_n = int(status_tag.split()[-1])
            except (ValueError, IndexError):
                superseded_n = 0

            src = await conn.fetchrow(
                """SELECT id FROM import_sources
                    WHERE code_book_id = $1 AND source_type = 'pdf_parse'
                    ORDER BY id DESC LIMIT 1""",
                code_book_id,
            )
            if src:
                source_id = src["id"]
            else:
                source_id = await conn.fetchval(
                    """INSERT INTO import_sources
                           (source_name, source_type, code_book_id, status)
                       VALUES ($1, 'pdf_parse', $2, 'pending')
                       RETURNING id""",
                    f"PDF uploads: {book['code_name']}", code_book_id,
                )

            import_log_id = await conn.fetchval(
                """INSERT INTO import_logs
                       (source_id, code_book_id, pdf_id, filename,
                        status, phase, imported_at, updated_at)
                   VALUES ($1, $2, $3, $4, 'processing', 'queued', NOW(), NOW())
                   RETURNING id""",
                source_id, code_book_id, pdf_id, safe_name,
            )

        # Reindex path: the operator already manually confirmed this PDF
        # belongs to this book when they originally uploaded it (or when
        # they hit /retry after an error-phase row). Skip the identity
        # check so retries don't keep re-paying the Ollama cost — they're
        # overwhelmingly going to be "yes, still the same book".
        _spawn_import_task(
            import_svc.import_pdf(
                file_path=temp_file_path,
                code_book_id=code_book_id,
                db_pool=db_pool,
                embedding_url=EMBEDDING_SERVICE_URL,
                source_id=source_id,
                import_log_id=import_log_id,
                source_pdf_id=pdf_id,
                ollama_url=OLLAMA_URL,
                ollama_model=OLLAMA_MODEL,
                ollama_num_ctx=OLLAMA_NUM_CTX,
                skip_identity_check=True,
            ),
            import_log_id=import_log_id,
            context=f"reindex pdf_id={pdf_id} book={code_book_id}",
        )

        logger.info(
            "Re-index started: pdf_id=%s book=%s import_log_id=%s superseded=%s",
            pdf_id, book["code_name"], import_log_id, superseded_n,
        )
        return {
            "status": "queued",
            "import_log_id": import_log_id,
            "pdf_id": pdf_id,
            "code_book_id": code_book_id,
            "filename": safe_name,
            "superseded_sections": superseded_n,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Re-index error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Image review: render PDF pages on demand -----------------------------
#
# All three review endpoints open the stored bytes with PyMuPDF. Rendering
# is CPU-bound and blocking, so we wrap the fitz calls in
# asyncio.to_thread() to keep the event loop responsive.

# --- In-memory caches for PDF rendering -----------------------------------
#
# Every PDF page render used to re-fetch the full PDF bytes from Postgres
# (~188 MB for ASCE 7-22) and re-render the PNG from scratch, even when
# the same user is flipping pages within one book. Two LRU caches now
# absorb those costs:
#
#   - PDF_BYTES cache: keyed by pdf_id. Skips the DB round-trip once
#     warm. Bounded by count (default 3 PDFs = ~600 MB worst case for
#     ASCE-sized files). Invalidated on reindex.
#
#   - PNG cache: keyed by (pdf_id, page, dpi). Lets the browser (and
#     any other client) ask for the same page repeatedly at basically
#     zero cost — the PNG is served straight from RAM. Bounded by
#     count (default 100 entries ≈ 50 MB at 500 KB/page).
#
# Both are thread-safe; the render path runs on a worker thread via
# asyncio.to_thread so locks matter.

_PDF_BYTES_CACHE_MAX = int(os.getenv("PDF_BYTES_CACHE_MAX", "3"))
_PNG_CACHE_MAX = int(os.getenv("PDF_PNG_CACHE_MAX", "100"))

_pdf_bytes_cache: "OrderedDict[int, tuple[bytes, Optional[str]]]" = OrderedDict()
_pdf_bytes_lock = threading.Lock()

_png_cache: "OrderedDict[tuple[int, int, int], bytes]" = OrderedDict()
_png_cache_lock = threading.Lock()


def _pdf_bytes_cache_get(pdf_id: int) -> Optional[tuple[bytes, Optional[str]]]:
    with _pdf_bytes_lock:
        v = _pdf_bytes_cache.get(pdf_id)
        if v is not None:
            _pdf_bytes_cache.move_to_end(pdf_id)
        return v


def _pdf_bytes_cache_put(pdf_id: int, value: tuple[bytes, Optional[str]]) -> None:
    with _pdf_bytes_lock:
        _pdf_bytes_cache[pdf_id] = value
        _pdf_bytes_cache.move_to_end(pdf_id)
        while len(_pdf_bytes_cache) > _PDF_BYTES_CACHE_MAX:
            _pdf_bytes_cache.popitem(last=False)


def _png_cache_get(key: tuple[int, int, int]) -> Optional[bytes]:
    with _png_cache_lock:
        v = _png_cache.get(key)
        if v is not None:
            _png_cache.move_to_end(key)
        return v


def _png_cache_put(key: tuple[int, int, int], value: bytes) -> None:
    with _png_cache_lock:
        _png_cache[key] = value
        _png_cache.move_to_end(key)
        while len(_png_cache) > _PNG_CACHE_MAX:
            _png_cache.popitem(last=False)


def _invalidate_pdf_caches(pdf_id: int) -> None:
    """Drop every cached artifact for ``pdf_id``. Call on reindex / delete."""
    with _pdf_bytes_lock:
        _pdf_bytes_cache.pop(pdf_id, None)
    with _png_cache_lock:
        victims = [k for k in _png_cache if k[0] == pdf_id]
        for k in victims:
            _png_cache.pop(k, None)


async def _fetch_pdf_bytes(pdf_id: int) -> tuple[bytes, Optional[str]]:
    """Cache-aware: warm path is RAM-only; cold path hits Postgres once."""
    cached = _pdf_bytes_cache_get(pdf_id)
    if cached is not None:
        return cached
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT filename, content, size_bytes FROM code_book_pdfs WHERE id = $1",
            pdf_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="PDF not found")
    result = (bytes(row["content"]), row["filename"])
    _pdf_bytes_cache_put(pdf_id, result)
    return result


@app.get("/api/code-book-pdfs/{pdf_id}/pages")
async def pdf_meta(pdf_id: int):
    """Return per-PDF metadata used by the review UI before it starts rendering."""
    try:
        pdf_bytes, filename = await _fetch_pdf_bytes(pdf_id)

        def inspect() -> dict:
            import fitz  # local import to keep cold start of main.py light
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                n = doc.page_count
                first = doc[0] if n > 0 else None
                w = first.rect.width if first else 0
                h = first.rect.height if first else 0
                return {"page_count": n, "first_width": w, "first_height": h}
            finally:
                doc.close()

        meta = await asyncio.to_thread(inspect)
        meta["filename"] = filename
        meta["size_bytes"] = len(pdf_bytes)
        return meta
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF meta error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-book-pdfs/{pdf_id}/pages/{page}.png")
async def pdf_page_png(
    pdf_id: int,
    page: int,
    request: Request,
    dpi: int = Query(150, ge=72, le=300),
):
    """Render a single PDF page to PNG on demand.

    Hot path layers (fastest first):
        1. Conditional-GET: ``If-None-Match`` === ETag → 304, no body.
        2. Server-side PNG LRU cache → RAM → Response body.
        3. Server-side PDF-bytes LRU cache + fitz render.
        4. Cold path: Postgres fetch + fitz render + cache population.

    ETag is ``"{pdf_id}-{page}-{dpi}"`` — bytes for that triple are
    immutable as long as the row in ``code_book_pdfs`` exists, so the
    strong-ETag semantics are correct.
    """
    try:
        etag = f'"{pdf_id}-{page}-{dpi}"'

        # Layer 1: conditional GET. The browser already has bytes that
        # match — respond 304 and skip everything downstream.
        inm = request.headers.get("if-none-match")
        if inm and etag in (v.strip() for v in inm.split(",")):
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "Cache-Control": "public, max-age=3600",
                },
            )

        # Layer 2: server-side PNG cache.
        key = (pdf_id, page, dpi)
        cached_png = _png_cache_get(key)
        if cached_png is not None:
            return Response(
                content=cached_png,
                media_type="image/png",
                headers={
                    "ETag": etag,
                    "Cache-Control": "public, max-age=3600",
                    "X-Cache": "HIT",
                },
            )

        # Layer 3/4: render. fetch_pdf_bytes itself is cached; the
        # fitz.open() cost on cached bytes is milliseconds.
        pdf_bytes, _ = await _fetch_pdf_bytes(pdf_id)

        def render() -> bytes:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                if page < 1 or page > doc.page_count:
                    raise HTTPException(
                        status_code=404,
                        detail=f"page {page} out of range (1..{doc.page_count})",
                    )
                pix = doc[page - 1].get_pixmap(dpi=dpi)
                return pix.tobytes("png")
            finally:
                doc.close()

        png = await asyncio.to_thread(render)
        _png_cache_put(key, png)
        return Response(
            content=png,
            media_type="image/png",
            headers={
                "ETag": etag,
                "Cache-Control": "public, max-age=3600",
                "X-Cache": "MISS",
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF page render error (pdf={pdf_id}, page={page}): {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-book-pdfs/{pdf_id}/pages/{page}/text")
async def pdf_page_text(pdf_id: int, page: int):
    """Return the raw PyMuPDF text for a single page (pre-filter)."""
    try:
        pdf_bytes, _ = await _fetch_pdf_bytes(pdf_id)

        def extract() -> str:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                if page < 1 or page > doc.page_count:
                    raise HTTPException(
                        status_code=404,
                        detail=f"page {page} out of range (1..{doc.page_count})",
                    )
                return doc[page - 1].get_text()
            finally:
                doc.close()

        text = await asyncio.to_thread(extract)
        return {"page": page, "text": text, "chars": len(text)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"PDF page text error (pdf={pdf_id}, page={page}): {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/code-books/{book_id}/sections")
async def list_book_sections(
    book_id: int,
    page: int | None = Query(None, description="Filter to sections on a specific page"),
    # The Browser panel needs the full outline (2k+ sections for ASCE 7-22)
    # to render a tree, so the upper bound had to grow. Payload is ~2MB
    # uncached and cached aggressively by react-query client-side.
    limit: int = Query(50, ge=1, le=5000),
    outline: bool = Query(False, description="Return tree-outline shape only: drops full_text to keep payload small."),
):
    """List sections for a code book, optionally filtered by source page.

    Used by the review UI's "Sections on this page" pane. Returns empty
    when ``page`` is provided but no sections were attributed to it
    (common for older imports that predate page_number tracking).
    """
    try:
        async with db_pool.acquire() as conn:
            if page is not None:
                rows = await conn.fetch(
                    """SELECT id, section_number, section_title, full_text,
                              depth, page_number, has_ca_amendment, amendment_agency,
                              section_type
                         FROM code_sections
                        WHERE code_book_id = $1 AND page_number = $2
                          AND superseded_date IS NULL
                        ORDER BY display_order NULLS LAST, id
                        LIMIT $3""",
                    book_id, page, limit,
                )
            else:
                rows = await conn.fetch(
                    """SELECT id, section_number, section_title, full_text,
                              depth, page_number, has_ca_amendment, amendment_agency,
                              section_type
                         FROM code_sections
                        WHERE code_book_id = $1
                          AND superseded_date IS NULL
                        ORDER BY display_order NULLS LAST, id
                        LIMIT $2""",
                    book_id, limit,
                )
        return [
            {
                "id": r["id"],
                "section_number": r["section_number"],
                "section_title": r["section_title"],
                # In outline mode, drop the body text — the Browser panel
                # fetches it lazily per-click via /api/sections/{id}, and
                # a 2k-section list is ~2MB smaller without it.
                "full_text": None if outline else r["full_text"],
                "depth": r["depth"],
                "page_number": r["page_number"],
                "has_ca_amendment": r["has_ca_amendment"],
                "amendment_agency": r["amendment_agency"],
                "section_type": r["section_type"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"List sections error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class FlagPageRequest(BaseModel):
    pdf_id: int
    code_book_id: int
    page: int
    reason: str  # text_missing | text_wrong | layout_broken | ocr_needed | other
    note: Optional[str] = None


@app.post("/api/review/flag")
async def flag_page(req: FlagPageRequest):
    """Route a user-flagged bad extraction into the existing quarantine queue.

    The reviewer flags a specific page; we grab the page's raw PyMuPDF text
    so Quarantine has the actual content the parser saw, then insert a
    row with ``validation_layer = 0`` (sentinel for "user-flagged"; the
    validator itself uses 1..4).
    """
    try:
        # Fetch page text up-front (also validates pdf/page).
        pdf_bytes, filename = await _fetch_pdf_bytes(req.pdf_id)

        def page_text() -> str:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                if req.page < 1 or req.page > doc.page_count:
                    raise HTTPException(
                        status_code=404,
                        detail=f"page {req.page} out of range",
                    )
                return doc[req.page - 1].get_text()
            finally:
                doc.close()

        raw_text = await asyncio.to_thread(page_text)

        # Find-or-create the pdf_parse import source for this book.
        async with db_pool.acquire() as conn:
            book = await conn.fetchrow(
                "SELECT id, code_name FROM code_books WHERE id = $1",
                req.code_book_id,
            )
            if not book:
                raise HTTPException(
                    status_code=404,
                    detail=f"code_book {req.code_book_id} not found",
                )
            src = await conn.fetchrow(
                """SELECT id FROM import_sources
                    WHERE code_book_id = $1 AND source_type = 'pdf_parse'
                    ORDER BY id DESC LIMIT 1""",
                req.code_book_id,
            )
            if src:
                source_id = src["id"]
            else:
                source_id = await conn.fetchval(
                    """INSERT INTO import_sources
                           (source_name, source_type, code_book_id, status)
                       VALUES ($1, 'pdf_parse', $2, 'pending')
                       RETURNING id""",
                    f"PDF uploads: {book['code_name']}",
                    req.code_book_id,
                )

            metadata = {
                "source": "review_ui",
                "pdf_id": req.pdf_id,
                "code_book_id": req.code_book_id,
                "page_number": req.page,
                "filename": filename,
                "flagged_at": datetime.utcnow().isoformat() + "Z",
            }
            msg = req.reason if not req.note else f"{req.reason} — {req.note}"

            quarantine_id = await conn.fetchval(
                """INSERT INTO content_quarantine
                       (source_id, validation_layer, error_message,
                        raw_content, metadata)
                   VALUES ($1, 0, $2, $3, $4::jsonb)
                   RETURNING id""",
                source_id, msg, raw_text[:5000], json.dumps(metadata),
            )
        return {
            "quarantine_id": quarantine_id,
            "source_id": source_id,
            "page": req.page,
            "reason": req.reason,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Flag page error: {e}")
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


def _import_row_to_dict(r) -> Dict[str, Any]:
    """Serialize an import_logs row (with optional book/pdf joins) for the UI.

    Produces the same shape for both the list and single-import endpoints so
    the frontend can bind one type to both. All extended progress fields
    (current_page, ocr_pages_count, stage_detail, etc.) are included when
    present on the row; they default to None/0 for pre-migration rows.
    """
    # asyncpg.Record supports `in` and `[]` but not `.get()`; this helper
    # lets us treat an optional joined column (book_name etc.) as missing
    # when the calling query didn't select it.
    def col(key: str, default=None):
        try:
            return r[key]
        except (KeyError, IndexError):
            return default

    total = r["records_total"] or 0
    processed = r["records_processed"] or 0
    phase = r["phase"] or "queued"
    current_page = col("current_page")
    total_pages = col("total_pages")
    # Prefer page-level percent during parse, section-level otherwise.
    pct: Optional[int] = None
    if phase == "parsing" and total_pages:
        pct = int(100 * (current_page or 0) / total_pages)
    elif total:
        pct = int(100 * processed / total)
    return {
        "id": r["id"],
        "source_id": r["source_id"],
        "source_type": col("source_type"),
        "status": r["status"],
        "phase": phase,
        "code_book_id": r["code_book_id"],
        "book_name": col("book_name"),
        "book_abbreviation": col("book_abbreviation"),
        "book_part_number": col("book_part_number"),
        "pdf_id": r["pdf_id"],
        "pdf_size_bytes": col("pdf_size_bytes"),
        "filename": r["filename"],
        "records_total": total or None,
        "records_processed": processed,
        "records_imported": r["records_imported"] or 0,
        "records_failed": r["records_failed"] or 0,
        "current_page": current_page,
        "total_pages": total_pages,
        "ocr_pages_count": col("ocr_pages_count", 0) or 0,
        "toc_entries_count": col("toc_entries_count"),
        "current_section_number": col("current_section_number"),
        "stage_detail": col("stage_detail"),
        "references_found": col("references_found", 0) or 0,
        "started_parsing_at": col("started_parsing_at").isoformat() if col("started_parsing_at") else None,
        "started_indexing_at": col("started_indexing_at").isoformat() if col("started_indexing_at") else None,
        "percent": pct,
        "error_message": r["error_message"],
        "imported_at": r["imported_at"].isoformat() if r["imported_at"] else None,
        "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
    }


@app.get("/api/imports")
async def get_imports(limit: int = Query(20, ge=1, le=100)):
    """Recent + active imports with joined metadata for the dashboard.

    Each row represents one import_logs entry — either a PDF upload or a
    scraper run — with the code book it targets and (for uploads) the
    stored PDF id so the UI can link back to the binary. Ordered by most
    recent activity first.
    """
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT il.id, il.source_id, il.status, il.phase,
                       il.code_book_id, il.pdf_id, il.filename,
                       il.records_total, il.records_processed,
                       il.records_imported, il.records_failed,
                       il.current_page, il.total_pages, il.ocr_pages_count,
                       il.toc_entries_count, il.current_section_number,
                       il.stage_detail, il.references_found,
                       il.started_parsing_at, il.started_indexing_at,
                       il.error_message, il.imported_at, il.updated_at,
                       il.completed_at,
                       cb.code_name AS book_name,
                       cb.abbreviation AS book_abbreviation,
                       cb.part_number AS book_part_number,
                       cbp.size_bytes AS pdf_size_bytes,
                       isrc.source_type
                  FROM import_logs il
                  LEFT JOIN code_books cb ON cb.id = il.code_book_id
                  LEFT JOIN code_book_pdfs cbp ON cbp.id = il.pdf_id
                  LEFT JOIN import_sources isrc ON isrc.id = il.source_id
                 ORDER BY COALESCE(il.updated_at, il.imported_at) DESC
                 LIMIT $1
                """,
                limit,
            )
            return [_import_row_to_dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Imports list error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/imports/{import_log_id}")
async def get_import(import_log_id: int):
    """Single import log row for progress polling after an upload."""
    try:
        async with db_pool.acquire() as conn:
            r = await conn.fetchrow(
                """SELECT il.*, cb.code_name AS book_name,
                          cb.abbreviation AS book_abbreviation,
                          cb.part_number AS book_part_number,
                          cbp.size_bytes AS pdf_size_bytes,
                          isrc.source_type
                   FROM import_logs il
                   LEFT JOIN code_books cb ON cb.id = il.code_book_id
                   LEFT JOIN code_book_pdfs cbp ON cbp.id = il.pdf_id
                   LEFT JOIN import_sources isrc ON isrc.id = il.source_id
                   WHERE il.id = $1""",
                import_log_id,
            )
        if not r:
            raise HTTPException(status_code=404, detail="import not found")
        return _import_row_to_dict(r)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Import detail error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/imports/{import_log_id}")
async def delete_import(
    import_log_id: int,
    delete_sections: bool = Query(
        True,
        description="Also delete the code_sections this import produced.",
    ),
    delete_pdf: bool = Query(
        True,
        description=(
            "Also delete the stored PDF bytes. Ignored if other import_logs "
            "still reference this pdf_id (those would break)."
        ),
    ),
):
    """Delete an import_log row, and optionally its produced sections + PDF.

    UX contract:
      - Always deletes the import_log row itself.
      - ``delete_sections`` removes every ``code_sections`` row that was
        produced by this PDF (joined via ``source_pdf_id``). Cascades take
        care of ``code_section_versions`` and ``code_references`` on the
        source side; cross-refs *into* these sections from other codes
        become NULL (handled by the FK ``ON DELETE SET NULL``).
      - ``delete_pdf`` removes the stored PDF bytes, but only if no other
        import_log still points at it (otherwise we'd leave an orphan log
        row with ``pdf_id`` silently NULLed, which would be confusing).

    Returns the counts of each kind of row removed so the UI can show a
    "Deleted X sections, 1 PDF" toast.
    """
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT id, pdf_id, code_book_id FROM import_logs WHERE id = $1",
                    import_log_id,
                )
                if not row:
                    raise HTTPException(status_code=404, detail="import not found")

                pdf_id = row["pdf_id"]
                sections_deleted = 0
                pdf_deleted = False

                # Delete the indexed sections first — references cascade.
                if delete_sections and pdf_id is not None:
                    deleted_sections = await conn.execute(
                        "DELETE FROM code_sections WHERE source_pdf_id = $1",
                        pdf_id,
                    )
                    # asyncpg returns "DELETE N"
                    try:
                        sections_deleted = int(deleted_sections.split()[-1])
                    except (ValueError, IndexError):
                        sections_deleted = 0

                # Drop the import_log (FK on pdf_id is SET NULL so this
                # doesn't block on the PDF still existing).
                await conn.execute(
                    "DELETE FROM import_logs WHERE id = $1", import_log_id,
                )

                # Drop the PDF bytes if no other log references it. We
                # check AFTER deleting the log so the self-reference is
                # already gone.
                if delete_pdf and pdf_id is not None:
                    other_refs = await conn.fetchval(
                        "SELECT count(*) FROM import_logs WHERE pdf_id = $1",
                        pdf_id,
                    )
                    if not other_refs:
                        del_pdf = await conn.execute(
                            "DELETE FROM code_book_pdfs WHERE id = $1", pdf_id,
                        )
                        try:
                            pdf_deleted = int(del_pdf.split()[-1]) > 0
                        except (ValueError, IndexError):
                            pdf_deleted = False

        return {
            "deleted": True,
            "import_log_id": import_log_id,
            "sections_deleted": sections_deleted,
            "pdf_deleted": pdf_deleted,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Delete import error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/imports/{import_log_id}/retry")
async def retry_import(import_log_id: int):
    """Re-run an import against the PDF bytes still stored in the database.

    This is the operator-facing path for failure-mode recovery after the
    empty-extraction / all-quarantined / crashed phases introduced in
    migration 003. Unlike ``/api/code-book-pdfs/{pdf_id}/reindex`` — which
    takes a pdf_id and is the "refresh everything" button — this endpoint
    takes the import_log_id the UI already has on screen, which means the
    Imports table can expose a one-click retry without the frontend having
    to go fetch pdf_id first.

    The semantics are otherwise identical to ``reindex_pdf``: supersede
    any currently-live sections for the target book, create a fresh
    import_logs row, spawn a wrapped worker, and return the new log id
    for progress polling.
    """
    try:
        async with db_pool.acquire() as conn:
            log_row = await conn.fetchrow(
                """SELECT id, pdf_id, code_book_id, status, phase
                     FROM import_logs WHERE id = $1""",
                import_log_id,
            )
        if not log_row:
            raise HTTPException(status_code=404, detail="import not found")
        if log_row["pdf_id"] is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "This import has no stored PDF to retry (pdf_id is NULL — "
                    "scraper runs and imports whose PDF was deleted cannot "
                    "be retried from here)."
                ),
            )
        # Delegate to the existing reindex path so both endpoints produce
        # identical state and we don't fork the ingest logic.
        return await reindex_pdf(log_row["pdf_id"])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Retry import error: {e}")
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
            # Dashboard "Code Sections" card shows current (non-superseded)
            # rows only; superseded historical versions shouldn't inflate it.
            sections = await conn.fetchval(
                "SELECT count(*) FROM code_sections WHERE superseded_date IS NULL"
            )
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


@app.get("/api/catalog")
async def get_catalog():
    """Return the full code catalog grouped by adopting authority and cycle.

    Includes per-book indexed_section_count and scan_status (derived from
    the latest import_sources row for that book). Used by the Catalog
    panel in the dashboard.
    """
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH section_counts AS (
                    -- Only count current versions so the Catalog's
                    -- indexed_section_count matches what chat/search see.
                    SELECT code_book_id, count(*)::int AS cnt
                    FROM code_sections
                    WHERE superseded_date IS NULL
                    GROUP BY code_book_id
                ),
                latest_import AS (
                    SELECT DISTINCT ON (code_book_id)
                        code_book_id, id AS source_id, status, last_crawled,
                        sections_imported, next_crawl_at
                    FROM import_sources
                    WHERE code_book_id IS NOT NULL
                    ORDER BY code_book_id, last_crawled DESC NULLS LAST, id DESC
                ),
                latest_pdf AS (
                    -- Most recent stored PDF per book so the Catalog UI can
                    -- show/enable a Review button without a second round-trip.
                    SELECT DISTINCT ON (code_book_id)
                        code_book_id, id AS pdf_id, filename, uploaded_at
                    FROM code_book_pdfs
                    ORDER BY code_book_id, uploaded_at DESC, id DESC
                )
                SELECT
                    po.abbreviation   AS org_abbr,
                    po.full_name      AS org_full_name,
                    cc.id             AS cycle_id,
                    cc.name           AS cycle_name,
                    cc.adopting_authority,
                    cc.effective_date AS cycle_effective_date,
                    cc.expiration_date AS cycle_expiration_date,
                    cc.status         AS cycle_status,
                    cb.id             AS book_id,
                    cb.code_name,
                    cb.abbreviation   AS book_abbr,
                    cb.part_number,
                    cb.category,
                    cb.base_model_abbreviation,
                    cb.base_code_year,
                    cb.digital_access_url,
                    cb.status         AS book_status,
                    cb.effective_date AS book_effective_date,
                    cb.superseded_date AS book_superseded_date,
                    COALESCE(sc.cnt, 0)   AS indexed_section_count,
                    li.source_id,
                    li.status         AS import_status,
                    li.last_crawled,
                    li.sections_imported,
                    lp.pdf_id         AS latest_pdf_id,
                    lp.filename       AS latest_pdf_filename
                FROM code_books cb
                JOIN code_cycles cc ON cc.id = cb.cycle_id
                JOIN publishing_orgs po ON po.id = cb.publishing_org_id
                LEFT JOIN section_counts sc ON sc.code_book_id = cb.id
                LEFT JOIN latest_import li ON li.code_book_id = cb.id
                LEFT JOIN latest_pdf lp ON lp.code_book_id = cb.id
                ORDER BY cc.adopting_authority, cc.effective_date DESC,
                         cb.part_number NULLS LAST, cb.abbreviation
                """
            )

            # Re-shape the flat rows into authority → cycles → books.
            authorities: dict[str, dict] = {}
            for r in rows:
                auth_key = r["adopting_authority"]
                auth = authorities.setdefault(auth_key, {
                    "adopting_authority": auth_key,
                    "publishing_org_abbr": r["org_abbr"],
                    "publishing_org_full_name": r["org_full_name"],
                    "cycles": {},
                })
                cyc = auth["cycles"].setdefault(r["cycle_id"], {
                    "id": r["cycle_id"],
                    "name": r["cycle_name"],
                    "effective_date": r["cycle_effective_date"].isoformat() if r["cycle_effective_date"] else None,
                    "expiration_date": r["cycle_expiration_date"].isoformat() if r["cycle_expiration_date"] else None,
                    "status": r["cycle_status"],
                    "books": [],
                })

                # Derive scan_status: not_scanned | scheduled | crawling | indexed | error
                indexed = r["indexed_section_count"] or 0
                import_status = r["import_status"]
                if import_status in (None, "pending") and indexed == 0:
                    scan_status = "not_scanned"
                elif import_status == "crawling":
                    scan_status = "crawling"
                elif import_status == "error":
                    scan_status = "error"
                elif indexed > 0:
                    scan_status = "indexed"
                else:
                    scan_status = import_status or "not_scanned"

                cyc["books"].append({
                    "id": r["book_id"],
                    "code_name": r["code_name"],
                    "abbreviation": r["book_abbr"],
                    "part_number": r["part_number"],
                    "category": r["category"],
                    "base_model_abbreviation": r["base_model_abbreviation"],
                    "base_code_year": r["base_code_year"],
                    "digital_access_url": r["digital_access_url"],
                    "status": r["book_status"],
                    "effective_date": r["book_effective_date"].isoformat() if r["book_effective_date"] else None,
                    "superseded_date": r["book_superseded_date"].isoformat() if r["book_superseded_date"] else None,
                    "indexed_section_count": indexed,
                    "scan_status": scan_status,
                    "source_id": r["source_id"],
                    "last_crawled": r["last_crawled"].isoformat() if r["last_crawled"] else None,
                    "latest_pdf_id": r["latest_pdf_id"],
                    "latest_pdf_filename": r["latest_pdf_filename"],
                })

            # Flatten dicts → lists, preserving insertion order (Python 3.7+).
            result = []
            for auth in authorities.values():
                auth_out = {
                    "adopting_authority": auth["adopting_authority"],
                    "publishing_org_abbr": auth["publishing_org_abbr"],
                    "publishing_org_full_name": auth["publishing_org_full_name"],
                    "cycles": list(auth["cycles"].values()),
                }
                result.append(auth_out)
            return {"authorities": result}
    except Exception as e:
        logger.error(f"Catalog error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class CatalogScanRequest(BaseModel):
    code_book_ids: List[int]


@app.post("/api/catalog/scan")
async def catalog_scan(req: CatalogScanRequest):
    """Trigger a scan for the given code_book_ids.

    For each book:
      - if digital_access_url is NULL  -> skipped_no_url
      - else: find or create an import_sources row + fire the ICC scraper
        in the background; the book's scan_status will flip to 'crawling'.
    """
    if not req.code_book_ids:
        raise HTTPException(status_code=400, detail="code_book_ids is required")

    triggered: list[dict] = []
    skipped_no_url: list[dict] = []
    errors: list[dict] = []

    try:
        async with db_pool.acquire() as conn:
            books = await conn.fetch(
                """SELECT id, code_name, abbreviation, digital_access_url
                   FROM code_books WHERE id = ANY($1::int[])""",
                req.code_book_ids,
            )
            book_map = {b["id"]: b for b in books}
            missing = [bid for bid in req.code_book_ids if bid not in book_map]
            for bid in missing:
                errors.append({"code_book_id": bid, "error": "code_book not found"})

            for book in books:
                if not book["digital_access_url"]:
                    skipped_no_url.append({
                        "code_book_id": book["id"],
                        "code_name": book["code_name"],
                    })
                    continue

                # Find or create a web_scrape import_sources row for this book.
                src = await conn.fetchrow(
                    """SELECT id FROM import_sources
                       WHERE code_book_id = $1 AND source_type = 'web_scrape'
                       ORDER BY id DESC LIMIT 1""",
                    book["id"],
                )
                if src:
                    source_id = src["id"]
                    await conn.execute(
                        """UPDATE import_sources
                           SET source_url = $1, status = 'pending', updated_at = NOW()
                           WHERE id = $2""",
                        book["digital_access_url"], source_id,
                    )
                else:
                    source_id = await conn.fetchval(
                        """INSERT INTO import_sources
                               (source_name, source_url, source_type,
                                code_book_id, status)
                           VALUES ($1, $2, 'web_scrape', $3, 'pending')
                           RETURNING id""",
                        f"Catalog scan: {book['code_name']}",
                        book["digital_access_url"],
                        book["id"],
                    )

                # Kick off the ICC scraper in the background. We only have one
                # scraper today; non-ICC URLs will error asynchronously, which
                # surfaces in the Import panel.
                if "iccsafe.org" in book["digital_access_url"]:
                    asyncio.create_task(
                        scrape_runner.run_icc_import(
                            code_url=book["digital_access_url"],
                            code_book_id=book["id"],
                            db_pool=db_pool,
                            embedding_url=EMBEDDING_SERVICE_URL,
                            source_id=source_id,
                        )
                    )
                    triggered.append({
                        "code_book_id": book["id"],
                        "code_name": book["code_name"],
                        "source_id": source_id,
                        "scraper": "icc",
                    })
                else:
                    errors.append({
                        "code_book_id": book["id"],
                        "error": "no scraper for this URL host yet — upload a PDF via Import panel or use an ICC URL",
                    })

        return {
            "triggered": triggered,
            "skipped_no_url": skipped_no_url,
            "errors": errors,
        }
    except Exception as e:
        logger.error(f"Catalog scan error: {e}")
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
