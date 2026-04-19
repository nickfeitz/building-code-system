# Building Code System

A self-hosted, UpCodes-style reader for building and engineering codes (ASCE, IBC, CBC, NEC, NFPA, …). Upload a code PDF once, the system parses the document's Table of Contents into a structured hierarchy, stores every section in PostgreSQL with vector embeddings, and exposes a browse-and-search UI that reads like a first-class digital edition of the code.

The original PDF is preserved as backup and every section links back to its source page.

## What it does

1. **Ingests code PDFs.** Upload a PDF via the Import panel (or REST API). The backend reads the PDF's own Table of Contents as ground truth, extracts body text per section via PyMuPDF, falls back to Tesseract OCR on sparse/scanned pages, strips running headers & footers, and stores each section — with its number, title, depth, page reference, chapter, and embedding vector — in Postgres.
2. **Browses like UpCodes.** The Code Browser panel opens the outline of any indexed book. Clicking a chapter renders the whole chapter as one flowing document — chapter title, then every subsection inline, depth-indented, with section numbers as anchors and in-text references ("Section 26.5.2", "Chapter 31", "Figure 26.5-1B") as clickable links that jump inside the book.
3. **Searches across codes.** Full-text search (Postgres `tsvector` + `pg_trgm`), plus a hybrid semantic + lexical endpoint backed by `intfloat/e5-large-v2` embeddings, scoped to a single book or the whole corpus.
4. **Links duplicates across codes.** Sections with identical normalized text across different codes (common boilerplate, shared definitions) get a shared `canonical_section_id`, so the reader can see "this same provision also appears in X and Y."
5. **Preserves sources.** Every uploaded PDF is stored in `code_book_pdfs` as bytea. Any section shows a "View PDF page N" link that renders the exact original page from that stored file.
6. **AI chat.** A streaming `/api/chat` endpoint backed by local Ollama models (with an optional Anthropic Claude path) answers questions using the indexed codes as grounding. (Citation enforcement is iterating — treat output as a research aid, not a substitute for opening the book.)
7. **Quality gates.** Every section goes through a 4-layer validator (format, garbage/OCR check, code-structure heuristic, integrity/dedup). Rejects land in a Quarantine panel for human review, and pages the user flags as bad extractions get routed into the same queue with the raw PyMuPDF text attached.
8. **Live progress.** The Imports panel surfaces per-PDF parse/index progress in real time — total TOC entries, rows indexed, rows quarantined, current phase.
9. **Web scraping (planned).** A scraper skeleton for ICC exists in `backend/scrapers/`. Phase 1 (PDF ingest) is the current focus; Phase 5 (web scrape + diff against PDF) is the next milestone.

## Architecture

```
                        ┌─────────────────┐
  upload PDF ─► Import ─►  backend (8010) ─► Postgres 17 + pgvector
                        │  FastAPI + asyncpg          │
                        │  TOC + OCR + embeddings     │
                        └──┬──────────────┬──────────┘
                           │              │
                           ▼              ▼
                 embedding-service   Ollama (host)
                   (E5-large-v2)     gemma/qwen/etc.
                                          │
  browser ─────► frontend (3010) ─► backend ◄──── scraper-service (8012)
            nginx + React + Vite     /api/*       (phase 5, idle for now)
```

- **backend** — FastAPI. PDF parsing in `backend/parsers/toc_extractor.py` + `document_extractor.py`. Orchestration in `backend/services/import_service.py`. Validation in `backend/validators/content_validator.py`. All endpoints live in `backend/main.py`.
- **embedding-service** — dedicated container that loads sentence-transformers `intfloat/e5-large-v2` once and exposes `/embed`.
- **frontend** — React + TypeScript + Tailwind. Key panels: `Dashboard`, `Catalog` (book list + scan triggers), `Import` (upload + progress), `Browser` (the reader — `frontend/src/panels/BrowserPanel.tsx`), `Review` (page-level QA with side-by-side image + text + flag), `Quarantine`, `Chat`.
- **Postgres** — schema in `backend/schema.sql`; additive migrations in `backend/migrations/`. Core tables: `code_books`, `code_sections` (with `embedding vector(1024)`, `canonical_section_id`, `normalized_hash`, `page_number`, `source_pdf_id`), `code_book_pdfs` (source bytes), `code_references` (cross-refs), `import_logs`, `content_quarantine`.

## Ingestion pipeline (how a PDF becomes browsable code)

1. **Upload** via `POST /api/import/upload` — bytes stored in `code_book_pdfs`, returning `import_log_id`.
2. **TOC discovery** — `TocExtractor` reads `fitz.get_toc()`; if the embedded outline is absent or too short, it visually parses leader-dot rows on the front-matter pages (with Tesseract OCR as a final fallback for scanned TOC pages).
3. **Page-target verification** — each outline entry's claimed page is validated against the actual heading text on that page; mismatches (common in PDFs whose outline points at commentary links instead of body pages) are corrected by searching forward from the last trusted entry.
4. **Body slicing** — `DocumentExtractor` concatenates pages in each TOC entry's range, strips running headers/footers by frequency, skips fronts-matter sections (Preface, Index, Commentary), and keeps the body text between this heading and the next one.
5. **Validation** — 4 layers: format (empty/binary/mojibake), garbage (web-scrape heuristics only on web sources, never on PDFs), code-structure (soft signal, doesn't hard-fail), integrity (dedup vs. live rows + truncation checks).
6. **Insert** — section rows, version history, embedding call to the embedding-service, cross-reference extraction, canonical-link to any existing section with the same normalized hash.
7. **Progress** — `import_logs.phase` + counters update every ~2% so the UI can show a live bar.

## Validation state

Validated end-to-end against ASCE/SEI 7-22 (1,046 pages, 188 MB PDF): **2,129 sections indexed from 2,526 TOC entries (84%)**. Chapter 26 (Wind Loads) fully recovered with real section bodies, correct page attribution, and working cross-references to Chapter 27 / 31 / Figure 26.5-1B etc. The ~16% gap is mostly legitimate duplicates and short boilerplate rejected by integrity validation; relaxing those rules further is the next follow-up.

## Quick start

Requires Docker, Docker Compose, and an existing `postgres-stack_default` network with a Postgres 17 container that has `pgvector` and `pg_trgm`.

```bash
# One-time: create the dedicated database
docker exec postgres psql -U postgres -c "CREATE DATABASE building_code OWNER postgres;"
docker exec postgres psql -U postgres -d building_code -c \
  "CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS pg_trgm;"
docker exec -i postgres psql -U postgres -d building_code < backend/schema.sql
docker exec -i postgres psql -U postgres -d building_code < backend/migrations/001_dedup_columns.sql

# Build + run
cp .env.example .env   # then set CLAUDE_API_KEY if you want the Claude path
docker compose up -d --build
```

Services:
- **Frontend** http://localhost:3010
- **Backend API** http://localhost:8010 (OpenAPI at `/openapi.json`)
- **Embedding** http://localhost:8011/health
- **Scraper** http://localhost:8012 (idle until Phase 5)

## Project layout

```
backend/
  main.py                       # FastAPI app — every endpoint
  schema.sql                    # initial schema
  migrations/                   # additive migrations (numbered)
  parsers/
    toc_extractor.py            # Stage A — outline discovery + verification
    document_extractor.py       # Stage B/C — body slicing + header scrub
    reference_extractor.py      # cross-ref detection in body text
    pdf_parser.py               # legacy Title 24 parser (retained, unused)
  services/import_service.py    # ingest orchestration + embeddings + dedup
  validators/content_validator.py  # 4-layer validation pipeline
  scrapers/                     # Phase 5 — ICC scraper skeleton
embedding-service/              # E5-large-v2 sidecar (FastAPI + sentence-transformers)
frontend/
  src/
    panels/                     # Browser, Import, Catalog, Review, Chat, Dashboard, Quarantine
    hooks/                      # useBrowser, useCatalog, useImports, useReview, useChat, …
    api/                        # typed fetch client + response shapes
docker-compose.yml
.env
STARTUP.md                      # detailed operational guide
```

## Roadmap

- ✅ **Phase 1 — PDF ingestion.** TOC-driven extractor, OCR fallback, dedup infrastructure, validated against ASCE 7-22.
- ✅ **Phase 2/3 — Database + Browser UI.** Schema with vector + trigram indexes; UpCodes-style single-window reader with inline reference linking.
- ⏳ **Phase 4 — Close the 16% validation gap.** Stop quarantining legitimate short sections and duplicate boilerplate within a single book.
- ⏳ **Phase 5 — Web scraping.** ICC + gov-source scrapers. When a web version is newer than the PDF, it becomes the displayed text; PDF remains the backup.
- ⏳ **Phase 6 — AI chat with enforced citations.** Already streaming against local LLMs + optional Claude; needs hard citation grounding before code answers should be relied on.
- ⏳ **Phase 7 — Dashboard polish.** Per-book coverage, quarantine trends, cross-code duplicate explorer.
