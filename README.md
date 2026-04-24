# Building Code System

A self-hosted reader for building and engineering codes — ASCE, IBC, CBC, NEC, NFPA, and friends — that turns a raw code PDF into a browsable, searchable, AI-queryable digital edition. Think **UpCodes, but on your own Postgres, with your own PDFs, with the source document always linked back**.

## What the app does, in one screen

Upload `ASCE-7-22.pdf`. A few minutes later you get:

- A **hierarchical outline** of the whole standard — chapters, sections, subsections, appendices — that matches the printed Table of Contents, because the ingester reads the PDF's TOC and uses it as ground truth instead of guessing at structure.
- A **reader view** that renders any chapter as one flowing document — chapter title, every subsection inline in document order, depth-indented, with section numbers as anchors. Soft line wraps from the PDF are collapsed into real prose; hyphenated wraps are repaired. Inline cross-references (`Section 26.5.2`, `Chapter 31`, `Figure 26.5-1B`, `Table 1.5-1`, `Appendix C`) are clickable and jump to the target within the same panel.
- A **search bar** that does full-text (Postgres `tsvector` + `pg_trgm` fuzzy) and a hybrid semantic + lexical endpoint backed by `intfloat/e5-large-v2` embeddings, scoped to one book or the whole corpus.
- A **"View PDF page" link** on every section that renders the exact original page as a PNG from the stored PDF bytes. Opt-in — you see clean code text first, not a PDF viewer. Rendered PNGs + raw PDF bytes are LRU-cached in-process with ETag/304 support and ±2-page prefetch so flipping pages is instant after the first render.
- An **AI chat** panel that talks to local Ollama models (gemma/qwen/mistral/deepseek) with the indexed codes as grounding, or Anthropic Claude if you drop in an API key.
- A **quarantine queue** for sections that failed extraction validation, plus a page-level flag button in the Review panel — "this page came out wrong" — so you can re-OCR or hand-edit later.
- A **live import dashboard** that streams _every_ stage of the ingest in real time: byte-upload bar, then parse phase with *current page / total pages + OCR fallback counter*, then index phase with *current section number + cumulative references extracted*. Every stat (pages/sec, sections/sec, ETA, elapsed-per-phase) ticks in-place so you always know what the worker is doing on a multi-minute OCR-heavy book.
- **Light & dark themes** with a persistent settings panel — the logo, surface palette, and chrome all swap together; the scale is driven by CSS variables so every Tailwind `bg-surface-*` / `text-surface-*` class flips the moment you toggle.

## The ingestion pipeline (how a PDF becomes code you can read)

```
   upload                   ┌──────────────────────────────────┐
    PDF   ──►  backend  ──► │  1. Store raw bytes              │──► code_book_pdfs (bytea)
            (FastAPI)       │  2. TOC discovery                │    fitz.get_toc() → visual fallback → OCR
                            │  3. Page-target verification     │    validate each entry against page text
                            │  4. Body slicing (per TOC entry) │    PyMuPDF + Tesseract fallback
                            │  5. Running header/footer strip  │    frequency-based
                            │  6. Text normalization           │ ──► store full_text (clean) + full_text_raw
                            │  7. 4-layer content validation   │    format / garbage / structure / integrity
                            │  8. Embed (E5-large-v2, 1024d)   │ ──► code_sections.embedding
                            │  9. Cross-reference extraction   │ ──► code_references
                            │  10. Canonical dedup across codes│ ──► code_sections.canonical_section_id
                            └──────────────────────────────────┘
```

Every step is live-streamed into `import_logs` with a `phase` + fine-grained counters (`current_page`, `total_pages`, `ocr_pages_count`, `toc_entries_count`, `current_section_number`, `references_found`, `stage_detail`, per-phase `started_*_at`), so the Imports panel can show a real progress bar with a phase timeline and a dense stats grid — not a spinner.

### Why the text looks right

PDFs encode visual line wraps, not semantic paragraphs — `page.get_text()` gives you
`"The design wind loads for\nbuildings and other structures..."` with a newline mid-sentence. Step 6 in the pipeline **normalizes every section at ingest time**: soft wraps collapse to single spaces, hyphenated wraps rejoin into single words (`"build-\ning"` → `"building"`), and real paragraph boundaries survive — blank lines, numbered/lettered list items (`1.`, `2.`, `(a)`), bullets, and callouts (`EXCEPTION:`, `User Note:`, `COMMENTARY:`). The output goes into `code_sections.full_text` as clean prose; the untouched PDF extraction is preserved in `full_text_raw` for audit and re-normalization. Search, embeddings, chat context, and the browser all read `full_text`, so search matches like `"wind loads for buildings"` work even when that phrase straddled a page wrap in the original.

Every future upload runs through the same normalization. Existing books are brought up to date with `python -m scripts.renormalize --book-id <N>`.

## Architecture

```
                           ┌─────────────────────────┐
  browser ─► frontend:3010 │ React + TS + Tailwind   │
            (nginx proxy)  │ Panels: Browser / Chat  │
                           │        Dashboard / …    │
                           └──────────┬──────────────┘
                                      │ /api/*
                                      ▼
                           ┌─────────────────────────┐
                           │  backend:8010           │   Postgres 17
                           │  FastAPI + asyncpg      │◄─ + pgvector
                           │  - upload & parse       │   + pg_trgm
                           │  - search / hybrid      │
                           │  - chat (streaming)     │
                           │  - reindex / quarantine │
                           └──┬─────────────┬────────┘
                              │             │
                              ▼             ▼
                     embedding:8011      Ollama (host)
                     E5-large-v2         + optional Claude
                     (1024-d vectors)
                              
                     scraper-service:8012  (Phase 5 — idle)
```

### Core tables

| Table | Contents |
|---|---|
| `code_books` | One row per published edition (ASCE 7-22, CBC 2022, IBC 2021, …) with publisher + cycle. |
| `code_book_pdfs` | Uploaded PDF bytes + SHA-256 + filename. Kept forever as the authoritative source. |
| `code_sections` | Extracted sections: number, title, `full_text` (clean), `full_text_raw` (PDF), `depth`, `page_number`, `chapter`, `embedding vector(1024)`, `normalized_hash`, `canonical_section_id`. |
| `code_section_versions` | Append-only version history per section. |
| `code_references` | Cross-refs detected in body text (internal section → section, external → standard). |
| `import_logs` / `import_sources` | Per-upload ingest state machine with phase + counters. |
| `content_quarantine` | Content that failed validation, grouped by layer with metadata for manual review. |
| `external_standards` / `topics` / `section_topics` | Auxiliary metadata. |

### Vector + text search

- `code_sections.embedding` is HNSW-indexed (`vector_cosine_ops`) for k-NN semantic search.
- `code_sections.full_text` has a `gin_trgm_ops` index for fuzzy matching and a `to_tsvector('english', full_text)` GIN for classic FTS.
- `/api/sections/search` does lexical with ILIKE + filters; `/api/sections/hybrid-search` blends trigram + embedding similarity.

## Quick start

Requires Docker, Docker Compose, and an existing `postgres-stack_default` network with a Postgres 17 container that has `pgvector` and `pg_trgm` installed.

```bash
# One-time database setup
docker exec postgres psql -U postgres -c \
  "CREATE DATABASE building_code OWNER postgres;"
docker exec postgres psql -U postgres -d building_code -c \
  "CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS pg_trgm;"
docker exec -i postgres psql -U postgres -d building_code < backend/schema.sql
for m in backend/migrations/*.sql; do
  docker exec -i postgres psql -U postgres -d building_code < "$m"
done

# Build + run
cp .env.example .env     # set CLAUDE_API_KEY if you want the Claude path
docker compose up -d --build
```

Services:
| URL | What |
|---|---|
| http://localhost:3010 | Frontend (open this) |
| http://localhost:8010 | Backend API (`/openapi.json` for the spec) |
| http://localhost:8011 | Embedding service |
| http://localhost:8012 | Scraper service (Phase 5 — idle) |

### Upload your first code

1. Open http://localhost:3010, go to the **Catalog** panel.
2. Find or create the book row (`ASCE/SEI 7-22`, etc.), click **Upload PDF**.
3. Watch the **Imports** panel — it streams the byte-upload bar → per-page parse progress (with an OCR fallback counter) → per-section indexing (with the current section number + reference count) → completion. A phase timeline strip (Upload → Queue → Parse → Index → Done) stays in sync with the bar.
4. When done, switch to the **Browser** panel, pick the book, click any chapter, read.

## Project layout

```
backend/
  main.py                       # FastAPI app — every HTTP endpoint
  schema.sql                    # initial schema
  migrations/                   # numbered, additive, idempotent
  parsers/
    toc_extractor.py            # Stage A — outline discovery + page verification
    document_extractor.py       # Stage B/C — body slicing + header scrub
    text_normalizer.py          # soft-wrap collapse, list preservation, hyphen repair
    reference_extractor.py      # cross-ref detection in body text
    pdf_parser.py               # legacy Title-24 parser (retained, unused)
  services/import_service.py    # ingest orchestration: validate → insert → embed → dedup
  validators/content_validator.py  # 4-layer validation pipeline
  scripts/
    renormalize.py              # backfill: re-normalize + re-embed existing rows
  scrapers/                     # Phase 5 — ICC / gov-source scrapers (skeletal)

embedding-service/              # E5-large-v2 sidecar (FastAPI + sentence-transformers)

frontend/
  src/
    panels/                     # Browser, Import, Catalog, Review, Chat,
                                # Dashboard, Quarantine
    hooks/                      # useBrowser, useCatalog, useImports,
                                # useReview, useChat, useHealth, useStats
    api/                        # typed fetch client + response shapes

docker-compose.yml
.env
STARTUP.md                      # operational deep-dive
```

## Development

- **Re-ingest a PDF** (after parser or normalizer changes):
  ```bash
  curl -X POST http://localhost:8010/api/code-book-pdfs/{pdf_id}/reindex
  ```
- **Re-normalize existing rows** (after just text-reflow changes — no re-ingest needed):
  ```bash
  docker exec backend python -m scripts.renormalize --book-id 148
  # or: --all-stale to catch any book with NULL full_text_raw
  # or: --no-embed for a dry-run that skips the embedding-service calls
  ```
- **Tail ingest logs** per service:
  ```bash
  docker compose logs -f backend
  ```
- **Hot-iterate on frontend**: mount the source, run `npm run dev` in `frontend/`, or `docker compose build frontend && docker compose up -d --force-recreate frontend` for a full production build.

## Validation state

Validated end-to-end against **ASCE/SEI 7-22** (1,046 pages, 188 MB PDF):

- **2,129 sections indexed** from **2,526 TOC entries** (84%).
- Chapter 26 (Wind Loads) fully recovered: real section titles, real body prose, page numbers attribute to actual PDF pages (clickable "View PDF page 322" for 26.1 etc.).
- Cross-references inside body text resolve to sibling sections (`Chapter 31`, `Section 26.12.3.2`, `Figure 26.5-1B`).
- Remaining ~16% are quarantined as short boilerplate / near-duplicates by the existing integrity validator; tightening that threshold is the next follow-up.

## Roadmap

- ✅ **Phase 1 — PDF ingestion.** TOC-driven extractor, OCR fallback, canonical dedup, normalization. Validated against ASCE 7-22.
- ✅ **Phase 2/3 — Database + Browser UI.** Hierarchical schema, vector + trigram indexes, UpCodes-style single-window reader with inline reference linking.
- ⏳ **Phase 4 — Close the 16% validation gap.** Relax within-book dedup + "insufficient code structure" rejections for legitimate short sections.
- ⏳ **Phase 5 — Web scraping.** ICC + gov-source scrapers. When a web version is newer than the PDF, the web text becomes authoritative for display; the PDF remains the permanent backup.
- ⏳ **Phase 6 — AI chat with enforced citations.** Streaming already works; needs hard citation grounding so answers always reference `code_book_id + section_number` and never hallucinate a section.
- ⏳ **Phase 7 — Dashboard polish.** Per-book coverage, quarantine trends, cross-code duplicate explorer, re-ingest from the UI with one click.

## License

Personal project; no license file yet. If you'd like to use or fork, open an issue.
