-- Richer per-phase progress fields on import_logs so the UI can show
-- what the worker is *actually* doing (which page it's on, how many
-- needed OCR, which section it's currently indexing, etc.) instead of
-- just a generic "parsing" / "indexing" label. Additive, idempotent.

ALTER TABLE import_logs
    ADD COLUMN IF NOT EXISTS current_page INTEGER,
    ADD COLUMN IF NOT EXISTS total_pages INTEGER,
    ADD COLUMN IF NOT EXISTS ocr_pages_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS toc_entries_count INTEGER,
    ADD COLUMN IF NOT EXISTS current_section_number VARCHAR(100),
    ADD COLUMN IF NOT EXISTS stage_detail TEXT,
    ADD COLUMN IF NOT EXISTS references_found INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS started_parsing_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS started_indexing_at TIMESTAMPTZ;
