-- Track how each import's TOC was obtained and what the pre-flight
-- identity check saw. Both columns are additive and nullable; the
-- identity check hasn't run for any existing log rows, and pre-migration
-- imports that reach these code paths (retry/reindex) will populate them
-- on the next run.
--
-- toc_source       — 'outline' | 'visual' | 'ocr' | 'article' | 'llm' |
--                    NULL (extractor never ran or the run aborted before
--                    a strategy resolved). Useful for debugging why a
--                    particular extraction produced N sections, and for
--                    the UI to surface "parsed via LLM" as a caveat.
-- identity_title   — Title the identity checker extracted from the PDF
--                    cover / front matter (e.g. "2025 California
--                    Electrical Code — Part 3"). Stored verbatim so we
--                    can present a side-by-side diff against the
--                    selected code_books row on mismatch.
-- identity_confidence — 0..1 float, how confident the checker is that
--                    the PDF matches the selected book. <0.7 triggers a
--                    rejected_identity_mismatch phase, preventing the
--                    parser from running against the wrong book.
-- identity_notes   — Free-form JSON string (reason codes, LLM response
--                    payload, etc.) for operator review.

ALTER TABLE import_logs
    ADD COLUMN IF NOT EXISTS toc_source VARCHAR(20),
    ADD COLUMN IF NOT EXISTS identity_title TEXT,
    ADD COLUMN IF NOT EXISTS identity_confidence REAL,
    ADD COLUMN IF NOT EXISTS identity_notes TEXT;
