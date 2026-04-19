-- Store the raw PDF-extracted body text separately from the normalized
-- prose so consumers (search, embeddings, LLM chat, UI) can rely on
-- full_text being clean while full_text_raw preserves audit-grade source.
-- Additive, idempotent; safe to re-run.

ALTER TABLE code_sections
    ADD COLUMN IF NOT EXISTS full_text_raw TEXT;

-- For existing rows we have no prior normalized form — the backfill
-- script (backend/scripts/renormalize.py) moves the current full_text
-- into full_text_raw and writes the normalized output back to full_text.
-- Until that runs, readers will see the raw version under both columns,
-- which is a strict subset of current behavior.
