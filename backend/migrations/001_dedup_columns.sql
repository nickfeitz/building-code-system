-- Dedup infrastructure: canonical_section_id + normalized_hash.
-- Idempotent; safe to run on an already-migrated DB.

ALTER TABLE code_sections
    ADD COLUMN IF NOT EXISTS canonical_section_id INTEGER
    REFERENCES code_sections(id) ON DELETE SET NULL;

ALTER TABLE code_sections
    ADD COLUMN IF NOT EXISTS normalized_hash VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_code_sections_normalized_hash
    ON code_sections (normalized_hash);
