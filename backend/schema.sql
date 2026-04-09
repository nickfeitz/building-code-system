-- Building Code Intelligence System Schema
-- PostgreSQL with pgvector and pg_trgm extensions

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Code Cycles
CREATE TABLE IF NOT EXISTS code_cycles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,                   -- '2025 California Building Standards'
    effective_date DATE,
    expiration_date DATE,
    status VARCHAR(50) CHECK (status IN ('active','superseded','upcoming')),
    adopting_authority VARCHAR(255),               -- 'State of California'
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Publishing Organizations
CREATE TABLE IF NOT EXISTS publishing_orgs (
    id SERIAL PRIMARY KEY,
    abbreviation VARCHAR(50) NOT NULL UNIQUE,
    full_name VARCHAR(255) NOT NULL,
    website VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Code Books (e.g., Title 24 Part 1, Part 2, etc.)
CREATE TABLE IF NOT EXISTS code_books (
    id SERIAL PRIMARY KEY,
    code_name VARCHAR(255) NOT NULL,              -- '2025 California Building Code'
    abbreviation VARCHAR(50) NOT NULL,            -- 'CBC'
    part_number VARCHAR(10),                      -- 'Part 2' (null for standalone NFPA)
    cycle_id INTEGER NOT NULL REFERENCES code_cycles(id) ON DELETE CASCADE,
    base_model_code VARCHAR(255),                 -- '2024 International Building Code'
    base_model_abbreviation VARCHAR(50),          -- 'IBC'
    base_code_year INTEGER,                       -- 2024
    publishing_org_id INTEGER NOT NULL REFERENCES publishing_orgs(id),
    category VARCHAR(100),                        -- 'Building', 'Fire', 'Electrical'
    digital_access_url TEXT,
    status VARCHAR(50) CHECK (status IN ('active','superseded')),
    effective_date DATE,
    superseded_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(cycle_id, part_number)
);

-- Code Sections
CREATE TABLE IF NOT EXISTS code_sections (
    id SERIAL PRIMARY KEY,
    code_book_id INTEGER NOT NULL REFERENCES code_books(id) ON DELETE CASCADE,
    parent_section_id INTEGER REFERENCES code_sections(id) ON DELETE SET NULL,
    chapter VARCHAR(100),
    section_number VARCHAR(100) NOT NULL,
    section_title VARCHAR(500),
    full_text TEXT NOT NULL,
    section_type VARCHAR(50) DEFAULT 'section',  -- 'section', 'table', 'figure', 'appendix', 'definition'
    depth INTEGER DEFAULT 0,                      -- 0=chapter, 1=section, 2=subsection, 3=sub-subsection
    path VARCHAR(1000),                           -- materialized path "7.706.706.1" for fast tree queries
    display_order INTEGER,
    has_ca_amendment BOOLEAN DEFAULT FALSE,
    amendment_agency VARCHAR(50),                  -- '[HCD]', '[SFM]', '[DSA]', '[OSHPD]', '[BSC-CG]'
    amendment_notes TEXT,
    effective_date DATE,
    superseded_date DATE,
    source_url TEXT,
    source_hash VARCHAR(64),                      -- SHA-256 for change detection
    last_verified TIMESTAMPTZ,
    embedding vector(1024),                       -- E5-large-v2 = 1024 dims
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Code Section Versions (audit trail)
CREATE TABLE IF NOT EXISTS code_section_versions (
    id SERIAL PRIMARY KEY,
    code_section_id INTEGER NOT NULL REFERENCES code_sections(id) ON DELETE CASCADE,
    version_number INTEGER NOT NULL,
    content TEXT NOT NULL,
    changed_by VARCHAR(255),
    change_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(code_section_id, version_number)
);

-- External Standards (ASTM, NFPA, etc.)
CREATE TABLE IF NOT EXISTS external_standards (
    id SERIAL PRIMARY KEY,
    standard_id VARCHAR(100) NOT NULL UNIQUE,
    title VARCHAR(500) NOT NULL,
    organization VARCHAR(255),
    year_published INTEGER,
    description TEXT,
    url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Code References (internal and external)
CREATE TABLE IF NOT EXISTS code_references (
    id SERIAL PRIMARY KEY,
    source_section_id INTEGER NOT NULL REFERENCES code_sections(id) ON DELETE CASCADE,
    target_section_id INTEGER REFERENCES code_sections(id) ON DELETE SET NULL,
    external_standard_id INTEGER REFERENCES external_standards(id) ON DELETE SET NULL,
    reference_type VARCHAR(50) NOT NULL,           -- 'mandatory', 'informational', 'exception', 'table', 'figure', 'external_standard', 'cross_part'
    reference_text TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Topics/Tags
CREATE TABLE IF NOT EXISTS topics (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Section Topics (many-to-many)
CREATE TABLE IF NOT EXISTS section_topics (
    section_id INTEGER NOT NULL REFERENCES code_sections(id) ON DELETE CASCADE,
    topic_id INTEGER NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    PRIMARY KEY (section_id, topic_id)
);

-- Import Sources
CREATE TABLE IF NOT EXISTS import_sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL,
    source_url TEXT,
    source_type VARCHAR(50),                      -- 'web_scrape', 'pdf_parse', 'api', 'manual'
    code_book_id INTEGER REFERENCES code_books(id),
    last_crawled TIMESTAMPTZ,
    last_hash VARCHAR(64),
    status VARCHAR(50) DEFAULT 'pending',         -- 'pending', 'crawling', 'completed', 'error', 'changed'
    sections_imported INTEGER DEFAULT 0,
    error_message TEXT,
    crawl_interval_hours INTEGER DEFAULT 168,     -- Weekly default
    next_crawl_at TIMESTAMPTZ,                    -- Smart scheduling based on publication dates
    configuration JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Import Logs
CREATE TABLE IF NOT EXISTS import_logs (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES import_sources(id) ON DELETE CASCADE,
    status VARCHAR(100),
    records_processed INTEGER DEFAULT 0,
    records_imported INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Content Quarantine (4-layer validation pipeline)
CREATE TABLE IF NOT EXISTS content_quarantine (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES import_sources(id) ON DELETE CASCADE,
    validation_layer INTEGER,
    error_message TEXT NOT NULL,
    raw_content TEXT NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    reviewed_by VARCHAR(255),
    action_taken VARCHAR(100)
);

-- Chat Sessions
CREATE TABLE IF NOT EXISTS chat_sessions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255),
    title VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chat Messages
CREATE TABLE IF NOT EXISTS chat_messages (
    id SERIAL PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES chat_sessions(id) ON DELETE CASCADE,
    role VARCHAR(50),
    content TEXT NOT NULL,
    referenced_sections JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User Annotations
CREATE TABLE IF NOT EXISTS user_annotations (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    section_id INTEGER NOT NULL REFERENCES code_sections(id) ON DELETE CASCADE,
    annotation_type VARCHAR(100),
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_code_sections_code_book_id ON code_sections(code_book_id);
CREATE INDEX idx_code_sections_parent_id ON code_sections(parent_section_id);
CREATE INDEX idx_code_sections_chapter ON code_sections(chapter);
CREATE INDEX idx_code_sections_updated_at ON code_sections(updated_at);
CREATE INDEX idx_code_sections_section_number ON code_sections(section_number);
CREATE INDEX idx_code_sections_section_book ON code_sections(section_number, code_book_id);
CREATE INDEX idx_code_sections_fulltext_trgm ON code_sections USING GIN (full_text gin_trgm_ops);
CREATE INDEX idx_code_sections_embedding ON code_sections USING HNSW (embedding vector_cosine_ops);

CREATE INDEX idx_code_references_source ON code_references(source_section_id);
CREATE INDEX idx_code_references_target ON code_references(target_section_id);
CREATE INDEX idx_code_references_external ON code_references(external_standard_id);

CREATE INDEX idx_section_topics_section ON section_topics(section_id);
CREATE INDEX idx_section_topics_topic ON section_topics(topic_id);

CREATE INDEX idx_import_logs_source ON import_logs(source_id);
CREATE INDEX idx_import_logs_status ON import_logs(status);
CREATE INDEX idx_import_logs_imported_at ON import_logs(imported_at);

CREATE INDEX idx_content_quarantine_source ON content_quarantine(source_id);
CREATE INDEX idx_content_quarantine_layer ON content_quarantine(validation_layer);
CREATE INDEX idx_content_quarantine_created ON content_quarantine(created_at);

CREATE INDEX idx_chat_sessions_user ON chat_sessions(user_id);
CREATE INDEX idx_chat_sessions_created ON chat_sessions(created_at);

CREATE INDEX idx_chat_messages_session ON chat_messages(session_id);
CREATE INDEX idx_chat_messages_created ON chat_messages(created_at);

CREATE INDEX idx_user_annotations_user ON user_annotations(user_id);
CREATE INDEX idx_user_annotations_section ON user_annotations(section_id);
CREATE INDEX idx_user_annotations_created ON user_annotations(created_at);

-- Composite index for common searches
CREATE INDEX idx_code_sections_composite ON code_sections(code_book_id, chapter, updated_at DESC);
CREATE INDEX idx_code_sections_source_hash ON code_sections(source_hash);
CREATE INDEX idx_code_sections_amendment ON code_sections(has_ca_amendment) WHERE has_ca_amendment = TRUE;

-- Full-text search indexes
CREATE INDEX idx_code_sections_search ON code_sections USING GIN (to_tsvector('english', full_text));
