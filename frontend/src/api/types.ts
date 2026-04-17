// Backend response shapes (subset — only what the UI consumes).

export interface HealthResponse {
  status: "healthy" | "degraded" | "unhealthy";
  database: string;
  embedding_service: string;
  claude_api: string;
  llm_provider: string;
  ollama?: {
    available: boolean;
    models: string[];
    url: string;
  };
}

export interface StatsResponse {
  total_sections: number;
  total_references: number;
  pending_quarantine: number;
  code_books: number;
  external_standards: number;
  topics: number;
}

export interface LLMStatusResponse {
  provider: "ollama" | "claude";
  model: string;
  claude_available: boolean;
  ollama_available: boolean;
  available_models: string[];
}

export interface CodeBook {
  id: number;
  title?: string;
  name?: string;
  abbreviation?: string;
  publishing_org_id?: number;
  cycle_id?: number;
}

export interface SectionSearchHit {
  id: number;
  section_number: string;
  section_title: string;
  full_text?: string;
  text?: string;
  similarity?: number;
  rank?: number;
}

export interface ImportStatusItem {
  id: number;
  source_name?: string;
  status: string;
  started_at?: string;
  finished_at?: string;
  message?: string;
  records_processed?: number;
}

export interface QuarantineItem {
  id: number;
  source: string;
  reason: string;
  content?: string;
  created_at?: string;
}

// --- Catalog ---------------------------------------------------------------

export type ScanStatus =
  | "not_scanned"
  | "crawling"
  | "indexed"
  | "error"
  | string;

export interface CatalogBook {
  id: number;
  code_name: string;
  abbreviation: string;
  part_number: string | null;
  category: string | null;
  base_model_abbreviation: string | null;
  base_code_year: number | null;
  digital_access_url: string | null;
  status: "active" | "superseded" | "upcoming" | string;
  effective_date: string | null;
  superseded_date: string | null;
  indexed_section_count: number;
  scan_status: ScanStatus;
  source_id: number | null;
  last_crawled: string | null;
}

export interface CatalogCycle {
  id: number;
  name: string;
  effective_date: string | null;
  expiration_date: string | null;
  status: "active" | "superseded" | "upcoming" | string;
  books: CatalogBook[];
}

export interface CatalogAuthority {
  adopting_authority: string;
  publishing_org_abbr: string;
  publishing_org_full_name: string;
  cycles: CatalogCycle[];
}

export interface CatalogResponse {
  authorities: CatalogAuthority[];
}

export interface CatalogScanResponse {
  triggered: {
    code_book_id: number;
    code_name: string;
    source_id: number;
    scraper: string;
  }[];
  skipped_no_url: {
    code_book_id: number;
    code_name: string;
  }[];
  errors: { code_book_id: number; error: string }[];
}

// --- Imports (progress tracking) -------------------------------------------

export type ImportPhase =
  | "queued"
  | "parsing"
  | "indexing"
  | "completed"
  | "failed"
  | string;

export interface ImportJob {
  id: number;
  source_id: number;
  source_type: string | null;
  status: string;
  phase: ImportPhase;
  code_book_id: number | null;
  book_name: string | null;
  book_abbreviation: string | null;
  book_part_number: string | null;
  pdf_id: number | null;
  pdf_size_bytes: number | null;
  filename: string | null;
  records_total: number | null;
  records_processed: number;
  records_imported: number;
  records_failed: number;
  percent: number | null;
  error_message: string | null;
  imported_at: string | null;
  updated_at: string | null;
  completed_at: string | null;
}
