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
