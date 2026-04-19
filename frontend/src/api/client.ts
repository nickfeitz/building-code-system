// Lightweight fetch wrapper. Same-origin via nginx proxy in production,
// localhost:8010 via Vite dev proxy in `npm run dev`.

const API_BASE = "/api";

export class ApiError extends Error {
  status: number;
  /** Parsed body, when the server sent structured JSON (e.g. 409 dup info). */
  body: unknown;
  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export interface DuplicateUploadDetail {
  status: "duplicate";
  message: string;
  pdf_id: number;
  filename: string;
  uploaded_at: string | null;
  current_sections: number;
  code_book_id: number;
  code_name: string;
}

/** Narrow an ApiError to the 409 duplicate shape if it looks like one. */
export function asDuplicate(e: unknown): DuplicateUploadDetail | null {
  if (!(e instanceof ApiError) || e.status !== 409) return null;
  const b = e.body as { detail?: DuplicateUploadDetail } | null;
  if (b && b.detail && b.detail.status === "duplicate") return b.detail;
  return null;
}

async function failFromResponse(r: Response): Promise<never> {
  // Preserve the parsed body (typically FastAPI's {"detail": ...}) so
  // callers can narrow on structured error payloads like 409 duplicate.
  const text = await r.text();
  let parsed: unknown = null;
  try { parsed = JSON.parse(text); } catch { /* leave null */ }
  const msg =
    (parsed && typeof parsed === "object" && "detail" in (parsed as object)
      ? ((parsed as { detail?: unknown }).detail)
      : null);
  const message =
    typeof msg === "string"
      ? msg
      : msg && typeof msg === "object" && "message" in (msg as object)
        ? String((msg as { message?: unknown }).message ?? r.statusText)
        : text || r.statusText;
  throw new ApiError(r.status, message, parsed);
}

export async function apiGet<T>(path: string): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`);
  if (!r.ok) await failFromResponse(r);
  return r.json() as Promise<T>;
}

export async function apiPost<T>(
  path: string,
  body?: unknown,
  init?: RequestInit,
): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
    ...init,
  });
  if (!r.ok) await failFromResponse(r);
  // Some endpoints return empty bodies
  const text = await r.text();
  return (text ? JSON.parse(text) : (undefined as unknown)) as T;
}

export async function apiUpload<T>(path: string, formData: FormData): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    body: formData,
  });
  if (!r.ok) throw new ApiError(r.status, await r.text());
  return r.json() as Promise<T>;
}

export interface UploadResponse {
  filename: string;
  status: string;
  import_log_id: number;
}

export interface UploadProgress {
  /** bytes uploaded so far */
  loaded: number;
  /** total bytes (may be 0 if unknown) */
  total: number;
  /** 0..1 */
  fraction: number;
  /** bytes per second (running average over the whole upload) */
  bytesPerSec: number;
  /** elapsed ms since upload start */
  elapsedMs: number;
}

/** URL for a rendered PDF page; used as an `<img src>`. */
export function pdfPageImageUrl(pdfId: number, page: number, dpi: number): string {
  return `${API_BASE}/code-book-pdfs/${pdfId}/pages/${page}.png?dpi=${dpi}`;
}

export interface ReindexResponse {
  status: string;
  import_log_id: number;
  pdf_id: number;
  code_book_id: number;
  filename: string;
  superseded_sections: number;
}

/**
 * Re-parse an already-stored PDF. No new upload; the backend loads the
 * bytes straight from code_book_pdfs and runs them through the parser
 * pipeline again. Current (non-superseded) sections for the book are
 * stamped superseded first, so the new parse produces a fresh generation.
 */
export async function reindexPdf(pdfId: number): Promise<ReindexResponse> {
  return apiPost<ReindexResponse>(`/code-book-pdfs/${pdfId}/reindex`);
}

/**
 * Upload a PDF into the given code_book. The backend expects `code_book_id`
 * as a query param (not a form field). Uses XHR so we can report progress
 * via `upload.onprogress`; fetch() does not expose this.
 *
 * The returned Promise resolves once the server has received the full body
 * and created the import_logs row; the actual parse/embed/index happens
 * asynchronously on the backend and should be tracked via /api/imports.
 */
export function uploadPdf(
  codeBookId: number,
  file: File,
  onProgress?: (p: UploadProgress) => void,
): Promise<UploadResponse> & { abort: () => void } {
  const xhr = new XMLHttpRequest();
  const started = performance.now();
  const q = new URLSearchParams({ code_book_id: String(codeBookId) });
  const fd = new FormData();
  fd.append("file", file);

  const promise = new Promise<UploadResponse>((resolve, reject) => {
    xhr.open("POST", `${API_BASE}/import/upload?${q.toString()}`);
    xhr.responseType = "text";

    xhr.upload.onprogress = (e) => {
      if (!onProgress) return;
      const elapsedMs = performance.now() - started;
      onProgress({
        loaded: e.loaded,
        total: e.lengthComputable ? e.total : 0,
        fraction: e.lengthComputable && e.total > 0 ? e.loaded / e.total : 0,
        bytesPerSec: elapsedMs > 0 ? (e.loaded / elapsedMs) * 1000 : 0,
        elapsedMs,
      });
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText) as UploadResponse);
        } catch (e) {
          reject(new ApiError(xhr.status, `invalid JSON: ${String(e)}`));
        }
      } else {
        // Preserve the parsed body so callers (Import/Catalog panels) can
        // distinguish 409 "duplicate" payloads from generic errors.
        let parsed: unknown = null;
        try { parsed = JSON.parse(xhr.responseText); } catch { /* leave null */ }
        const msg =
          (parsed && typeof parsed === "object" && "detail" in (parsed as object)
            ? ((parsed as { detail?: unknown }).detail)
            : null);
        const text =
          typeof msg === "string"
            ? msg
            : msg && typeof msg === "object" && "message" in (msg as object)
              ? String((msg as { message?: unknown }).message ?? xhr.statusText)
              : xhr.responseText || xhr.statusText;
        reject(new ApiError(xhr.status, text, parsed));
      }
    };

    xhr.onerror = () => reject(new ApiError(0, "network error"));
    xhr.onabort = () => reject(new ApiError(0, "upload aborted"));

    xhr.send(fd);
  }) as Promise<UploadResponse> & { abort: () => void };

  promise.abort = () => xhr.abort();
  return promise;
}

// Streaming chat: backend returns a plain-text StreamingResponse
// (chunks of model tokens, no SSE framing). We expose an async iterator.
export async function* streamChat(
  message: string,
  opts?: { model?: string; useClaude?: boolean; sessionId?: number; codeBookId?: number },
  signal?: AbortSignal,
): AsyncGenerator<string, void, unknown> {
  const r = await fetch(`${API_BASE}/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      message,
      model: opts?.model,
      use_claude: opts?.useClaude ?? false,
      session_id: opts?.sessionId,
      code_book_id: opts?.codeBookId,
    }),
    signal,
  });
  if (!r.ok || !r.body) {
    const text = r.body ? await r.text() : "";
    throw new ApiError(r.status, text || `chat request failed (${r.status})`);
  }
  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) return;
    if (value) yield decoder.decode(value, { stream: true });
  }
}
