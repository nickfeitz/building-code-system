// Lightweight fetch wrapper. Same-origin via nginx proxy in production,
// localhost:8010 via Vite dev proxy in `npm run dev`.

const API_BASE = "/api";

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function apiGet<T>(path: string): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`);
  if (!r.ok) throw new ApiError(r.status, await r.text());
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
  if (!r.ok) throw new ApiError(r.status, await r.text());
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
        reject(new ApiError(xhr.status, xhr.responseText || xhr.statusText));
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
