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
