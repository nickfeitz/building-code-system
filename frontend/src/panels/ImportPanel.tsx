import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiGet, apiUpload, ApiError } from "../api/client";
import type { ImportStatusItem, CodeBook } from "../api/types";

export function ImportPanel() {
  const [file, setFile] = useState<File | null>(null);
  const [bookId, setBookId] = useState<number | "">("");
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const books = useQuery({
    queryKey: ["code-books"],
    queryFn: () => apiGet<CodeBook[]>("/code-books"),
    staleTime: 60_000,
  });

  const status = useQuery({
    queryKey: ["import-status"],
    queryFn: () => apiGet<ImportStatusItem[]>("/import/status"),
    refetchInterval: 5_000,
  });

  const upload = async () => {
    if (!file) return;
    setBusy(true);
    setMsg(null);
    try {
      const fd = new FormData();
      fd.append("file", file);
      if (bookId) fd.append("code_book_id", String(bookId));
      await apiUpload("/import/upload", fd);
      setMsg(`Uploaded ${file.name}. Watch the status table below.`);
      setFile(null);
    } catch (e) {
      setMsg(e instanceof ApiError ? `Upload failed (${e.status}): ${e.message}` : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="h-full overflow-y-auto p-6 space-y-6">
      <div className="card">
        <h2 className="text-sm font-semibold text-white mb-3">Upload PDF</h2>
        <div className="grid gap-3 sm:grid-cols-[1fr_240px_auto]">
          <input
            type="file"
            accept=".pdf"
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
            className="input"
          />
          <select
            className="input"
            value={bookId}
            onChange={(e) => setBookId(e.target.value ? Number(e.target.value) : "")}
          >
            <option value="">(no code book)</option>
            {(books.data ?? []).map((b) => (
              <option key={b.id} value={b.id}>
                {b.title || b.name || b.abbreviation || `#${b.id}`}
              </option>
            ))}
          </select>
          <button onClick={upload} disabled={!file || busy} className="btn-primary">
            {busy ? "Uploading…" : "Upload"}
          </button>
        </div>
        {msg && <div className="text-xs text-surface-100 mt-2">{msg}</div>}
      </div>

      <div className="card">
        <h2 className="text-sm font-semibold text-white mb-3">Recent Imports</h2>
        {status.isLoading && <div className="text-xs text-surface-100">loading…</div>}
        {status.data && status.data.length === 0 && (
          <div className="text-xs text-surface-100">No imports yet.</div>
        )}
        <div className="divide-y divide-surface-500">
          {(status.data ?? []).map((it) => (
            <div key={it.id} className="py-2 flex items-center justify-between text-sm">
              <div>
                <div className="text-white">{it.source_name ?? `import #${it.id}`}</div>
                {it.message && <div className="text-xs text-surface-100">{it.message}</div>}
              </div>
              <div className="text-xs text-surface-100">{it.status}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
