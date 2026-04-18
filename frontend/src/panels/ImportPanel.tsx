import { useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { uploadPdf, ApiError, asDuplicate, type UploadProgress } from "../api/client";
import type { CatalogBook, CatalogResponse } from "../api/types";
import { useCatalog } from "../hooks/useCatalog";
import { useImports } from "../hooks/useImports";
import { ImportsTable, ProgressBar } from "../components/ImportsTable";

/** Compact picker rendering the catalog as an optgroup-free searchable list. */
function BookPicker({
  catalog,
  value,
  onChange,
}: {
  catalog: CatalogResponse | undefined;
  value: CatalogBook | null;
  onChange: (b: CatalogBook | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");

  const flat = useMemo(() => {
    if (!catalog) return [];
    const out: { book: CatalogBook; authority: string; cycle: string }[] = [];
    for (const a of catalog.authorities) {
      for (const c of a.cycles) {
        for (const b of c.books) {
          out.push({ book: b, authority: a.adopting_authority, cycle: c.name });
        }
      }
    }
    return out;
  }, [catalog]);

  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    if (!needle) return flat;
    return flat.filter(({ book, authority }) => {
      const hay = `${book.code_name} ${book.abbreviation} ${authority} ${book.category ?? ""} ${book.base_model_abbreviation ?? ""}`.toLowerCase();
      return hay.includes(needle);
    });
  }, [flat, q]);

  const grouped = useMemo(() => {
    const m = new Map<string, typeof filtered>();
    for (const item of filtered) {
      if (!m.has(item.authority)) m.set(item.authority, []);
      m.get(item.authority)!.push(item);
    }
    return [...m.entries()];
  }, [filtered]);

  return (
    <div className="relative">
      <button
        type="button"
        className="input flex items-center justify-between w-full text-left"
        onClick={() => setOpen((o) => !o)}
      >
        <span className={value ? "text-white truncate" : "text-surface-200"}>
          {value ? value.code_name : "Pick a code book…"}
        </span>
        <span className="text-surface-100 text-xs ml-2">{open ? "▾" : "▸"}</span>
      </button>

      {open && (
        <div className="absolute z-20 mt-1 w-full max-h-96 overflow-y-auto bg-surface-800 border border-surface-400 rounded shadow-lg">
          <div className="sticky top-0 bg-surface-800 border-b border-surface-400 p-2">
            <input
              autoFocus
              className="input !py-1 text-sm"
              placeholder="Search 178 books…"
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
            <div className="mt-1 text-xs text-surface-100">
              {filtered.length} match{filtered.length === 1 ? "" : "es"}
              {value && (
                <button
                  type="button"
                  className="ml-3 underline hover:text-white"
                  onClick={() => {
                    onChange(null);
                    setOpen(false);
                  }}
                >
                  clear selection
                </button>
              )}
            </div>
          </div>
          {grouped.length === 0 && (
            <div className="p-3 text-xs text-surface-100">No books match.</div>
          )}
          {grouped.map(([authority, items]) => (
            <div key={authority}>
              <div className="px-3 py-1.5 bg-surface-900 text-xs uppercase tracking-wider text-surface-100">
                {authority}
              </div>
              {items.map(({ book, cycle }) => (
                <button
                  type="button"
                  key={book.id}
                  onClick={() => {
                    onChange(book);
                    setOpen(false);
                  }}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-surface-700 ${
                    value?.id === book.id ? "bg-accent/20" : ""
                  }`}
                >
                  <div className="text-white truncate">{book.code_name}</div>
                  <div className="text-xs text-surface-100 mt-0.5">
                    {cycle}
                    {book.part_number && ` · ${book.part_number}`}
                    {book.base_model_abbreviation &&
                      ` · based on ${book.base_model_abbreviation}${
                        book.base_code_year ? " " + book.base_code_year : ""
                      }`}
                  </div>
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function formatBytes(n: number): string {
  const u = ["B", "KB", "MB", "GB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 10 ? 0 : 1)} ${u[i]}`;
}

function formatRate(bps: number): string {
  return `${formatBytes(bps)}/s`;
}

function etaSeconds(p: UploadProgress): number | null {
  if (!p.total || p.bytesPerSec <= 0) return null;
  return Math.max(0, (p.total - p.loaded) / p.bytesPerSec);
}

export function ImportPanel() {
  const catalog = useCatalog();
  const imports = useImports(25);
  const qc = useQueryClient();

  const [book, setBook] = useState<CatalogBook | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [busy, setBusy] = useState(false);
  const [progress, setProgress] = useState<UploadProgress | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const abortRef = useRef<(() => void) | null>(null);

  const upload = async () => {
    if (!file || !book) return;
    setBusy(true);
    setMsg(null);
    setProgress({ loaded: 0, total: file.size, fraction: 0, bytesPerSec: 0, elapsedMs: 0 });
    const handle = uploadPdf(book.id, file, setProgress);
    abortRef.current = handle.abort;
    try {
      const r = await handle;
      setMsg(
        `Uploaded ${r.filename} → ${book.code_name}. Server queued import_log_id=${r.import_log_id}; watch its progress below.`
      );
      setFile(null);
      setProgress(null);
      qc.invalidateQueries({ queryKey: ["imports"] });
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    } catch (e) {
      const dup = asDuplicate(e);
      if (dup) {
        setMsg(
          `✓ Already uploaded — identical bytes were stored on ${dup.uploaded_at ? new Date(dup.uploaded_at).toLocaleString() : "a prior upload"} as ${dup.filename}. ` +
          `${dup.current_sections} current section${dup.current_sections === 1 ? "" : "s"} indexed for this book. Nothing to do.`
        );
        setProgress(null);
      } else {
        setMsg(
          e instanceof ApiError
            ? `Upload failed (${e.status}): ${e.message}`
            : String(e)
        );
      }
    } finally {
      setBusy(false);
      abortRef.current = null;
    }
  };

  const cancel = () => abortRef.current?.();

  return (
    <div className="h-full overflow-y-auto p-6 space-y-6">
      <div className="card">
        <h2 className="text-sm font-semibold text-white mb-3">Upload PDF</h2>
        <p className="text-xs text-surface-100 mb-3">
          Pick the code this PDF is for, then choose a file. The PDF is stored
          in Postgres and the parsed sections are linked to that book + indexed
          for chat and browse. Watch the progress below during both upload and
          server-side processing.
        </p>

        <div className="space-y-3">
          <div>
            <div className="text-xs text-surface-100 mb-1">Code book</div>
            <BookPicker catalog={catalog.data} value={book} onChange={setBook} />
          </div>

          <div>
            <div className="text-xs text-surface-100 mb-1">PDF file</div>
            <input
              type="file"
              accept=".pdf"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              className="input"
            />
          </div>

          <div className="flex items-center gap-3">
            {busy ? (
              <button type="button" onClick={cancel} className="btn-ghost">
                Cancel upload
              </button>
            ) : (
              <button
                onClick={upload}
                disabled={!file || !book}
                className="btn-primary"
              >
                Upload
              </button>
            )}
            {book && (
              <div className="text-xs text-surface-100">
                → {book.code_name}
                {book.part_number ? ` (${book.part_number})` : ""}
              </div>
            )}
          </div>

          {progress && (
            <div className="space-y-1 mt-2">
              <ProgressBar percent={Math.round(progress.fraction * 100)} phase="indexing" />
              <div className="flex items-center justify-between text-xs text-surface-100">
                <span>
                  {formatBytes(progress.loaded)}
                  {progress.total ? ` / ${formatBytes(progress.total)}` : ""}
                  {progress.total ? ` · ${Math.round(progress.fraction * 100)}%` : ""}
                </span>
                <span>
                  {formatRate(progress.bytesPerSec)}
                  {etaSeconds(progress) != null &&
                    ` · ETA ${Math.round(etaSeconds(progress)!)}s`}
                </span>
              </div>
            </div>
          )}
          {msg && <div className="text-xs text-surface-50">{msg}</div>}
        </div>
      </div>

      <div className="card">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold text-white">Recent Imports</h2>
          <span className="text-xs text-surface-100">
            auto-refreshing · 2 s while active
          </span>
        </div>
        <ImportsTable rows={imports.data} />
      </div>
    </div>
  );
}
