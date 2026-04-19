import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { reindexPdf, ApiError } from "../api/client";
import type { ImportJob } from "../api/types";

function PhaseBadge({ phase, status }: { phase: string; status: string }) {
  const tone = ({
    queued: "bg-surface-400 text-surface-50",
    parsing: "bg-warn/20 text-warn",
    indexing: "bg-accent/20 text-accent",
    completed: "bg-success/20 text-success",
    failed: "bg-danger/20 text-danger",
  } as Record<string, string>)[phase] ?? "bg-surface-400 text-surface-50";
  const active = phase === "parsing" || phase === "indexing" || phase === "queued";
  const label = phase === "queued" && status === "processing" ? "queued" : phase;
  return (
    <span className={`badge ${tone} ${active ? "animate-pulse" : ""}`}>
      {label}
    </span>
  );
}

function formatBytes(n: number | null): string {
  if (!n) return "—";
  const u = ["B", "KB", "MB", "GB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 10 ? 0 : 1)} ${u[i]}`;
}

function formatWhen(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  const now = Date.now();
  const s = Math.round((now - d.getTime()) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return d.toLocaleDateString();
}

export function ProgressBar({ percent, phase }: { percent: number | null; phase: string }) {
  const p = Math.max(0, Math.min(100, percent ?? 0));
  const indeterminate = percent == null;
  const barColor =
    phase === "failed"
      ? "bg-danger"
      : phase === "completed"
        ? "bg-success"
        : "bg-accent";
  return (
    <div className="h-1.5 w-full bg-surface-500 rounded overflow-hidden">
      <div
        className={`h-full transition-all ${barColor} ${
          indeterminate ? "animate-pulse w-1/3" : ""
        }`}
        style={indeterminate ? undefined : { width: `${p}%` }}
      />
    </div>
  );
}

function ReindexButton({ pdfId }: { pdfId: number }) {
  const qc = useQueryClient();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const onClick = async () => {
    if (busy) return;
    setBusy(true);
    setErr(null);
    try {
      await reindexPdf(pdfId);
      qc.invalidateQueries({ queryKey: ["imports"] });
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <button
        type="button"
        onClick={onClick}
        disabled={busy}
        className="underline hover:text-white disabled:opacity-50"
        title="Re-parse the stored PDF for this book"
      >
        {busy ? "queueing…" : "re-index"}
      </button>
      {err && <span className="text-danger ml-2">{err}</span>}
    </>
  );
}

export function ImportsTable({
  rows,
  compact = false,
  emptyMessage = "No imports yet.",
}: {
  rows: ImportJob[] | undefined;
  compact?: boolean;
  emptyMessage?: string;
}) {
  if (!rows) return <div className="text-xs text-surface-100">loading…</div>;
  if (rows.length === 0)
    return <div className="text-xs text-surface-100">{emptyMessage}</div>;

  return (
    <div className="divide-y divide-surface-500">
      {rows.map((r) => {
        const terminal = r.phase === "completed" || r.phase === "failed";
        const pctLabel =
          r.percent != null
            ? `${r.percent}%`
            : r.records_processed
              ? `${r.records_processed}${r.records_total ? ` / ${r.records_total}` : ""}`
              : "";
        return (
          <div key={r.id} className="py-2.5 space-y-1">
            <div className="flex items-baseline gap-2 flex-wrap">
              <PhaseBadge phase={r.phase} status={r.status} />
              <span className="text-sm text-white truncate flex-1 min-w-0">
                {r.book_name ?? (r.source_type === "web_scrape" ? "Catalog scan" : `#${r.id}`)}
              </span>
              <span className="text-xs text-surface-100 shrink-0">
                {formatWhen(r.updated_at ?? r.imported_at)}
              </span>
            </div>
            {r.filename && !compact && (
              <div className="text-xs text-surface-100 truncate">
                📄 {r.filename}
                {r.pdf_size_bytes && ` · ${formatBytes(r.pdf_size_bytes)}`}
              </div>
            )}
            {!terminal && <ProgressBar percent={r.percent} phase={r.phase} />}
            <div className="flex items-center gap-3 text-xs text-surface-100">
              {pctLabel && <span>{pctLabel}</span>}
              {r.records_imported > 0 && (
                <span className="text-success">✓ {r.records_imported}</span>
              )}
              {r.records_failed > 0 && (
                <span className="text-warn">⚠ {r.records_failed}</span>
              )}
              {r.pdf_id && (
                <span className="ml-auto flex items-center gap-3">
                  {(r.phase === "failed" || r.phase === "completed") && (
                    <ReindexButton pdfId={r.pdf_id} />
                  )}
                  <a
                    className="underline hover:text-white"
                    href={`/api/code-book-pdfs/${r.pdf_id}/content`}
                    target="_blank"
                    rel="noreferrer"
                    title="Download stored PDF"
                  >
                    download
                  </a>
                </span>
              )}
            </div>
            {r.error_message && (
              <div className="text-xs text-danger truncate" title={r.error_message}>
                ✗ {r.error_message}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
