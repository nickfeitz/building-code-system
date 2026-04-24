import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  reindexPdf,
  retryImport,
  deleteImport,
  ApiError,
  type UploadProgress,
} from "../api/client";
import type { ImportJob } from "../api/types";

// Phases that mean "the worker stopped without producing usable output".
// Any of these should render with the danger tone and surface a retry
// button. These are emitted by backend/services/import_service.py and the
// done-callback in backend/main.py:_spawn_import_task.
const ERROR_PHASES = new Set([
  "failed",
  "empty_extraction",
  "all_quarantined",
  "no_candidates",
  "crashed",
  "cancelled",
  "rejected_identity_mismatch",
]);

function isErrorJob(j: { status: string; phase: string }): boolean {
  return j.status === "error" || ERROR_PHASES.has(j.phase);
}

// --- shared formatting helpers --------------------------------------------

function formatBytes(n: number | null | undefined): string {
  if (n == null) return "—";
  const u = ["B", "KB", "MB", "GB", "TB"];
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

function formatDuration(ms: number): string {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`;
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m}m ${rs}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
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

// --- phase labels + timeline ----------------------------------------------

function PhaseBadge({ phase, status }: { phase: string; status: string }) {
  // Error phases share the same red tone + short human label so the table
  // reads consistently regardless of which failure mode tripped. The raw
  // phase value still sits on `title` for operators who want the detail.
  const errorLabel: Record<string, string> = {
    failed: "failed",
    empty_extraction: "empty",
    all_quarantined: "quarantined",
    no_candidates: "no sections",
    crashed: "crashed",
    cancelled: "cancelled",
    rejected_identity_mismatch: "wrong book",
  };
  const isError = status === "error" || errorLabel[phase] !== undefined;
  const tone = isError
    ? "bg-danger/20 text-danger"
    : ({
        queued: "bg-surface-400 text-surface-50",
        parsing: "bg-warn/20 text-warn",
        indexing: "bg-accent/20 text-accent",
        completed: "bg-success/20 text-success",
      } as Record<string, string>)[phase] ?? "bg-surface-400 text-surface-50";
  const active = phase === "parsing" || phase === "indexing" || phase === "queued";
  const pulsing = active && !isError;
  const label = isError
    ? errorLabel[phase] ?? "error"
    : phase === "queued" && status === "processing"
      ? "queued"
      : phase;
  return (
    <span
      className={`badge ${tone} ${pulsing ? "animate-pulse" : ""}`}
      title={isError ? `status=${status} phase=${phase}` : undefined}
    >
      {label}
    </span>
  );
}

/**
 * Thin progress bar. Used for compact table rows. For the rich inline
 * progress card (stats, phase timeline, OCR/page counters) use
 * `ImportProgressCard`.
 */
export function ProgressBar({
  percent,
  phase,
}: {
  percent: number | null;
  phase: string;
}) {
  const p = Math.max(0, Math.min(100, percent ?? 0));
  const indeterminate = percent == null;
  const barColor =
    phase === "failed"
      ? "bg-danger"
      : phase === "completed"
        ? "bg-success"
        : phase === "parsing"
          ? "bg-warn"
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

// --- rich progress card ---------------------------------------------------

type StageKey = "upload" | "queued" | "parsing" | "indexing" | "completed" | "failed";

interface StageDef {
  key: StageKey;
  label: string;
  /** Show during this stage as the animated/highlighted chip. */
  shortLabel: string;
}

const STAGES: StageDef[] = [
  { key: "upload", label: "Uploading", shortLabel: "Upload" },
  { key: "queued", label: "Queued", shortLabel: "Queued" },
  { key: "parsing", label: "Parsing PDF", shortLabel: "Parse" },
  { key: "indexing", label: "Indexing sections", shortLabel: "Index" },
  { key: "completed", label: "Done", shortLabel: "Done" },
];

function stageIndex(active: StageKey): number {
  return STAGES.findIndex((s) => s.key === active);
}

/**
 * Phase-timeline strip. Renders all five stages as a row of chips with the
 * current one highlighted, past ones checkmarked, and future ones dimmed.
 * Failed jobs render the failure stage in danger tone.
 */
function PhaseTimeline({
  active,
  failed,
}: {
  active: StageKey;
  failed: boolean;
}) {
  const activeIdx = stageIndex(active);
  return (
    <div className="flex items-center gap-1 text-[11px]">
      {STAGES.map((s, i) => {
        const isPast = activeIdx > i || (active === "completed" && i < STAGES.length);
        const isCurrent = i === activeIdx;
        let tone: string;
        if (failed && isCurrent) {
          tone = "bg-danger/20 text-danger border-danger/40";
        } else if (isCurrent) {
          tone = "bg-accent/20 text-accent border-accent/40 animate-pulse";
        } else if (isPast) {
          tone = "bg-success/15 text-success border-success/30";
        } else {
          tone = "bg-surface-600 text-surface-200 border-surface-400";
        }
        return (
          <div key={s.key} className="flex items-center gap-1">
            <span
              className={`px-2 py-0.5 rounded border ${tone} whitespace-nowrap`}
              title={s.label}
            >
              {isPast && !isCurrent ? "✓ " : ""}
              {s.shortLabel}
            </span>
            {i < STAGES.length - 1 && (
              <span className="text-surface-300">›</span>
            )}
          </div>
        );
      })}
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string | number | null | undefined;
  tone?: "success" | "warn" | "danger" | "accent";
}) {
  if (value == null || value === "") return null;
  const toneClass =
    tone === "success"
      ? "text-success"
      : tone === "warn"
        ? "text-warn"
        : tone === "danger"
          ? "text-danger"
          : tone === "accent"
            ? "text-accent"
            : "text-surface-50";
  return (
    <div className="flex flex-col">
      <div className="text-[10px] uppercase tracking-wider text-surface-200">
        {label}
      </div>
      <div className={`text-sm font-medium tabular-nums ${toneClass}`}>
        {value}
      </div>
    </div>
  );
}

/**
 * Derive elapsed ms since a started-at ISO string; null if missing. Updated
 * by a local tick so the "elapsed" stat ticks up in real time even between
 * API polls.
 */
function useElapsed(startedAtIso: string | null | undefined, active: boolean): number | null {
  const [now, setNow] = useState<number>(() => Date.now());
  useEffect(() => {
    if (!active) return;
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, [active]);
  if (!startedAtIso) return null;
  const t = Date.parse(startedAtIso);
  if (Number.isNaN(t)) return null;
  return Math.max(0, now - t);
}

interface ImportProgressCardProps {
  /** Upload-phase progress from XHR (null once the server has the file). */
  upload?: UploadProgress | null;
  /** Server-side import_log row (null before the upload completes). */
  job?: ImportJob | null;
  /** Optional filename shown when we don't yet have a job row. */
  filename?: string | null;
  /** Show an intrinsic card shell; set false when already inside a `.card`. */
  bare?: boolean;
}

/**
 * Full progress display for a single import: upload bytes → parse pages
 * (with OCR counter) → index sections (with current section #) → done.
 * Renders a phase timeline, a main bar, and a dense grid of counters
 * (pages, OCR, TOC, sections, refs, imported, failed, elapsed, ETA).
 */
export function ImportProgressCard({
  upload,
  job,
  filename,
  bare = false,
}: ImportProgressCardProps) {
  // Determine the active stage. Priority:
  //   1. Byte-upload still streaming → "upload"
  //   2. Server row present → derive from job.phase
  //   3. Neither (we've handed off to the server but the first poll hasn't
  //      landed yet) → "queued"
  const jobPhase: StageKey = (() => {
    if (!job) return upload && upload.fraction < 1 ? "upload" : "queued";
    // Any status='error' row — empty_extraction, all_quarantined, crashed,
    // rejected_identity_mismatch, etc. — collapses to the "failed" stage
    // for timeline + bar colouring. The PhaseBadge keeps the specific
    // label so operators still see the reason at a glance.
    if (isErrorJob(job)) return "failed";
    if (job.phase === "completed") return "completed";
    if (job.phase === "indexing") return "indexing";
    if (job.phase === "parsing") return "parsing";
    return "queued";
  })();
  const active: StageKey =
    upload && upload.fraction < 1 && !job ? "upload" : jobPhase;

  const failed = jobPhase === "failed";
  const isTerminal = jobPhase === "completed" || failed;

  // When failed, figure out which stage the worker was in at the time so
  // the timeline can mark *that* chip red instead of dimming the whole row.
  // Falls back to "parsing" since a failure with no timestamps most likely
  // happened during text extraction.
  const failedDuring: StageKey = (() => {
    if (!failed || !job) return "queued";
    if (job.started_indexing_at) return "indexing";
    if (job.started_parsing_at) return "parsing";
    return "queued";
  })();

  // Main bar percent: choose by stage.
  let percent: number | null = null;
  let barPhase: string = active;
  if (active === "upload" && upload) {
    percent = Math.round(upload.fraction * 100);
    barPhase = "upload";
  } else if (active === "parsing" && job) {
    percent = job.total_pages
      ? Math.round(((job.current_page ?? 0) * 100) / job.total_pages)
      : null;
    barPhase = "parsing";
  } else if (active === "indexing" && job) {
    percent = job.records_total
      ? Math.round(((job.records_processed ?? 0) * 100) / job.records_total)
      : null;
    barPhase = "indexing";
  } else if (active === "completed") {
    percent = 100;
    barPhase = "completed";
  } else if (active === "failed") {
    percent = job?.percent ?? null;
    barPhase = "failed";
  } else if (active === "queued") {
    percent = null;
    barPhase = "queued";
  }

  // Elapsed timers: tick every second while the stage is active so the
  // readout moves between 2-second API polls.
  const uploadElapsed =
    upload && upload.elapsedMs >= 0 ? upload.elapsedMs : null;
  const parseElapsed = useElapsed(
    job?.started_parsing_at ?? null,
    active === "parsing",
  );
  const indexElapsed = useElapsed(
    job?.started_indexing_at ?? null,
    active === "indexing",
  );
  // Total elapsed since worker first touched the row (parsing) or since the
  // upload started if parsing hasn't begun.
  const totalElapsed = useElapsed(
    job?.started_parsing_at ?? job?.imported_at ?? null,
    !isTerminal && !!job,
  );

  // ETA: rate-based.
  //   upload → bytes/sec
  //   parse  → pages/sec (from parseElapsed)
  //   index  → sections/sec (from indexElapsed)
  let etaSeconds: number | null = null;
  if (active === "upload" && upload && upload.total && upload.bytesPerSec > 0) {
    etaSeconds = Math.max(
      0,
      (upload.total - upload.loaded) / upload.bytesPerSec,
    );
  } else if (
    active === "parsing" &&
    job?.current_page &&
    job?.total_pages &&
    parseElapsed &&
    parseElapsed > 1500
  ) {
    const pagesLeft = job.total_pages - job.current_page;
    const rate = job.current_page / (parseElapsed / 1000);
    if (rate > 0 && pagesLeft > 0) etaSeconds = pagesLeft / rate;
  } else if (
    active === "indexing" &&
    job?.records_processed &&
    job?.records_total &&
    indexElapsed &&
    indexElapsed > 1500
  ) {
    const left = job.records_total - job.records_processed;
    const rate = job.records_processed / (indexElapsed / 1000);
    if (rate > 0 && left > 0) etaSeconds = left / rate;
  }

  // Resolve the headline stage description. Prefer the server's
  // `stage_detail` when we have one, fall back to a synthesized label.
  const headline: string = (() => {
    if (active === "upload" && upload) {
      return `Uploading · ${formatBytes(upload.loaded)} of ${formatBytes(
        upload.total ?? null,
      )}`;
    }
    if (job?.stage_detail) return job.stage_detail;
    if (active === "queued") return "Queued — waiting for worker…";
    if (active === "parsing") return "Extracting text from PDF pages…";
    if (active === "indexing") return "Validating, embedding & inserting sections…";
    if (active === "completed") return "Import complete.";
    if (active === "failed") return job?.error_message ?? "Import failed.";
    return "";
  })();

  // Dynamic rate labels to display next to the active phase.
  const rateLabel: string | null = (() => {
    if (active === "upload" && upload && upload.bytesPerSec > 0) {
      return formatRate(upload.bytesPerSec);
    }
    if (active === "parsing" && parseElapsed && job?.current_page) {
      const pps = job.current_page / (parseElapsed / 1000);
      return pps > 0 ? `${pps.toFixed(1)} pages/s` : null;
    }
    if (active === "indexing" && indexElapsed && job?.records_processed) {
      const sps = job.records_processed / (indexElapsed / 1000);
      return sps > 0 ? `${sps.toFixed(1)} sections/s` : null;
    }
    return null;
  })();

  const shell = bare ? "" : "card";
  return (
    <div className={`space-y-3 ${shell}`}>
      {/* Header row: filename + main phase badge + timeline */}
      <div className="flex items-start justify-between gap-3 flex-wrap">
        <div className="min-w-0 flex-1">
          <div className="text-xs text-surface-100 truncate">
            {filename ?? job?.filename ?? "PDF import"}
            {job?.book_name && (
              <span className="text-surface-200"> · {job.book_name}</span>
            )}
          </div>
          <div className="text-sm text-surface-50 mt-0.5 truncate" title={headline}>
            {headline}
          </div>
        </div>
        <div className="shrink-0">
          {job ? (
            <PhaseBadge phase={job.phase} status={job.status} />
          ) : (
            <span className="badge bg-accent/20 text-accent animate-pulse">
              uploading
            </span>
          )}
        </div>
      </div>

      {/* Phase timeline */}
      <PhaseTimeline
        active={failed ? failedDuring : active}
        failed={failed}
      />

      {/* Main progress bar */}
      <div className="space-y-1">
        <div className="flex items-baseline justify-between text-xs text-surface-100">
          <span className="tabular-nums">
            {percent != null ? `${percent}%` : "working…"}
            {rateLabel && <span className="text-surface-200"> · {rateLabel}</span>}
          </span>
          <span className="tabular-nums">
            {etaSeconds != null && `ETA ${formatDuration(etaSeconds * 1000)}`}
          </span>
        </div>
        <ProgressBar percent={percent} phase={barPhase} />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3 pt-1">
        {active === "upload" && upload && (
          <>
            <Stat
              label="Uploaded"
              value={`${formatBytes(upload.loaded)}${
                upload.total ? ` / ${formatBytes(upload.total)}` : ""
              }`}
            />
            <Stat label="Speed" value={formatRate(upload.bytesPerSec)} />
            <Stat
              label="Elapsed"
              value={uploadElapsed != null ? formatDuration(uploadElapsed) : null}
            />
            <Stat
              label="ETA"
              value={etaSeconds != null ? formatDuration(etaSeconds * 1000) : null}
            />
          </>
        )}
        {job && (
          <>
            <Stat
              label="Page"
              value={
                job.total_pages
                  ? `${job.current_page ?? 0} / ${job.total_pages}`
                  : job.current_page != null
                    ? String(job.current_page)
                    : null
              }
              tone={active === "parsing" ? "accent" : undefined}
            />
            <Stat
              label="OCR pages"
              value={job.ocr_pages_count || null}
              tone={job.ocr_pages_count > 0 ? "warn" : undefined}
            />
            <Stat
              label="TOC entries"
              value={job.toc_entries_count ?? null}
            />
            <Stat
              label="Sections"
              value={
                job.records_total
                  ? `${job.records_processed ?? 0} / ${job.records_total}`
                  : job.records_processed
                    ? String(job.records_processed)
                    : null
              }
              tone={active === "indexing" ? "accent" : undefined}
            />
            <Stat
              label="Current §"
              value={job.current_section_number ?? null}
            />
            <Stat
              label="Imported"
              value={job.records_imported || null}
              tone="success"
            />
            <Stat
              label="Failed"
              value={job.records_failed || null}
              tone={job.records_failed > 0 ? "warn" : undefined}
            />
            <Stat
              label="References"
              value={job.references_found || null}
            />
            <Stat
              label="PDF size"
              value={job.pdf_size_bytes ? formatBytes(job.pdf_size_bytes) : null}
            />
            <Stat
              label="Parse time"
              value={parseElapsed != null ? formatDuration(parseElapsed) : null}
            />
            <Stat
              label="Index time"
              value={indexElapsed != null ? formatDuration(indexElapsed) : null}
            />
            <Stat
              label="Total"
              value={totalElapsed != null ? formatDuration(totalElapsed) : null}
            />
          </>
        )}
      </div>

      {job?.error_message && active === "failed" && (
        <div className="text-xs text-danger border-t border-danger/20 pt-2">
          ✗ {job.error_message}
        </div>
      )}
    </div>
  );
}

// --- reindex button + table -----------------------------------------------

/**
 * Two-click delete control: first click reveals confirm + cancel inline,
 * second click fires the DELETE. Shows what will be removed (section
 * count, PDF yes/no) so the user isn't surprised.
 */
function DeleteImportButton({
  job,
  onDeleted,
}: {
  job: ImportJob;
  onDeleted?: () => void;
}) {
  const qc = useQueryClient();
  const [armed, setArmed] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const sectionCount =
    job.records_imported || job.records_processed || 0;
  const hasPdf = job.pdf_id != null;

  const onDelete = async () => {
    if (busy) return;
    setBusy(true);
    setErr(null);
    try {
      await deleteImport(job.id);
      qc.invalidateQueries({ queryKey: ["imports"] });
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
      onDeleted?.();
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : String(e));
      setArmed(false);
    } finally {
      setBusy(false);
    }
  };

  if (err) {
    return (
      <span className="text-danger" title={err}>
        delete failed
        <button
          type="button"
          className="ml-2 underline hover:text-surface-50"
          onClick={() => setErr(null)}
        >
          dismiss
        </button>
      </span>
    );
  }

  if (!armed) {
    return (
      <button
        type="button"
        onClick={() => setArmed(true)}
        className="underline text-danger hover:text-danger/80"
        title="Delete this import (and its sections/PDF)"
      >
        delete
      </button>
    );
  }

  const summary = (() => {
    const parts: string[] = [];
    if (sectionCount) parts.push(`${sectionCount} section${sectionCount === 1 ? "" : "s"}`);
    if (hasPdf) parts.push("stored PDF");
    if (parts.length === 0) return "this import log";
    return parts.join(" + ");
  })();

  return (
    <span className="inline-flex items-center gap-2">
      <span className="text-danger">Delete {summary}?</span>
      <button
        type="button"
        onClick={onDelete}
        disabled={busy}
        className="underline text-danger hover:text-danger/80 disabled:opacity-50"
      >
        {busy ? "deleting…" : "yes"}
      </button>
      <button
        type="button"
        onClick={() => setArmed(false)}
        disabled={busy}
        className="underline hover:text-surface-50 disabled:opacity-50"
      >
        no
      </button>
    </span>
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
        className="underline hover:text-surface-50 disabled:opacity-50"
        title="Re-parse the stored PDF for this book"
      >
        {busy ? "queueing…" : "re-index"}
      </button>
      {err && <span className="text-danger ml-2">{err}</span>}
    </>
  );
}

/**
 * One-click retry for a failed import. Calls /api/imports/{id}/retry —
 * which resolves the stored pdf_id, supersedes the book's current
 * sections, and spawns a fresh worker. Shown on rows whose status is
 * "error" (empty_extraction, all_quarantined, crashed, etc.) so the
 * operator doesn't have to re-upload the PDF after fixing the extractor.
 */
function RetryImportButton({ importLogId }: { importLogId: number }) {
  const qc = useQueryClient();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const onClick = async () => {
    if (busy) return;
    setBusy(true);
    setErr(null);
    try {
      await retryImport(importLogId);
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
        className="underline text-accent hover:text-surface-50 disabled:opacity-50"
        title="Re-run the parser against the stored PDF"
      >
        {busy ? "retrying…" : "retry"}
      </button>
      {err && <span className="text-danger ml-2" title={err}>failed</span>}
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
        const errored = isErrorJob(r);
        const terminal =
          r.phase === "completed" || r.phase === "failed" ||
          r.status === "completed" || r.status === "error";
        // Primary percent label: prefer section-based, fall back to
        // page-based for the parsing phase.
        const pctLabel: string =
          r.percent != null
            ? `${r.percent}%`
            : r.records_processed
              ? `${r.records_processed}${r.records_total ? ` / ${r.records_total}` : ""}`
              : "";
        return (
          <div key={r.id} className="py-2.5 space-y-1">
            <div className="flex items-baseline gap-2 flex-wrap">
              <PhaseBadge phase={r.phase} status={r.status} />
              <span className="text-sm text-surface-50 truncate flex-1 min-w-0">
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
            {!terminal && (
              <>
                <ProgressBar percent={r.percent} phase={r.phase} />
                {r.stage_detail && (
                  <div className="text-xs text-surface-100 truncate" title={r.stage_detail}>
                    {r.stage_detail}
                  </div>
                )}
              </>
            )}
            <div className="flex items-center gap-3 text-xs text-surface-100 flex-wrap">
              {pctLabel && <span>{pctLabel}</span>}
              {r.phase === "parsing" && r.total_pages && (
                <span>
                  pg {r.current_page ?? 0}/{r.total_pages}
                </span>
              )}
              {r.ocr_pages_count > 0 && (
                <span className="text-warn">OCR {r.ocr_pages_count}</span>
              )}
              {r.phase === "indexing" && r.current_section_number && (
                <span className="truncate max-w-[12rem]">
                  §{r.current_section_number}
                </span>
              )}
              {r.records_imported > 0 && (
                <span className="text-success">✓ {r.records_imported}</span>
              )}
              {r.records_failed > 0 && (
                <span className="text-warn">⚠ {r.records_failed}</span>
              )}
              {r.references_found > 0 && (
                <span>🔗 {r.references_found}</span>
              )}
              <span className="ml-auto flex items-center gap-3">
                {r.pdf_id && errored && (
                  <RetryImportButton importLogId={r.id} />
                )}
                {r.pdf_id && (r.phase === "completed" || r.status === "completed") && (
                  <ReindexButton pdfId={r.pdf_id} />
                )}
                {r.pdf_id && (
                  <a
                    className="underline hover:text-surface-50"
                    href={`/api/code-book-pdfs/${r.pdf_id}/content`}
                    target="_blank"
                    rel="noreferrer"
                    title="Download stored PDF"
                  >
                    download
                  </a>
                )}
                <DeleteImportButton job={r} />
              </span>
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
