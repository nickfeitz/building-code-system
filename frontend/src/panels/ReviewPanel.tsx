import { useCallback, useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { pdfPageImageUrl, reindexPdf, ApiError } from "../api/client";
import type { FlagReason, ReviewContext } from "../api/types";
import {
  useFlagPage,
  usePageSections,
  usePageText,
  usePdfMeta,
} from "../hooks/useReview";

const DPIS = [75, 100, 150, 200, 300];

const REASON_LABELS: Record<FlagReason, string> = {
  text_missing: "Text missing",
  text_wrong: "Text wrong",
  layout_broken: "Layout broken",
  ocr_needed: "Needs OCR (scan)",
  other: "Other",
};

function FlagModal({
  open,
  page,
  onClose,
  onSubmit,
  pending,
}: {
  open: boolean;
  page: number;
  onClose: () => void;
  onSubmit: (reason: FlagReason, note: string) => void;
  pending: boolean;
}) {
  const [reason, setReason] = useState<FlagReason>("text_missing");
  const [note, setNote] = useState("");

  useEffect(() => {
    if (!open) {
      setReason("text_missing");
      setNote("");
    }
  }, [open]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-40 bg-black/60 flex items-center justify-center"
      onClick={onClose}
    >
      <div
        className="card w-full max-w-md !p-5 space-y-3"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="text-sm font-semibold text-white">
          Flag page {page} as bad extraction
        </h2>
        <p className="text-xs text-surface-100">
          Routes this page into the Quarantine panel for later triage. The
          page's raw PyMuPDF text is captured alongside.
        </p>
        <div>
          <div className="text-xs text-surface-100 mb-1">Reason</div>
          <select
            className="input"
            value={reason}
            onChange={(e) => setReason(e.target.value as FlagReason)}
          >
            {Object.entries(REASON_LABELS).map(([v, l]) => (
              <option key={v} value={v}>{l}</option>
            ))}
          </select>
        </div>
        <div>
          <div className="text-xs text-surface-100 mb-1">Note (optional)</div>
          <textarea
            className="input resize-none"
            rows={3}
            value={note}
            maxLength={500}
            placeholder="e.g. title cut off by binding; equations not captured"
            onChange={(e) => setNote(e.target.value)}
          />
        </div>
        <div className="flex justify-end gap-2">
          <button type="button" onClick={onClose} className="btn-ghost" disabled={pending}>
            Cancel
          </button>
          <button
            type="button"
            className="btn-primary"
            onClick={() => onSubmit(reason, note.trim())}
            disabled={pending}
          >
            {pending ? "Submitting…" : "Flag page"}
          </button>
        </div>
      </div>
    </div>
  );
}

export function ReviewPanel({
  context,
  onClose,
}: {
  context: ReviewContext;
  onClose: () => void;
}) {
  const { pdfId, codeBookId, codeName, filename } = context;

  const meta = usePdfMeta(pdfId);
  const pageCount = meta.data?.page_count ?? 0;

  const [page, setPage] = useState(1);
  const [dpi, setDpi] = useState(150);
  const [imgLoading, setImgLoading] = useState(true);
  const [flagOpen, setFlagOpen] = useState(false);
  const [flagToast, setFlagToast] = useState<string | null>(null);
  const [reindexing, setReindexing] = useState(false);
  const qc = useQueryClient();

  const pageText = usePageText(pdfId, page);
  const pageSections = usePageSections(codeBookId, page);
  const flag = useFlagPage();

  const goto = useCallback((n: number) => {
    if (!pageCount) return;
    const clamped = Math.max(1, Math.min(pageCount, n));
    setPage(clamped);
    setImgLoading(true);
  }, [pageCount]);

  // Keyboard nav
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (flagOpen) return;
      const t = e.target as HTMLElement | null;
      if (t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable)) return;
      if (e.key === "ArrowLeft") { e.preventDefault(); goto(page - 1); }
      else if (e.key === "ArrowRight") { e.preventDefault(); goto(page + 1); }
      else if (e.key === "f" || e.key === "F") { e.preventDefault(); setFlagOpen(true); }
      else if (e.key === "Escape") { e.preventDefault(); onClose(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [page, goto, flagOpen, onClose]);

  const onReindex = async () => {
    if (reindexing) return;
    const ok = window.confirm(
      `Re-parse "${codeName}"? This will mark any current indexed sections for this book as superseded and replace them with a fresh parse of the stored PDF.`,
    );
    if (!ok) return;
    setReindexing(true);
    try {
      const r = await reindexPdf(pdfId);
      setFlagToast(`Re-index queued · import_log_id ${r.import_log_id}`);
      setTimeout(() => setFlagToast(null), 4000);
      qc.invalidateQueries({ queryKey: ["imports"] });
      qc.invalidateQueries({ queryKey: ["book-page-sections"] });
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    } catch (e) {
      setFlagToast(
        e instanceof ApiError
          ? `Re-index failed (${e.status}): ${e.message}`
          : `Re-index failed: ${String(e)}`,
      );
      setTimeout(() => setFlagToast(null), 6000);
    } finally {
      setReindexing(false);
    }
  };

  const onSubmitFlag = async (reason: FlagReason, note: string) => {
    try {
      const r = await flag.mutateAsync({
        pdf_id: pdfId,
        code_book_id: codeBookId,
        page,
        reason,
        note: note || undefined,
      });
      setFlagOpen(false);
      setFlagToast(`Flagged page ${page} · quarantine #${r.quarantine_id}`);
      setTimeout(() => setFlagToast(null), 4000);
    } catch (e) {
      setFlagToast(`Flag failed: ${e instanceof Error ? e.message : String(e)}`);
      setTimeout(() => setFlagToast(null), 6000);
    }
  };

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="px-4 py-2 border-b border-surface-400 bg-surface-800 flex flex-wrap items-center gap-3 text-sm">
        <button type="button" onClick={onClose} className="btn-ghost !py-1 !px-2 !text-xs">
          ← Back to Catalog
        </button>
        <div className="min-w-0 flex-1">
          <div className="text-white truncate">{codeName}</div>
          <div className="text-xs text-surface-100 truncate">
            {filename ?? `PDF #${pdfId}`} · pdf_id={pdfId}
          </div>
        </div>

        <div className="flex items-center gap-1">
          <button
            type="button"
            className="btn-ghost !py-1 !px-2 !text-xs"
            onClick={() => goto(page - 1)}
            disabled={page <= 1}
          >
            ◀
          </button>
          <input
            type="number"
            min={1}
            max={pageCount || 1}
            value={page}
            onChange={(e) => {
              const v = parseInt(e.target.value, 10);
              if (!Number.isNaN(v)) goto(v);
            }}
            className="input !py-1 w-16 text-center text-xs"
          />
          <span className="text-xs text-surface-100">/ {pageCount || "?"}</span>
          <button
            type="button"
            className="btn-ghost !py-1 !px-2 !text-xs"
            onClick={() => goto(page + 1)}
            disabled={pageCount > 0 && page >= pageCount}
          >
            ▶
          </button>
        </div>

        <div className="flex items-center gap-1 text-xs text-surface-100">
          <span>DPI</span>
          <select
            className="input !py-1 !text-xs"
            value={dpi}
            onChange={(e) => {
              setDpi(Number(e.target.value));
              setImgLoading(true);
            }}
          >
            {DPIS.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
        </div>

        <button
          type="button"
          onClick={onReindex}
          disabled={reindexing}
          className="btn-ghost !py-1 !px-2 !text-xs"
          title="Re-parse the stored PDF for this book (replaces current sections)"
        >
          {reindexing ? "Queueing…" : "↻ Re-index"}
        </button>
        <button
          type="button"
          onClick={() => setFlagOpen(true)}
          className="btn-ghost !py-1 !px-2 !text-xs"
          title="Flag this page (f)"
        >
          🚩 Flag page
        </button>
      </div>

      {/* Split body */}
      <div className="flex-1 min-h-0 grid grid-cols-1 md:grid-cols-2">
        {/* Left: rendered page */}
        <div className="relative overflow-auto bg-surface-900 border-r border-surface-400 flex justify-center items-start p-4">
          {meta.isLoading && (
            <div className="text-sm text-surface-100 mt-10">Loading PDF metadata…</div>
          )}
          {meta.error && (
            <div className="text-sm text-danger mt-10">
              Couldn't load PDF metadata: {(meta.error as Error).message}
            </div>
          )}
          {pageCount > 0 && (
            <>
              {imgLoading && (
                <div className="absolute inset-0 flex items-center justify-center text-sm text-surface-100 pointer-events-none">
                  Rendering page {page} at {dpi} dpi…
                </div>
              )}
              <img
                key={`${pdfId}:${page}:${dpi}`}
                src={pdfPageImageUrl(pdfId, page, dpi)}
                alt={`Page ${page}`}
                onLoad={() => setImgLoading(false)}
                onError={() => setImgLoading(false)}
                className="max-w-full h-auto bg-white shadow-xl"
                style={{ imageRendering: "auto" }}
              />
            </>
          )}
        </div>

        {/* Right: text + sections */}
        <div className="overflow-auto p-4 space-y-4">
          <div className="card">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-semibold text-white">
                Extracted text (PyMuPDF)
              </h3>
              <span className="text-xs text-surface-100">
                page {page}
                {pageText.data && ` · ${pageText.data.chars} chars`}
              </span>
            </div>
            {pageText.isLoading && <div className="text-xs text-surface-100">loading…</div>}
            {pageText.error && (
              <div className="text-xs text-danger">
                {(pageText.error as Error).message}
              </div>
            )}
            {pageText.data && pageText.data.chars === 0 && (
              <div className="text-xs text-warn">
                No text extracted from this page — it may be an image-only scan
                (OCR not enabled) or a pure-table layout the parser couldn't read.
              </div>
            )}
            <pre className="text-xs text-surface-50 whitespace-pre-wrap break-words max-h-[48vh] overflow-auto font-mono">
              {pageText.data?.text ?? ""}
            </pre>
          </div>

          <div className="card">
            <div className="flex items-center justify-between mb-2">
              <h3 className="text-sm font-semibold text-white">
                Sections on this page
              </h3>
              <span className="text-xs text-surface-100">
                {pageSections.data ? `${pageSections.data.length} found` : ""}
              </span>
            </div>
            {pageSections.isLoading && <div className="text-xs text-surface-100">loading…</div>}
            {pageSections.data && pageSections.data.length === 0 && (
              <div className="text-xs text-surface-100 italic">
                No sections mapped to this page. If this book was uploaded
                before the image-review feature landed, re-upload it to
                enable per-page section tracking.
              </div>
            )}
            <div className="space-y-2">
              {(pageSections.data ?? []).map((s) => (
                <div key={s.id} className="border-l-2 border-accent pl-3 py-1">
                  <div className="font-mono text-xs text-accent">
                    {s.section_number}
                    {s.has_ca_amendment && s.amendment_agency && (
                      <span className="ml-2 badge bg-warn/20 text-warn">
                        [{s.amendment_agency}]
                      </span>
                    )}
                  </div>
                  <div className="text-sm text-white">{s.section_title}</div>
                  <div className="text-xs text-surface-100 mt-0.5 line-clamp-3">
                    {s.full_text.slice(0, 280)}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="text-[11px] text-surface-100 text-center">
            keys: ← → page · f flag · esc close
          </div>
        </div>
      </div>

      {flagToast && (
        <div className="fixed bottom-6 right-6 z-50 card !p-3 text-xs">
          {flagToast}
        </div>
      )}

      <FlagModal
        open={flagOpen}
        page={page}
        onClose={() => setFlagOpen(false)}
        onSubmit={onSubmitFlag}
        pending={flag.isPending}
      />
    </div>
  );
}
