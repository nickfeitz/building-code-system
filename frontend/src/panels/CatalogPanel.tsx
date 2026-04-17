import { useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useCatalog, useCatalogScan } from "../hooks/useCatalog";
import { uploadPdf, ApiError } from "../api/client";
import type {
  CatalogAuthority,
  CatalogBook,
  CatalogCycle,
  CatalogScanResponse,
} from "../api/types";

type StatusFilter = "all" | "active" | "superseded";
type ScanFilter = "all" | "scanned" | "unscanned";

function Badge({ children, tone = "neutral" }: { children: React.ReactNode; tone?: "neutral" | "accent" | "success" | "warn" | "danger" }) {
  const toneClass = {
    neutral: "bg-surface-500 text-surface-50",
    accent: "bg-accent/30 text-accent",
    success: "bg-success/20 text-success",
    warn: "bg-warn/20 text-warn",
    danger: "bg-danger/20 text-danger",
  }[tone];
  return <span className={`badge ${toneClass}`}>{children}</span>;
}

function ScanStatusDot({ book }: { book: CatalogBook }) {
  const map: Record<string, { color: string; label: string }> = {
    indexed: { color: "bg-success", label: `indexed · ${book.indexed_section_count}` },
    crawling: { color: "bg-warn animate-pulse", label: "crawling…" },
    error: { color: "bg-danger", label: "error" },
    not_scanned: { color: "bg-surface-200", label: "not scanned" },
  };
  const entry = map[book.scan_status] ?? { color: "bg-surface-200", label: book.scan_status };
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-surface-100">
      <span className={`w-2 h-2 rounded-full ${entry.color}`} />
      {entry.label}
    </span>
  );
}

function BookRow({
  book,
  checked,
  onToggle,
}: {
  book: CatalogBook;
  checked: boolean;
  onToggle: () => void;
}) {
  const scannable = !!book.digital_access_url;
  const qc = useQueryClient();
  const fileInput = useRef<HTMLInputElement>(null);
  const [uploading, setUploading] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const onPickFile = () => fileInput.current?.click();

  const onFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    e.target.value = ""; // reset so the same file can be re-picked
    if (!f) return;
    setUploading(true);
    setMsg(null);
    try {
      const r = await uploadPdf(book.id, f);
      setMsg(`Uploaded ${r.filename} · processing…`);
      qc.invalidateQueries({ queryKey: ["import-status"] });
      qc.invalidateQueries({ queryKey: ["catalog"] });
      qc.invalidateQueries({ queryKey: ["stats"] });
    } catch (err) {
      setMsg(
        err instanceof ApiError
          ? `Upload failed (${err.status}): ${err.message}`
          : String(err)
      );
    } finally {
      setUploading(false);
    }
  };

  return (
    <div
      className={`flex items-center gap-3 py-2 px-4 rounded border-l-2 ${
        checked ? "bg-accent/10 border-accent" : "border-transparent hover:bg-surface-700"
      }`}
    >
      <input
        type="checkbox"
        checked={checked}
        onChange={onToggle}
        disabled={!scannable}
        title={scannable ? "Select for scanning" : "No URL available — use Upload PDF →"}
        className="w-4 h-4 accent-blue-500 disabled:opacity-40 disabled:cursor-not-allowed"
      />
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 text-sm text-white">
          {book.part_number && (
            <span className="font-mono text-xs text-surface-100 w-16 shrink-0">
              {book.part_number}
            </span>
          )}
          <span className="truncate">{book.code_name}</span>
        </div>
        <div className="flex flex-wrap items-center gap-2 mt-1">
          {book.category && <Badge>{book.category}</Badge>}
          {book.base_model_abbreviation && (
            <Badge tone="accent">
              based on {book.base_model_abbreviation}
              {book.base_code_year ? ` ${book.base_code_year}` : ""}
            </Badge>
          )}
          <ScanStatusDot book={book} />
          {!scannable && (
            <span className="text-xs text-surface-100 italic">no URL</span>
          )}
          {msg && (
            <span
              className={`text-xs ${
                msg.startsWith("Upload failed") ? "text-danger" : "text-success"
              }`}
            >
              {msg}
            </span>
          )}
        </div>
      </div>
      <input
        ref={fileInput}
        type="file"
        accept=".pdf"
        className="hidden"
        onChange={onFile}
      />
      <button
        type="button"
        onClick={onPickFile}
        disabled={uploading}
        title={`Upload a PDF into ${book.code_name}`}
        className="btn-ghost !py-1 !px-2 !text-xs shrink-0"
      >
        {uploading ? "Uploading…" : "Upload PDF"}
      </button>
    </div>
  );
}

function CycleGroup({
  cycle,
  selected,
  toggle,
  toggleAll,
}: {
  cycle: CatalogCycle;
  selected: Set<number>;
  toggle: (id: number) => void;
  toggleAll: (bookIds: number[], makeChecked: boolean) => void;
}) {
  const scannableIds = cycle.books.filter((b) => !!b.digital_access_url).map((b) => b.id);
  const allChecked = scannableIds.length > 0 && scannableIds.every((id) => selected.has(id));
  const someChecked = scannableIds.some((id) => selected.has(id));

  const statusTone = cycle.status === "active" ? "success" : cycle.status === "upcoming" ? "warn" : "neutral";

  return (
    <details className="group" open={cycle.status === "active"}>
      <summary className="cursor-pointer select-none flex items-center gap-2 px-4 py-2 hover:bg-surface-700 rounded text-sm">
        <span className="text-surface-100 group-open:rotate-90 transition-transform">▸</span>
        <span className="text-white font-medium">{cycle.name}</span>
        <Badge tone={statusTone as "success" | "warn" | "neutral"}>{cycle.status}</Badge>
        {cycle.effective_date && (
          <span className="text-xs text-surface-100">eff. {cycle.effective_date}</span>
        )}
        <span className="flex-1" />
        <span className="text-xs text-surface-100">{cycle.books.length} books</span>
        {scannableIds.length > 0 && (
          <label
            className="text-xs text-surface-100 hover:text-white flex items-center gap-1 ml-3"
            onClick={(e) => e.stopPropagation()}
          >
            <input
              type="checkbox"
              checked={allChecked}
              ref={(el) => {
                if (el) el.indeterminate = !allChecked && someChecked;
              }}
              onChange={() => toggleAll(scannableIds, !allChecked)}
              className="w-3.5 h-3.5 accent-blue-500"
            />
            all scannable
          </label>
        )}
      </summary>
      <div className="pl-4 border-l border-surface-400 ml-3 mt-1 space-y-0.5">
        {cycle.books.map((b) => (
          <BookRow key={b.id} book={b} checked={selected.has(b.id)} onToggle={() => toggle(b.id)} />
        ))}
      </div>
    </details>
  );
}

function AuthorityGroup({
  authority,
  selected,
  toggle,
  toggleAll,
  statusFilter,
  scanFilter,
  search,
}: {
  authority: CatalogAuthority;
  selected: Set<number>;
  toggle: (id: number) => void;
  toggleAll: (bookIds: number[], makeChecked: boolean) => void;
  statusFilter: StatusFilter;
  scanFilter: ScanFilter;
  search: string;
}) {
  const filteredCycles = useMemo(() => {
    const q = search.trim().toLowerCase();
    return authority.cycles
      .map((cyc) => {
        const books = cyc.books.filter((b) => {
          if (statusFilter !== "all" && b.status !== statusFilter) return false;
          if (scanFilter === "scanned" && b.scan_status !== "indexed") return false;
          if (scanFilter === "unscanned" && b.scan_status === "indexed") return false;
          if (q) {
            const hay = `${b.code_name} ${b.abbreviation} ${b.category ?? ""} ${b.base_model_abbreviation ?? ""}`.toLowerCase();
            if (!hay.includes(q)) return false;
          }
          return true;
        });
        return { ...cyc, books };
      })
      .filter((cyc) => cyc.books.length > 0);
  }, [authority.cycles, statusFilter, scanFilter, search]);

  if (filteredCycles.length === 0) return null;

  const totalBooks = filteredCycles.reduce((n, c) => n + c.books.length, 0);

  return (
    <details className="card !p-3 group" open>
      <summary className="cursor-pointer select-none flex items-center gap-3 text-white">
        <span className="text-surface-100 group-open:rotate-90 transition-transform">▸</span>
        <span className="font-semibold">{authority.adopting_authority}</span>
        <Badge>{authority.publishing_org_abbr}</Badge>
        <span className="flex-1" />
        <span className="text-xs text-surface-100">
          {totalBooks} book{totalBooks === 1 ? "" : "s"}
        </span>
      </summary>
      <div className="mt-3 space-y-1">
        {filteredCycles.map((cyc) => (
          <CycleGroup
            key={cyc.id}
            cycle={cyc}
            selected={selected}
            toggle={toggle}
            toggleAll={toggleAll}
          />
        ))}
      </div>
    </details>
  );
}

export function CatalogPanel() {
  const { data, isLoading, error } = useCatalog();
  const scan = useCatalogScan();

  const [selected, setSelected] = useState<Set<number>>(new Set());
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");
  const [scanFilter, setScanFilter] = useState<ScanFilter>("all");
  const [search, setSearch] = useState("");
  const [result, setResult] = useState<CatalogScanResponse | null>(null);

  const toggle = (id: number) =>
    setSelected((s) => {
      const n = new Set(s);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });

  const toggleAll = (ids: number[], makeChecked: boolean) =>
    setSelected((s) => {
      const n = new Set(s);
      ids.forEach((id) => (makeChecked ? n.add(id) : n.delete(id)));
      return n;
    });

  const clear = () => setSelected(new Set());

  const onScan = async () => {
    if (selected.size === 0) return;
    try {
      const r = await scan.mutateAsync([...selected]);
      setResult(r);
      setSelected(new Set());
    } catch (e) {
      setResult({
        triggered: [],
        skipped_no_url: [],
        errors: [{ code_book_id: 0, error: e instanceof Error ? e.message : String(e) }],
      });
    }
  };

  // Summary counts for the top bar
  const summary = useMemo(() => {
    if (!data) return { total: 0, indexed: 0, scannable: 0 };
    let total = 0,
      indexed = 0,
      scannable = 0;
    for (const a of data.authorities) {
      for (const c of a.cycles) {
        for (const b of c.books) {
          total++;
          if (b.scan_status === "indexed") indexed++;
          if (b.digital_access_url) scannable++;
        }
      }
    }
    return { total, indexed, scannable };
  }, [data]);

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="px-6 py-3 border-b border-surface-400 bg-surface-800 flex flex-wrap items-center gap-3">
        <input
          className="input max-w-xs"
          placeholder="Search name / abbreviation / base model…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <div className="flex items-center gap-1 text-xs">
          <span className="text-surface-100">Status:</span>
          {(["all", "active", "superseded"] as StatusFilter[]).map((k) => (
            <button
              key={k}
              className={`px-2 py-1 rounded ${
                statusFilter === k ? "bg-accent text-white" : "text-surface-50 hover:bg-surface-500"
              }`}
              onClick={() => setStatusFilter(k)}
            >
              {k}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1 text-xs">
          <span className="text-surface-100">Scan:</span>
          {(["all", "scanned", "unscanned"] as ScanFilter[]).map((k) => (
            <button
              key={k}
              className={`px-2 py-1 rounded ${
                scanFilter === k ? "bg-accent text-white" : "text-surface-50 hover:bg-surface-500"
              }`}
              onClick={() => setScanFilter(k)}
            >
              {k}
            </button>
          ))}
        </div>
        <div className="flex-1" />
        <div className="text-xs text-surface-100">
          {summary.total} books · {summary.indexed} indexed · {summary.scannable} scannable
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {isLoading && <div className="text-sm text-surface-100">Loading catalog…</div>}
        {error && (
          <div className="text-sm text-danger">
            Failed to load catalog: {(error as Error).message}
          </div>
        )}
        {result && (
          <div className="card">
            <div className="text-sm text-white">Scan request submitted</div>
            <ul className="mt-2 text-xs text-surface-50 space-y-1">
              <li>
                <span className="text-success">Triggered: {result.triggered.length}</span>
                {result.triggered.length > 0 && (
                  <span className="text-surface-100">
                    {" "}
                    ({result.triggered.map((t) => t.code_name).slice(0, 3).join(", ")}
                    {result.triggered.length > 3 ? "…" : ""})
                  </span>
                )}
              </li>
              {result.skipped_no_url.length > 0 && (
                <li className="text-warn">
                  Skipped (no URL): {result.skipped_no_url.length}
                </li>
              )}
              {result.errors.length > 0 && (
                <li className="text-danger">
                  Errors: {result.errors.map((e) => e.error).join("; ")}
                </li>
              )}
            </ul>
            <button
              className="btn-ghost mt-3"
              onClick={() => setResult(null)}
            >
              Dismiss
            </button>
          </div>
        )}
        {data?.authorities.map((a) => (
          <AuthorityGroup
            key={a.adopting_authority}
            authority={a}
            selected={selected}
            toggle={toggle}
            toggleAll={toggleAll}
            statusFilter={statusFilter}
            scanFilter={scanFilter}
            search={search}
          />
        ))}
      </div>

      {/* Footer */}
      <div className="px-6 py-3 border-t border-surface-400 bg-surface-800 flex items-center gap-3">
        <div className="text-sm text-surface-50">
          <span className="text-white font-semibold tabular-nums">{selected.size}</span> selected
        </div>
        <div className="flex-1" />
        <button className="btn-ghost" onClick={clear} disabled={selected.size === 0}>
          Clear
        </button>
        <button
          className="btn-primary"
          onClick={onScan}
          disabled={selected.size === 0 || scan.isPending}
        >
          {scan.isPending ? "Submitting…" : `Scan ${selected.size || ""} Selected`}
        </button>
      </div>
    </div>
  );
}
