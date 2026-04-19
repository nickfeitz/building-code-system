import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";
import type { CatalogBook, SectionSearchHit } from "../api/types";
import { useCatalog } from "../hooks/useCatalog";

/** One flat-listed book with enough context to render a full label. */
interface IndexedBook {
  book: CatalogBook;
  authority: string;
  cycleName: string;
}

/**
 * Labels that mirror the Catalog + Import panels so a user sees the same
 * name for a code everywhere. Falls back to abbreviation only when
 * code_name is missing (shouldn't happen post-seed).
 */
function bookLabel(b: CatalogBook): string {
  return b.code_name || b.abbreviation || `#${b.id}`;
}

export function BrowserPanel() {
  const catalog = useCatalog();
  const [q, setQ] = useState("");
  const [submitted, setSubmitted] = useState("");
  const [bookId, setBookId] = useState<number | "">("");

  // Only show codes that actually have indexed sections. The Code Browser
  // is useless for empty books — searches return nothing, so the drop-down
  // shouldn't pretend 178 codes are available when only a handful are.
  const indexed = useMemo<IndexedBook[]>(() => {
    if (!catalog.data) return [];
    const out: IndexedBook[] = [];
    for (const a of catalog.data.authorities) {
      for (const c of a.cycles) {
        for (const b of c.books) {
          if (b.indexed_section_count > 0) {
            out.push({ book: b, authority: a.adopting_authority, cycleName: c.name });
          }
        }
      }
    }
    // Sort: authority, then newest cycle first by effective_date (via
    // the order already coming out of /api/catalog), then part number.
    return out;
  }, [catalog.data]);

  // Keep the current selection legal if the book gets unindexed between
  // renders (e.g. superseded by a new upload that's still processing).
  const hasSelection = bookId !== "" && indexed.some((i) => i.book.id === bookId);

  const groupedForSelect = useMemo(() => {
    const m = new Map<string, IndexedBook[]>();
    for (const item of indexed) {
      if (!m.has(item.authority)) m.set(item.authority, []);
      m.get(item.authority)!.push(item);
    }
    return [...m.entries()];
  }, [indexed]);

  const results = useQuery({
    queryKey: ["search", submitted, bookId],
    queryFn: () => {
      const params = new URLSearchParams({ q: submitted });
      if (bookId) params.set("code_book_id", String(bookId));
      return apiGet<SectionSearchHit[]>(`/sections/search?${params.toString()}`);
    },
    enabled: submitted.length > 0,
  });

  const canSearch = indexed.length > 0;

  return (
    <div className="h-full flex flex-col">
      <div className="px-6 py-4 border-b border-surface-400 bg-surface-800 space-y-2">
        <div className="flex gap-2">
          <input
            className="input flex-1"
            placeholder={canSearch ? "Search sections by keyword…" : "No codes indexed yet"}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && canSearch) setSubmitted(q.trim());
            }}
            disabled={!canSearch}
          />
          <select
            className="input max-w-md"
            value={hasSelection ? (bookId as number) : ""}
            onChange={(e) => setBookId(e.target.value ? Number(e.target.value) : "")}
            disabled={!canSearch}
          >
            <option value="">
              {canSearch
                ? `All indexed codes (${indexed.length})`
                : "— no codes indexed —"}
            </option>
            {groupedForSelect.map(([authority, items]) => (
              <optgroup key={authority} label={authority}>
                {items.map(({ book, cycleName }) => (
                  <option key={book.id} value={book.id}>
                    {bookLabel(book)} · {cycleName}
                    {book.indexed_section_count
                      ? ` · ${book.indexed_section_count} sections`
                      : ""}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
          <button
            className="btn-primary"
            onClick={() => canSearch && setSubmitted(q.trim())}
            disabled={!canSearch}
          >
            Search
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {/* Empty state — nothing indexed anywhere */}
        {catalog.isLoading && (
          <div className="text-sm text-surface-100">Loading catalog…</div>
        )}
        {!catalog.isLoading && !canSearch && (
          <div className="card max-w-lg mx-auto mt-8 text-sm">
            <h3 className="text-white font-semibold mb-2">Nothing to browse yet</h3>
            <p className="text-surface-100">
              The Code Browser searches across indexed code sections, but no
              codes have been scanned yet. Upload a PDF or trigger a scan for
              a book in the{" "}
              <span className="text-accent">Catalog</span> panel; once its
              sections are indexed, the book will appear in the dropdown here.
            </p>
          </div>
        )}

        {/* Default hint once at least one book is indexed */}
        {canSearch && !submitted && (
          <div className="text-center text-sm text-surface-100 mt-10">
            Enter a query above to search {indexed.length} indexed code{" "}
            {indexed.length === 1 ? "book" : "books"}.
          </div>
        )}

        {/* Search states */}
        {submitted && results.isLoading && (
          <div className="text-sm text-surface-100">Searching…</div>
        )}
        {submitted && results.data && results.data.length === 0 && (
          <div className="text-sm text-surface-100">No results.</div>
        )}

        <div className="space-y-3">
          {(results.data ?? []).map((hit) => (
            <div key={hit.id} className="card">
              <div className="flex items-baseline justify-between gap-4">
                <div className="font-mono text-xs text-accent">
                  {hit.section_number}
                </div>
                {hit.similarity != null && (
                  <div className="text-xs text-surface-100">
                    sim {hit.similarity.toFixed(3)}
                  </div>
                )}
              </div>
              <div className="text-sm text-white mt-1">{hit.section_title}</div>
              <div className="text-xs text-surface-100 mt-2 line-clamp-3">
                {(hit.full_text || hit.text || "").slice(0, 280)}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
