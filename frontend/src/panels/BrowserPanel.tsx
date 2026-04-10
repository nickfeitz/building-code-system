import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { apiGet } from "../api/client";
import type { CodeBook, SectionSearchHit } from "../api/types";

export function BrowserPanel() {
  const [q, setQ] = useState("");
  const [submitted, setSubmitted] = useState("");
  const [bookId, setBookId] = useState<number | "">("");

  const books = useQuery({
    queryKey: ["code-books"],
    queryFn: () => apiGet<CodeBook[]>("/code-books"),
    staleTime: 60_000,
  });

  const results = useQuery({
    queryKey: ["search", submitted, bookId],
    queryFn: () => {
      const params = new URLSearchParams({ q: submitted });
      if (bookId) params.set("code_book_id", String(bookId));
      return apiGet<SectionSearchHit[]>(`/sections/search?${params.toString()}`);
    },
    enabled: submitted.length > 0,
  });

  return (
    <div className="h-full flex flex-col">
      <div className="px-6 py-4 border-b border-surface-400 bg-surface-800 space-y-2">
        <div className="flex gap-2">
          <input
            className="input flex-1"
            placeholder="Search sections by keyword…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") setSubmitted(q.trim());
            }}
          />
          <select
            className="input max-w-xs"
            value={bookId}
            onChange={(e) => setBookId(e.target.value ? Number(e.target.value) : "")}
          >
            <option value="">All code books</option>
            {(books.data ?? []).map((b) => (
              <option key={b.id} value={b.id}>
                {b.title || b.name || b.abbreviation || `#${b.id}`}
              </option>
            ))}
          </select>
          <button className="btn-primary" onClick={() => setSubmitted(q.trim())}>
            Search
          </button>
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-6">
        {!submitted && (
          <div className="text-center text-sm text-surface-100 mt-10">
            Enter a query above to search the code corpus.
          </div>
        )}
        {submitted && results.isLoading && <div className="text-sm text-surface-100">Searching…</div>}
        {results.data && results.data.length === 0 && (
          <div className="text-sm text-surface-100">No results.</div>
        )}
        <div className="space-y-3">
          {(results.data ?? []).map((hit) => (
            <div key={hit.id} className="card">
              <div className="flex items-baseline justify-between gap-4">
                <div className="font-mono text-xs text-accent">{hit.section_number}</div>
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
