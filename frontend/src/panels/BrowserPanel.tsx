import { useMemo, useState } from "react";
import { pdfPageImageUrl } from "../api/client";
import type { CatalogBook } from "../api/types";
import { useCatalog } from "../hooks/useCatalog";
import {
  useBookOutline,
  useBookPdfs,
  type OutlineRow,
} from "../hooks/useBrowser";

/**
 * Code Browser — single-window, UpCodes-style reader.
 *
 * Landing view: hierarchical outline of the whole code book.
 *
 * After clicking a node: a full reading view of that node AND all its
 * descendants, rendered as one flowing document (so tapping "Chapter 26"
 * gives you the entire chapter scrollable top-to-bottom, not just a
 * chapter stub you then have to click into piece by piece). Deeper
 * subsections use indentation and slightly smaller headings.
 *
 * The original PDF page preview is opt-in — a small "View PDF page N"
 * link at the bottom of each section opens the rendered page inline
 * below that section's text. Never as a side pane.
 */

interface IndexedBook {
  book: CatalogBook;
  authority: string;
  cycleName: string;
}

interface TreeNode {
  row: OutlineRow;
  children: TreeNode[];
}

function bookLabel(b: CatalogBook): string {
  return b.code_name || b.abbreviation || `#${b.id}`;
}

/**
 * Build a parent-child tree from the flat outline. Parents are derived by
 * walking section_number backwards ("26.1.2.1" → "26.1.2" → "26.1" →
 * "Chapter 26: …"). If no parent match is found, the node is a root.
 * Document order of children is preserved via the iteration order of the
 * outline list.
 */
function buildTree(rows: OutlineRow[]): TreeNode[] {
  const nodes = new Map<string, TreeNode>();
  const byNumber = new Map<string, OutlineRow>();

  // First-wins dedupe when the outline contains two rows with the same
  // section_number (body vs. commentary occasionally collide).
  for (const r of rows) {
    const key = r.section_number.trim();
    if (!key) continue;
    if (!byNumber.has(key)) byNumber.set(key, r);
  }
  for (const r of byNumber.values()) {
    nodes.set(r.section_number.trim(), { row: r, children: [] });
  }

  const roots: TreeNode[] = [];
  for (const node of nodes.values()) {
    const num = node.row.section_number.trim();
    const parentKey = deriveParentKey(num, nodes);
    if (parentKey && nodes.has(parentKey)) {
      nodes.get(parentKey)!.children.push(node);
    } else {
      roots.push(node);
    }
  }
  return roots;
}

function deriveParentKey(num: string, nodes: Map<string, TreeNode>): string | null {
  if (/^\d+(?:\.\d+)+$/.test(num)) {
    const parts = num.split(".");
    parts.pop();
    const candidate = parts.join(".");
    if (candidate && nodes.has(candidate)) return candidate;
    const n = parts[0];
    for (const key of nodes.keys()) {
      if (new RegExp(`^Chapter\\s+${n}\\b`, "i").test(key)) return key;
    }
    return null;
  }
  const appMatch = num.match(/^([A-Z]{1,3})\.(?:\d+|\d+\.\d+)/);
  if (appMatch) {
    const letter = appMatch[1];
    for (const key of nodes.keys()) {
      if (new RegExp(`^Appendix\\s+${letter}\\b`, "i").test(key)) return key;
    }
    if (nodes.has(letter)) return letter;
    return null;
  }
  return null;
}

function filterTree(roots: TreeNode[], needle: string): TreeNode[] {
  const low = needle.trim().toLowerCase();
  if (!low) return roots;
  const match = (t: TreeNode): boolean =>
    t.row.section_number.toLowerCase().includes(low) ||
    (t.row.section_title || "").toLowerCase().includes(low);
  const walk = (t: TreeNode): TreeNode | null => {
    const kids = t.children.map(walk).filter(Boolean) as TreeNode[];
    if (match(t) || kids.length) return { row: t.row, children: kids };
    return null;
  };
  return roots.map(walk).filter(Boolean) as TreeNode[];
}

/** Flatten a node + its whole subtree into document order. */
function flattenSubtree(root: TreeNode): OutlineRow[] {
  const out: OutlineRow[] = [];
  const walk = (t: TreeNode) => {
    out.push(t.row);
    t.children.forEach(walk);
  };
  walk(root);
  return out;
}

function findNode(roots: TreeNode[], id: number): TreeNode | null {
  for (const r of roots) {
    if (r.row.id === id) return r;
    const found = findNode(r.children, id);
    if (found) return found;
  }
  return null;
}

function buildBreadcrumbs(roots: TreeNode[], id: number): OutlineRow[] {
  const path: OutlineRow[] = [];
  const walk = (nodes: TreeNode[], trail: OutlineRow[]): boolean => {
    for (const n of nodes) {
      const next = [...trail, n.row];
      if (n.row.id === id) {
        path.push(...next);
        return true;
      }
      if (walk(n.children, next)) return true;
    }
    return false;
  };
  walk(roots, []);
  return path;
}

export function BrowserPanel() {
  const catalog = useCatalog();
  const [bookId, setBookId] = useState<number | "">("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [filter, setFilter] = useState("");
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

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
    return out;
  }, [catalog.data]);

  const groupedForSelect = useMemo(() => {
    const m = new Map<string, IndexedBook[]>();
    for (const item of indexed) {
      if (!m.has(item.authority)) m.set(item.authority, []);
      m.get(item.authority)!.push(item);
    }
    return [...m.entries()];
  }, [indexed]);

  const effectiveBookId = useMemo<number | null>(() => {
    if (bookId !== "") return bookId;
    if (indexed.length === 1) return indexed[0].book.id;
    return null;
  }, [bookId, indexed]);

  const outline = useBookOutline(effectiveBookId);
  const pdfs = useBookPdfs(effectiveBookId);
  const pdfId = pdfs.data && pdfs.data.length > 0 ? pdfs.data[0].id : null;

  const tree = useMemo(() => {
    if (!outline.data) return [] as TreeNode[];
    return buildTree(outline.data);
  }, [outline.data]);

  const visibleTree = useMemo(
    () => (filter.trim() ? filterTree(tree, filter) : tree),
    [tree, filter],
  );

  const effectiveExpanded = useMemo(() => {
    if (filter.trim()) {
      const s = new Set<string>();
      const walk = (t: TreeNode) => {
        s.add(t.row.section_number);
        t.children.forEach(walk);
      };
      visibleTree.forEach(walk);
      return s;
    }
    return expanded;
  }, [visibleTree, filter, expanded]);

  const selectedNode = useMemo(
    () => (selectedId != null ? findNode(tree, selectedId) : null),
    [tree, selectedId],
  );
  const breadcrumbs = useMemo(
    () => (selectedId != null ? buildBreadcrumbs(tree, selectedId) : []),
    [tree, selectedId],
  );

  const canBrowse = indexed.length > 0;
  const viewing = selectedId != null && selectedNode != null;

  return (
    <div className="h-full flex flex-col">
      {/* Toolbar */}
      <div className="px-6 py-3 border-b border-surface-400 bg-surface-800 flex gap-2 items-center">
        {viewing ? (
          <button
            type="button"
            className="btn-ghost text-xs"
            onClick={() => setSelectedId(null)}
          >
            ← Back to outline
          </button>
        ) : (
          <>
            <select
              className="input max-w-md"
              value={effectiveBookId ?? ""}
              onChange={(e) => {
                setBookId(e.target.value ? Number(e.target.value) : "");
                setExpanded(new Set());
              }}
              disabled={!canBrowse}
            >
              <option value="">
                {canBrowse ? `— select code book —` : "— no codes indexed —"}
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
            <input
              className="input flex-1"
              placeholder={
                effectiveBookId
                  ? "Filter this book (e.g. 26.1 or wind)…"
                  : "Select a code book to browse"
              }
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              disabled={effectiveBookId == null}
            />
          </>
        )}
      </div>

      {/* Empty state */}
      {!canBrowse && !catalog.isLoading && (
        <div className="p-10 text-sm text-surface-100">
          No codes have been indexed yet. Upload a PDF via the Import panel.
        </div>
      )}

      {/* Outline view */}
      {canBrowse && !viewing && (
        <div className="flex-1 overflow-y-auto">
          {outline.isLoading && (
            <div className="p-4 text-xs text-surface-100">Loading outline…</div>
          )}
          {outline.data && visibleTree.length === 0 && (
            <div className="p-4 text-xs text-surface-100">
              {filter.trim() ? "No sections match." : "No sections."}
            </div>
          )}
          <TreeView
            nodes={visibleTree}
            onSelect={setSelectedId}
            expanded={effectiveExpanded}
            toggle={(k) =>
              setExpanded((prev) => {
                const next = new Set(prev);
                if (next.has(k)) next.delete(k);
                else next.add(k);
                return next;
              })
            }
          />
        </div>
      )}

      {/* Reading view — selected node + all descendants inline */}
      {canBrowse && viewing && selectedNode && (
        <div className="flex-1 overflow-y-auto bg-surface-900/40">
          <Reader
            key={selectedNode.row.id}
            node={selectedNode}
            breadcrumbs={breadcrumbs}
            pdfId={pdfId}
            onJump={setSelectedId}
          />
        </div>
      )}
    </div>
  );
}

function TreeView({
  nodes,
  onSelect,
  expanded,
  toggle,
  depth = 0,
}: {
  nodes: TreeNode[];
  onSelect: (id: number) => void;
  expanded: Set<string>;
  toggle: (key: string) => void;
  depth?: number;
}) {
  return (
    <ul>
      {nodes.map((n) => {
        const key = n.row.section_number;
        const hasKids = n.children.length > 0;
        const isOpen = expanded.has(key);
        return (
          <li key={`${n.row.id}-${key}`}>
            <div
              className="flex items-start gap-1 cursor-pointer px-2 py-1 text-sm hover:bg-surface-700/50 text-surface-50"
              style={{ paddingLeft: 10 + depth * 16 }}
              onClick={() => onSelect(n.row.id)}
            >
              <button
                type="button"
                className="w-3 text-surface-100 shrink-0"
                onClick={(e) => {
                  e.stopPropagation();
                  if (hasKids) toggle(key);
                }}
                aria-label={hasKids ? (isOpen ? "Collapse" : "Expand") : ""}
              >
                {hasKids ? (isOpen ? "▾" : "▸") : " "}
              </button>
              <span className="font-mono text-accent shrink-0">
                {n.row.section_number}
              </span>
              <span className="truncate">{n.row.section_title || ""}</span>
            </div>
            {hasKids && isOpen && (
              <TreeView
                nodes={n.children}
                onSelect={onSelect}
                expanded={expanded}
                toggle={toggle}
                depth={depth + 1}
              />
            )}
          </li>
        );
      })}
    </ul>
  );
}

/**
 * The reading view. Flattens the node's subtree into document order and
 * renders each section as its own block with depth-appropriate heading
 * size and indentation. This is the UpCodes reading experience — "open
 * Chapter 26 and scroll straight through it".
 */
function Reader({
  node,
  breadcrumbs,
  pdfId,
  onJump,
}: {
  node: TreeNode;
  breadcrumbs: OutlineRow[];
  pdfId: number | null;
  onJump: (id: number) => void;
}) {
  const rows = useMemo(() => flattenSubtree(node), [node]);
  const baseDepth = rows[0]?.depth ?? 0;
  const rootNumber = rows[0]?.section_number;

  return (
    <div className="max-w-3xl mx-auto px-6 py-8">
      {/* Breadcrumb */}
      {breadcrumbs.length > 1 && (
        <nav className="text-xs text-surface-100 mb-4 flex flex-wrap gap-1 items-center">
          {breadcrumbs.map((b, i) => (
            <span key={b.id} className="flex items-center gap-1">
              {i > 0 && <span className="text-surface-200">/</span>}
              {i < breadcrumbs.length - 1 ? (
                <button
                  type="button"
                  className="hover:text-accent underline-offset-2 hover:underline"
                  onClick={() => onJump(b.id)}
                >
                  {b.section_number}
                </button>
              ) : (
                <span className="text-surface-50">{b.section_number}</span>
              )}
            </span>
          ))}
        </nav>
      )}

      {/* Sections */}
      {rows.map((row) => (
        <SectionBlock
          key={row.id}
          row={row}
          isRoot={row.section_number === rootNumber}
          indent={Math.max(0, row.depth - baseDepth)}
          pdfId={pdfId}
        />
      ))}
    </div>
  );
}

function SectionBlock({
  row,
  isRoot,
  indent,
  pdfId,
}: {
  row: OutlineRow;
  isRoot: boolean;
  indent: number;
  pdfId: number | null;
}) {
  const [showPage, setShowPage] = useState(false);
  const title = row.section_title || "";
  const num = row.section_number;

  // Depth controls visual hierarchy. Root node of the reading view gets
  // the page-title treatment; deeper subsections get progressively
  // smaller / more indented. This mirrors how UpCodes renders a chapter:
  // one big chapter title then a flowing stream of subsection blocks.
  const HeadTag: "h1" | "h2" | "h3" | "h4" =
    isRoot ? "h1" : indent === 0 ? "h2" : indent === 1 ? "h3" : "h4";
  const headClass = {
    h1: "text-2xl font-semibold text-white",
    h2: "text-xl font-semibold text-white",
    h3: "text-base font-semibold text-white",
    h4: "text-sm font-semibold text-white uppercase tracking-wide",
  }[HeadTag];

  return (
    <section
      id={`sec-${row.id}`}
      className={isRoot ? "mb-8" : "mt-6"}
      style={{ paddingLeft: indent * 14 }}
    >
      <HeadTag className={`${headClass} flex items-baseline gap-2`}>
        <span className="font-mono text-accent">{num}</span>
        <span>{title}</span>
      </HeadTag>

      {row.has_ca_amendment && row.amendment_agency && (
        <div className="mt-1 text-xs inline-block px-2 py-0.5 rounded bg-amber-900/40 text-amber-200">
          {row.amendment_agency} amendment
        </div>
      )}

      {row.full_text && (
        <pre className="mt-2 whitespace-pre-wrap text-[15px] leading-7 text-surface-50 font-sans">
          {row.full_text}
        </pre>
      )}

      {pdfId != null && row.page_number != null && (
        <div className="mt-3 text-xs text-surface-100">
          {showPage ? (
            <div>
              <button
                type="button"
                className="btn-ghost text-xs mb-2"
                onClick={() => setShowPage(false)}
              >
                Hide PDF page {row.page_number}
              </button>
              <img
                src={pdfPageImageUrl(pdfId, row.page_number, 150)}
                alt={`PDF page ${row.page_number}`}
                className="border border-surface-400 rounded bg-white w-full"
              />
            </div>
          ) : (
            <button
              type="button"
              className="underline hover:text-accent"
              onClick={() => setShowPage(true)}
            >
              View PDF page {row.page_number} →
            </button>
          )}
        </div>
      )}
    </section>
  );
}
