import { useEffect, useMemo, useState, type ReactNode } from "react";
import { pdfPageImageUrl } from "../api/client";
import type { CatalogBook } from "../api/types";
import { useCatalog } from "../hooks/useCatalog";
import { usePdfMeta } from "../hooks/useReview";
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

/**
 * Reflow PDF-extracted body text into prose paragraphs. PyMuPDF's text
 * layer preserves visual line wraps from the printed page — i.e. every
 * line-break in the output is usually just where the PDF wrapped, not
 * where the author intended a new paragraph. We undo that so a section
 * reads as flowing prose.
 *
 * Heuristics, in order:
 *   1. Strip the leading "N.N.N  Title" header if the text starts with
 *      its own section_number (redundant — we already rendered the
 *      heading in <h1/2/3/4>).
 *   2. Split on blank lines as hard paragraph boundaries.
 *   3. Within a block, put each list item / EXCEPTION / Note on its own
 *      paragraph so the ordered-list structure of the code survives.
 *   4. All remaining soft line wraps collapse to a single space. De-hyphenates
 *      words split across wraps ("build-\ning" → "building").
 */
function reflow(text: string, leadingHeader?: string): string[] {
  if (!text) return [];
  let work = text;
  if (leadingHeader) {
    // If the first non-whitespace token is the section number with
    // optional title, strip it — we already show it as the heading.
    const esc = leadingHeader.replace(/[-\/\\^$*+?.()|[\]{}]/g, "\\$&");
    const re = new RegExp(`^\\s*${esc}[^\\n]{0,120}?(\\r?\\n|$)`);
    work = work.replace(re, "");
  }

  // 2. Split on blank lines.
  const blocks = work.split(/\n\s*\n+/);
  const paragraphs: string[] = [];

  for (const block of blocks) {
    const lines = block
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);
    if (lines.length === 0) continue;

    let buf = "";
    const flush = () => {
      if (buf) {
        paragraphs.push(collapse(buf));
        buf = "";
      }
    };

    for (const line of lines) {
      const isListLike =
        /^(\d+\.\s)/.test(line) ||
        /^\(\s*[a-z0-9ivx]+\s*\)/i.test(line) ||
        /^(EXCEPTION|EXCEPTIONS|User Note|Note|COMMENTARY)s?:/i.test(line) ||
        /^[•◦·●]\s/.test(line);

      if (isListLike) {
        flush();
        buf = line;
      } else if (buf && /-$/.test(buf)) {
        // Word split across wrapped lines: drop the hyphen, no space.
        buf = buf.slice(0, -1) + line;
      } else if (buf) {
        buf += " " + line;
      } else {
        buf = line;
      }
    }
    flush();
  }

  return paragraphs;
}

function collapse(s: string): string {
  return s.replace(/\s{2,}/g, " ").trim();
}

function isListItem(p: string): boolean {
  return (
    /^(\d+[a-z]?\.\s)/.test(p) ||
    /^\(\s*[a-z0-9ivx]+\s*\)/i.test(p) ||
    /^[•◦·●]\s/.test(p)
  );
}

/**
 * A formula line is a list-style item (numbered/lettered) whose body
 * contains arithmetic operators or parentheses — i.e. the ASCE load
 * combinations "1a. D + L", "6a. D + 0.75L + 0.75(…)", etc. We render
 * these in monospace so the equation aligns visually.
 */
const _FORMULA_STARTER = /^\d+[a-z]?\.\s+[A-Z0-9(]/;
const _FORMULA_OPERATOR = /[+\u2212=]|\(/; // +, −, =, (
function isFormulaLine(p: string): boolean {
  return _FORMULA_STARTER.test(p) && _FORMULA_OPERATOR.test(p);
}

/**
 * Turn "W_T", "L_r", "S_DS" style tokens (emitted by the backend
 * subscript restorer) into <span>X<sub>Y</sub></span>. Token shape:
 * one to six capital letters, then "_", then one to three alphanum
 * characters. Anything else stays literal.
 */
const _SUBSCRIPT_TOKEN_RE = /\b([A-Z][A-Za-z]{0,5})_([A-Za-z0-9]{1,3})\b/g;
function renderWithSubscripts(text: string): ReactNode[] {
  const parts: ReactNode[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  let key = 0;
  while ((m = _SUBSCRIPT_TOKEN_RE.exec(text)) !== null) {
    if (m.index > last) parts.push(text.slice(last, m.index));
    parts.push(
      <span key={`s${key++}`}>
        {m[1]}
        <sub>{m[2]}</sub>
      </span>,
    );
    last = m.index + m[0].length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts;
}

/**
 * Replace "Section X.Y.Z", "Chapter N", "Figure X.Y-Z", "Table X.Y-Z",
 * "Appendix X" occurrences with clickable links when the target exists
 * in the current book's outline. Plain text is returned for references
 * to sections outside the book (other codes, external standards).
 */
function linkifyRefs(
  text: string,
  refIndex: Map<string, OutlineRow>,
  onJump: (id: number) => void,
): ReactNode[] {
  const parts: ReactNode[] = [];
  const regex =
    /(Sections?|Chapters?|Figures?|Tables?|Appendix|Appendices)\s+([A-Z]{0,3}\d+(?:[.\-]\d+)*(?:[A-Z]+)?)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  let key = 0;
  // Non-link segments are routed through renderWithSubscripts so "W_T"
  // becomes W<sub>T</sub>; the link anchor text isn't subscript-rewritten
  // because Section/Chapter/Figure identifiers never use underscores.
  const pushText = (s: string) => {
    if (!s) return;
    for (const node of renderWithSubscripts(s)) parts.push(node);
  };
  while ((m = regex.exec(text)) !== null) {
    const [full, , num] = m;
    if (m.index > last) pushText(text.slice(last, m.index));
    // Try direct hit, then "Chapter N" / "Figure X.Y" forms.
    const candidates = [
      num,
      `Chapter ${num}`,
      `Figure ${num}`,
      `Table ${num}`,
      `Appendix ${num}`,
    ];
    const target = candidates
      .map((c) => refIndex.get(c))
      .find(Boolean) as OutlineRow | undefined;
    if (target) {
      parts.push(
        <a
          key={`r${key++}`}
          className="text-accent hover:underline cursor-pointer"
          onClick={() => onJump(target.id)}
        >
          {full}
        </a>,
      );
    } else {
      parts.push(full);
    }
    last = regex.lastIndex;
  }
  if (last < text.length) pushText(text.slice(last));
  return parts;
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
            allRows={outline.data ?? []}
            onJump={setSelectedId}
            bookLabel={
              indexed.find((i) => i.book.id === effectiveBookId)?.book
                .abbreviation ||
              indexed.find((i) => i.book.id === effectiveBookId)?.book
                .code_name ||
              null
            }
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
  allRows,
  onJump,
  bookLabel,
}: {
  node: TreeNode;
  breadcrumbs: OutlineRow[];
  pdfId: number | null;
  allRows: OutlineRow[];
  onJump: (id: number) => void;
  bookLabel: string | null;
}) {
  const rows = useMemo(() => flattenSubtree(node), [node]);
  const baseDepth = rows[0]?.depth ?? 0;
  const rootNumber = rows[0]?.section_number;

  // Build a cheap lookup table so "Section 26.5.2" references in body
  // text can be linkified to the section's in-book anchor. Keyed by
  // section_number so both "26.5.2" and "Figure 26.5-1B" resolve if
  // they exist in the outline.
  const refIndex = useMemo(() => {
    const m = new Map<string, OutlineRow>();
    for (const r of allRows) {
      const key = r.section_number.trim();
      if (key && !m.has(key)) m.set(key, r);
    }
    return m;
  }, [allRows]);

  // Cross-reference click handler. If the target is already rendered
  // inside this chapter view, scroll smoothly to its anchor; otherwise
  // fall back to onJump() which swaps the whole reader subtree.
  const inViewIds = useMemo(() => new Set(rows.map((r) => r.id)), [rows]);
  const handleJump = (id: number) => {
    if (inViewIds.has(id)) {
      const el = document.getElementById(`sec-${id}`);
      if (el) {
        el.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
    }
    onJump(id);
  };

  return (
    <div className="max-w-[72ch] mx-auto px-8 py-10 text-[17px] leading-[1.75]">
      {/* Breadcrumb — book abbreviation prefix, then the chapter/section trail */}
      {(bookLabel || breadcrumbs.length > 1) && (
        <nav className="text-sm text-surface-100 mb-8 flex flex-wrap gap-x-2 gap-y-1 items-baseline">
          {bookLabel && (
            <span className="text-xs font-semibold uppercase tracking-widest text-surface-200">
              {bookLabel}
            </span>
          )}
          {breadcrumbs.map((b, i) => (
            <span key={b.id} className="flex items-baseline gap-2">
              <span className="text-surface-200">·</span>
              {i < breadcrumbs.length - 1 ? (
                <button
                  type="button"
                  className="hover:text-accent underline-offset-2 hover:underline tabular-nums"
                  onClick={() => handleJump(b.id)}
                >
                  {b.section_number}
                </button>
              ) : (
                <span className="text-surface-50 tabular-nums">{b.section_number}</span>
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
          relativeDepth={Math.max(0, row.depth - baseDepth)}
          pdfId={pdfId}
          refIndex={refIndex}
          onJump={handleJump}
        />
      ))}
    </div>
  );
}

/** DOM-safe slug of a section_number. "26.1.2.1" → "26-1-2-1". */
function slugifySectionNumber(s: string): string {
  return s.trim().replace(/[^A-Za-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

const CALLOUT_RE = /^(EXCEPTION|EXCEPTIONS|User Note|Note|COMMENTARY)s?:\s*/i;

function SectionBlock({
  row,
  isRoot,
  relativeDepth,
  pdfId,
  refIndex,
  onJump,
}: {
  row: OutlineRow;
  isRoot: boolean;
  relativeDepth: number;
  pdfId: number | null;
  refIndex: Map<string, OutlineRow>;
  onJump: (id: number) => void;
}) {
  const [showPage, setShowPage] = useState(false);
  const title = row.section_title || "";
  const num = row.section_number;

  const paragraphs = useMemo(
    () => reflow(row.full_text || "", row.section_number),
    [row.full_text, row.section_number],
  );

  const innerContent = (
    <>
      <SectionHeader
        num={num}
        title={title}
        isRoot={isRoot}
        relativeDepth={relativeDepth}
      />

      {paragraphs.length > 0 && (
        <div className="mt-3 text-surface-50 space-y-4">
          {paragraphs.map((p, i) => {
            const calloutMatch = p.match(CALLOUT_RE);
            if (calloutMatch) {
              const stripped = p.slice(calloutMatch[0].length);
              return (
                <Callout key={i} label={calloutMatch[1].toUpperCase()}>
                  {linkifyRefs(stripped, refIndex, onJump)}
                </Callout>
              );
            }
            const formula = isFormulaLine(p);
            const classes = formula
              ? "pl-6 -indent-6 font-mono text-[15px] whitespace-pre-wrap"
              : isListItem(p)
                ? "pl-6 -indent-6"
                : "";
            return (
              <p key={i} className={classes}>
                {linkifyRefs(p, refIndex, onJump)}
              </p>
            );
          })}
        </div>
      )}

      {pdfId != null && row.page_number != null && (
        <div className="mt-4 text-xs text-surface-100">
          {showPage ? (
            <PdfPager
              pdfId={pdfId}
              startPage={row.page_number}
              onClose={() => setShowPage(false)}
            />
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
    </>
  );

  // Amendment sidebar: wrap the whole section in a left-border block
  // instead of a floating chip.
  const amendmentWrap = row.has_ca_amendment && row.amendment_agency;

  // Top margin depends on depth: chapters get the most air, subsections
  // least. Keeps the overall outline rhythm relaxed without the ragged
  // paddingLeft-based indentation the old layout used.
  const topMargin = isRoot ? "" : relativeDepth === 0 ? "mt-12" : relativeDepth === 1 ? "mt-8" : "mt-6";

  return (
    <section
      id={`sec-${row.id}`}
      className={`${isRoot ? "mb-10" : topMargin} scroll-mt-6`}
    >
      {/* Dual anchor so future href="#26-1-2" deep-links work. */}
      <a id={slugifySectionNumber(num)} aria-hidden className="sr-only" />
      {amendmentWrap ? (
        <div className="border-l-2 border-amber-500 pl-4 -ml-4">
          <div className="text-[11px] font-semibold uppercase tracking-wider text-amber-600 mb-1">
            {row.amendment_agency} amendment
          </div>
          {innerContent}
        </div>
      ) : (
        innerContent
      )}
    </section>
  );
}

/**
 * UpCodes-style section header. Root drill-down target gets a two-line
 * treatment (small accent "CHAPTER 26" eyebrow + big title). Nested
 * levels keep the section number inline with the title in an accent
 * color, using tabular-nums so numbers right-align in the visual gutter.
 */
function SectionHeader({
  num,
  title,
  isRoot,
  relativeDepth,
}: {
  num: string;
  title: string;
  isRoot: boolean;
  relativeDepth: number;
}) {
  if (isRoot) {
    return (
      <header>
        <div className="text-xs font-semibold tracking-[0.2em] uppercase text-accent">
          {num}
        </div>
        <h1 className="text-3xl font-semibold mt-1 text-surface-50 leading-tight">
          {title}
        </h1>
      </header>
    );
  }
  if (relativeDepth === 0) {
    return (
      <h2 className="text-2xl font-semibold text-surface-50 flex items-baseline gap-3 leading-tight">
        <span className="text-accent tabular-nums">{num}</span>
        <span>{title}</span>
      </h2>
    );
  }
  if (relativeDepth === 1) {
    return (
      <h3 className="text-lg font-semibold text-surface-50 flex items-baseline gap-2 leading-snug">
        <span className="text-accent tabular-nums">{num}</span>
        <span>{title}</span>
      </h3>
    );
  }
  // Deepest levels: small, capitalized metadata-style.
  return (
    <h4 className="text-sm font-semibold text-surface-100 flex items-baseline gap-2 uppercase tracking-wide">
      <span className="text-accent tabular-nums normal-case tracking-normal">
        {num}
      </span>
      <span>{title}</span>
    </h4>
  );
}

/**
 * Bordered aside used for EXCEPTION, User Note, and COMMENTARY blocks
 * inside a section. UpCodes uses a left-border accent strip with a
 * small uppercase label above the body.
 */
function Callout({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <aside className="my-2 border-l-2 border-accent pl-4 py-2 bg-surface-800/60 rounded-r">
      <div className="text-[11px] font-semibold uppercase tracking-wider text-accent mb-1">
        {label}
      </div>
      <div>{children}</div>
    </aside>
  );
}

/**
 * Inline pager for the source PDF.
 *
 *   - Opens on the section's own ``page_number``; ◀ / ▶ walk adjacent pages.
 *   - Zoom: − / + step through DPI presets, refetching a crisper PNG at
 *     the new DPI. Keyboard ``-`` / ``+`` (or ``=``) also work.
 *   - Loading bar: while the current page PNG is in flight, an indeterminate
 *     progress stripe animates across the top of the image area. Clears on
 *     ``<img onLoad>``.
 *   - Keyboard: ``ArrowLeft`` / ``ArrowRight`` to page, ``-`` / ``+`` to
 *     zoom, while the wrapper has focus (``tabIndex={0}``).
 *   - Neighbor pages are prefetched at the current DPI so flips feel
 *     instant once the browser has them cached.
 */

/** DPI presets. Kept in sync with the backend clamp (72–300). */
const PDF_DPI_STEPS = [100, 125, 150, 200, 250, 300] as const;
const PDF_DPI_DEFAULT = 150;

function PdfPager({
  pdfId,
  startPage,
  onClose,
}: {
  pdfId: number;
  startPage: number;
  onClose: () => void;
}) {
  const [page, setPage] = useState<number>(startPage);
  const [dpi, setDpi] = useState<number>(PDF_DPI_DEFAULT);
  const [loading, setLoading] = useState<boolean>(true);
  const meta = usePdfMeta(pdfId);
  const pageCount = meta.data?.page_count ?? null;

  // If the caller swaps to a different section without unmounting us
  // (e.g. breadcrumb navigation reusing the component), sync to the new
  // starting page. Normal section switches unmount via <Reader key=…>.
  useEffect(() => {
    setPage(startPage);
  }, [startPage]);

  // Whenever page or zoom changes, the <img> is about to fetch a new
  // URL. Flip loading true until onLoad fires.
  useEffect(() => {
    setLoading(true);
  }, [page, dpi, pdfId]);

  const clampedPage = (p: number) => {
    if (p < 1) return 1;
    if (pageCount != null && p > pageCount) return pageCount;
    return p;
  };
  const goPrev = () => setPage((p) => clampedPage(p - 1));
  const goNext = () => setPage((p) => clampedPage(p + 1));

  const zoomIdx = PDF_DPI_STEPS.indexOf(dpi as (typeof PDF_DPI_STEPS)[number]);
  const safeIdx = zoomIdx < 0 ? PDF_DPI_STEPS.indexOf(PDF_DPI_DEFAULT) : zoomIdx;
  const atMinZoom = safeIdx <= 0;
  const atMaxZoom = safeIdx >= PDF_DPI_STEPS.length - 1;
  const zoomOut = () => {
    if (!atMinZoom) setDpi(PDF_DPI_STEPS[safeIdx - 1]);
  };
  const zoomIn = () => {
    if (!atMaxZoom) setDpi(PDF_DPI_STEPS[safeIdx + 1]);
  };
  const zoomReset = () => setDpi(PDF_DPI_DEFAULT);
  const zoomPct = Math.round((dpi / PDF_DPI_DEFAULT) * 100);

  const atStart = page <= 1;
  const atEnd = pageCount != null && page >= pageCount;

  return (
    <div
      tabIndex={0}
      className="outline-none focus-visible:ring-1 focus-visible:ring-accent/60 rounded"
      onKeyDown={(e) => {
        if (e.key === "ArrowLeft") {
          if (!atStart) goPrev();
          e.preventDefault();
        } else if (e.key === "ArrowRight") {
          if (!atEnd) goNext();
          e.preventDefault();
        } else if (e.key === "+" || e.key === "=") {
          if (!atMaxZoom) zoomIn();
          e.preventDefault();
        } else if (e.key === "-" || e.key === "_") {
          if (!atMinZoom) zoomOut();
          e.preventDefault();
        } else if (e.key === "0") {
          zoomReset();
          e.preventDefault();
        }
      }}
    >
      <div className="flex items-center justify-between mb-2 gap-2 flex-wrap">
        <button type="button" className="btn-ghost text-xs" onClick={onClose}>
          Hide PDF page {page}
        </button>

        {/* Zoom controls */}
        <div className="flex items-center gap-1">
          <button
            type="button"
            className="px-2 py-0.5 rounded border border-surface-400 bg-surface-800 hover:bg-surface-700 disabled:opacity-40 disabled:cursor-not-allowed"
            onClick={zoomOut}
            disabled={atMinZoom}
            aria-label="Zoom out"
            title="Zoom out (−)"
          >
            −
          </button>
          <button
            type="button"
            className="px-2 py-0.5 rounded border border-surface-400 bg-surface-800 hover:bg-surface-700 min-w-[3.5rem] tabular-nums"
            onClick={zoomReset}
            aria-label={`Current zoom ${zoomPct}% — click to reset`}
            title="Reset zoom (0)"
          >
            {zoomPct}%
          </button>
          <button
            type="button"
            className="px-2 py-0.5 rounded border border-surface-400 bg-surface-800 hover:bg-surface-700 disabled:opacity-40 disabled:cursor-not-allowed"
            onClick={zoomIn}
            disabled={atMaxZoom}
            aria-label="Zoom in"
            title="Zoom in (+)"
          >
            +
          </button>
        </div>

        <div className="flex items-center gap-2">
          {page !== startPage && (
            <button
              type="button"
              className="underline hover:text-accent"
              onClick={() => setPage(startPage)}
              title="Return to the page this section starts on"
            >
              Back to page {startPage}
            </button>
          )}
          <span>
            Page {page}
            {pageCount != null ? ` / ${pageCount}` : ""}
          </span>
        </div>
      </div>

      <div className="flex items-stretch gap-2">
        <button
          type="button"
          className="px-2 py-1 rounded border border-surface-400 bg-surface-800 hover:bg-surface-700 disabled:opacity-40 disabled:cursor-not-allowed shrink-0"
          onClick={goPrev}
          disabled={atStart}
          aria-label="Previous page"
        >
          ◀
        </button>

        {/* Image + loading bar overlay. The bar is an animated stripe
            pinned to the top of the image frame; it clears the instant
            onLoad fires so users get fast visual feedback on cache hits
            (neighbor prefetch) and a clear "still loading" signal on
            zoom-up misses. */}
        <div className="relative flex-1 min-w-0">
          {loading && (
            <div className="absolute top-0 left-0 right-0 h-1 overflow-hidden rounded-t">
              <div className="h-full w-1/3 bg-accent/80 animate-[pdf-bar_1.1s_ease-in-out_infinite]" />
            </div>
          )}
          <img
            src={pdfPageImageUrl(pdfId, page, dpi)}
            alt={`PDF page ${page}`}
            onLoad={() => setLoading(false)}
            onError={() => setLoading(false)}
            className="border border-surface-400 rounded bg-white w-full object-contain"
          />
        </div>

        <button
          type="button"
          className="px-2 py-1 rounded border border-surface-400 bg-surface-800 hover:bg-surface-700 disabled:opacity-40 disabled:cursor-not-allowed shrink-0"
          onClick={goNext}
          disabled={atEnd}
          aria-label="Next page"
        >
          ▶
        </button>
      </div>

      {/* Prefetch ±2 neighbors at current zoom so page flips are instant
          even when the reader skips ahead two. Zero-sized, no layout
          impact; the browser's HTTP cache plus our server-side PNG cache
          mean repeated flips are essentially free. */}
      {[-2, -1, 1, 2].map((delta) => {
        const p = page + delta;
        if (p < 1) return null;
        if (pageCount != null && p > pageCount) return null;
        return (
          <img
            key={`pre-${delta}`}
            src={pdfPageImageUrl(pdfId, p, dpi)}
            alt=""
            aria-hidden
            className="w-0 h-0 opacity-0 pointer-events-none"
          />
        );
      })}
    </div>
  );
}
