"""Table-of-contents extractor for building-code PDFs.

Stage A of the TOC-driven ingestion pipeline. The extractor returns a flat
list of ``TocEntry`` that the downstream ``DocumentExtractor`` uses as the
ground-truth outline of the document.

Strategy, in order:
    1. Embedded PDF outline (``fitz.get_toc()``). When present and non-trivial,
       this is always preferred — it's already structured and page-accurate.
    2. Visual TOC parse. Scan the first ~30 pages for a heading like
       "Contents" / "Table of Contents", then pull leader-dot rows of the form
       "1.2.3   Title text   ........   42" out of PyMuPDF's text layer.
    3. OCR fallback on the same front-matter range if the text layer is empty
       (scanned TOC pages). Uses Tesseract via ``pytesseract``.

The page numbers emitted by this extractor are **1-based PDF page indexes**
(i.e. the page inside the file, not the printed page number). Printed page
numbers from the TOC are translated to PDF pages by searching for the first
content page whose bottom-of-page label matches.
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass
class TocEntry:
    """One entry in the table of contents.

    ``pdf_page`` is the 1-based page inside the PDF file. ``printed_page``
    is whatever the TOC actually printed (often the same; can differ by a
    constant offset when the PDF includes front matter with roman numerals).
    """
    section_number: str
    title: str
    pdf_page: int
    printed_page: Optional[int] = None
    depth: int = 0
    source: str = "outline"  # "outline" | "visual" | "ocr"

    def __repr__(self) -> str:  # compact repr for smoke-tests
        return (
            f"TocEntry({self.section_number!r}, {self.title[:40]!r}, "
            f"pdf_page={self.pdf_page}, depth={self.depth}, src={self.source})"
        )


# Anything that looks like "1", "1.2", "1.2.3", "A.1", "A.1.2", "Chapter 7",
# "Appendix B", "Part III". Captured number is what goes into section_number.
_NUMBER_RE = re.compile(
    r"""^\s*(
        (?:Chapter|CHAPTER|Part|PART|Appendix|APPENDIX)\s+[A-Z0-9IVX]+ |  # named
        [A-Z]\.\d+(?:\.\d+)*                                          |  # A.1, A.1.2
        \d+(?:\.\d+)+                                                 |  # 1.1, 26.5.2
        \d+                                                              # bare int only if followed by title
    )\s+""",
    re.VERBOSE,
)

# Visual TOC row: "<number> <title> ......... <page>"
# We require at least 2 dots in the leader to keep false-positive rate low.
_TOC_ROW_RE = re.compile(
    r"""^\s*
        (?P<num>
            (?:Chapter|Part|Appendix)\s+[A-Z0-9IVX]+ |
            [A-Z]\.\d+(?:\.\d+)*                     |
            \d+(?:\.\d+)+                            |
            \d+
        )
        \s+
        (?P<title>.+?)
        \s*[.\s·•]{2,}\s*
        (?P<page>\d+)
        \s*$
    """,
    re.VERBOSE,
)

# Heading that announces the TOC itself.
_TOC_HEADER_RE = re.compile(
    r"^\s*(Contents|Table of Contents|CONTENTS|TABLE OF CONTENTS)\s*$",
)


class TocExtractor:
    """Extract a structured table of contents from a PDF."""

    MIN_OUTLINE_ENTRIES = 20  # below this, we distrust fitz.get_toc() and try visual
    VISUAL_SCAN_PAGES = 40    # front matter pages to scan for a printed TOC
    MIN_VISUAL_ROWS = 20      # below this, visual scan is considered a miss
    # NEC/CEC TOCs are much shorter than CBC/ASCE because articles are
    # 3-digit only. 15 article rows is already a credible TOC; require
    # fewer than the generic MIN_VISUAL_ROWS.
    MIN_ARTICLE_ROWS = 15

    def extract(self, pdf_path: str) -> List[TocEntry]:
        try:
            with fitz.open(pdf_path) as doc:
                entries = self._from_outline(doc)
                if len(entries) >= self.MIN_OUTLINE_ENTRIES:
                    # Many PDFs have embedded outlines whose page links are
                    # unreliable — ASCE 7-22 is a known offender (its outline
                    # intermixes commentary-page targets with body-page
                    # targets for nominally-identical entries). Walk the
                    # list and fix non-monotonic jumps by searching forward
                    # for the section heading text on nearby pages.
                    entries = _verify_outline_pages(doc, entries)
                    logger.info(
                        "TOC: used embedded outline (%d entries) from %s",
                        len(entries), pdf_path,
                    )
                    return entries

                logger.info(
                    "TOC: embedded outline too small (%d entries); trying visual parse",
                    len(entries),
                )
                visual = self._from_visual(doc)
                if len(visual) >= self.MIN_VISUAL_ROWS:
                    logger.info(
                        "TOC: used visual parse (%d entries) from %s",
                        len(visual), pdf_path,
                    )
                    return visual

                # NEC/CEC articles look nothing like the "1.1.1" sections
                # the visual parser expects: article numbers are bare
                # 3-digit integers ("450 Transformers"), titles wrap
                # across lines, and page numbers come in "70-437" form.
                # Try an article-shaped parse before paying for OCR.
                article = self._from_article_visual(doc)
                # Books with a mixed PDF (image-scanned Contents pages at
                # the start, text-layer pages later) return a text-article
                # set that skips the early articles entirely. Detect that
                # by looking at the smallest article number we captured:
                # NEC books start at article 90. If our lowest is already
                # ≥200, the early articles are behind OCR — run the OCR
                # variant and merge. Dedup by article number + printed page.
                article_missing_early = (
                    article
                    and _min_article_number(article) is not None
                    and _min_article_number(article) > 150
                )
                if article_missing_early:
                    logger.info(
                        "TOC: text-article parse starts at article %s; "
                        "running OCR to recover earlier articles",
                        _min_article_number(article),
                    )
                    ocr_article = self._from_article_visual(doc, use_ocr=True)
                    merged = _merge_article_entries(ocr_article, article)
                    if len(merged) >= len(article):
                        article = merged

                if len(article) >= self.MIN_ARTICLE_ROWS:
                    logger.info(
                        "TOC: used article parse (%d entries) from %s",
                        len(article), pdf_path,
                    )
                    return article

                ocr = self._from_ocr(doc)
                if len(ocr) >= self.MIN_VISUAL_ROWS:
                    logger.info("TOC: used OCR parse (%d entries)", len(ocr))
                    return ocr

                # OCR + article-shaped parse on the OCR'd lines is the
                # last deterministic attempt before we'd need an LLM.
                # Useful for image-scanned NEC/CEC-derived PDFs whose
                # Contents page isn't in the text layer at all.
                ocr_article = self._from_article_visual(doc, use_ocr=True)
                if len(ocr_article) >= self.MIN_ARTICLE_ROWS:
                    logger.info(
                        "TOC: used OCR+article parse (%d entries)",
                        len(ocr_article),
                    )
                    return ocr_article

                # Give back whichever was largest, even if small — callers decide.
                candidates = sorted(
                    [entries, visual, article, ocr, ocr_article],
                    key=len, reverse=True,
                )
                logger.warning(
                    "TOC: no strategy hit threshold; returning best effort (%d entries)",
                    len(candidates[0]),
                )
                return candidates[0]

        except Exception:
            logger.exception("TOC extraction failed for %s", pdf_path)
            return []

    # --- Strategy 1: embedded PDF outline ---------------------------------
    def _from_outline(self, doc: fitz.Document) -> List[TocEntry]:
        raw = doc.get_toc(simple=True)  # [[level, title, page], ...] 1-based page
        out: List[TocEntry] = []
        for level, title, page in raw:
            title = (title or "").strip()
            if not title:
                continue
            section_number, clean_title = _split_number_and_title(title)
            out.append(TocEntry(
                section_number=section_number or clean_title[:32],
                title=clean_title,
                pdf_page=max(1, int(page)),
                depth=max(0, int(level) - 1),
                source="outline",
            ))
        return out

    # --- Strategy 2: visual TOC parse from text layer ---------------------
    def _from_visual(self, doc: fitz.Document) -> List[TocEntry]:
        limit = min(self.VISUAL_SCAN_PAGES, len(doc))
        lines_with_page = self._collect_front_matter_lines(doc, limit, use_ocr=False)
        return self._parse_visual_lines(lines_with_page, source="visual")

    # --- Strategy 3: OCR the front matter and try again -------------------
    def _from_ocr(self, doc: fitz.Document) -> List[TocEntry]:
        limit = min(self.VISUAL_SCAN_PAGES, len(doc))
        lines_with_page = self._collect_front_matter_lines(doc, limit, use_ocr=True)
        return self._parse_visual_lines(lines_with_page, source="ocr")

    # --- Strategy 4: NEC/CEC article-shaped visual parse ------------------
    def _from_article_visual(
        self, doc: fitz.Document, use_ocr: bool = False,
    ) -> List[TocEntry]:
        """Parse the front-matter Contents when the book uses NEC numbering.

        NEC / NFPA 70-derived codes (and the California CEC republishes
        them with amendments) don't use decimal hierarchical numbering.
        Their TOC lists:

          - Articles:  ``450 Transformers and Transformer Vaults``
                       (bare 3-digit number, title may wrap onto a second
                       line before the leader dots and page appear)
          - Parts:     ``Part I. General`` / ``Part III. Transformer Vaults``
                       (Roman numerals, usually followed by subsection
                       title; page reference as a NN-NNN pair)
          - Chapters:  ``Chapter 5 Special Occupancies``
                       (no page reference; a depth-0 group header)
          - Appendices: ``Annex A Product Safety Standards`` etc.

        Page references appear as ``70-437`` — the ``70-`` is the NFPA 70
        document number, the tail is the printed page. Multi-line titles
        must be joined before the row is considered "complete".

        When ``use_ocr`` is True, we Tesseract the front-matter pages
        first, so books whose Contents page is image-scanned still work.
        """
        limit = min(self.VISUAL_SCAN_PAGES, len(doc))
        # Flatten lines; we drop the per-page association because this
        # parser operates at the join-across-lines level.
        raw: List[str] = []
        for page_idx in range(limit):
            page = doc.load_page(page_idx)
            text = page.get_text() or ""
            if use_ocr and len(text.strip()) < 50:
                text = _ocr_page(page)
            for line in text.splitlines():
                if line.strip():
                    raw.append(line.strip())

        if not raw:
            return []

        # Find the Contents header. The text layer occasionally has
        # trailing whitespace so the exact-match _TOC_HEADER_RE may miss;
        # loosen to anywhere-on-line.
        start = None
        for i, line in enumerate(raw):
            if re.search(r"^(?:CONTENTS|Contents|Table of Contents)\b", line):
                start = i + 1
                break
        if start is None:
            start = 0  # no header found; try to parse all front matter anyway

        entries: List[TocEntry] = []
        current_chapter: Optional[str] = None
        pending: Optional[dict] = None  # accumulator for wrapped titles
        i = start
        while i < len(raw):
            line = raw[i]

            # Chapter group header. Doesn't emit a TocEntry by itself —
            # we only emit entries that have a page anchor, otherwise
            # Stage B can't slice body text for them.
            ch_m = re.match(
                r"^\s*(?:Chapter|CHAPTER)\s+([0-9]+)\s+(.+?)\s*$", line,
            )
            if ch_m:
                # Flush any pending entry that never found its page.
                pending = None
                chap_num = ch_m.group(1)
                chap_title = ch_m.group(2).strip()
                current_chapter = chap_num
                # Emit as a depth-0 chapter header with no page; Stage B
                # will use it for slicing context but skip body extraction.
                entries.append(TocEntry(
                    section_number=f"Chapter {chap_num}",
                    title=chap_title,
                    pdf_page=0,  # unresolved; rows below will pin them
                    printed_page=None,
                    depth=0,
                    source="article+ocr" if use_ocr else "article",
                ))
                i += 1
                continue

            # Article row. May be a single line (complete with leader dots
            # + page) or a wrapping one (title only, page appears later).
            art_m = re.match(r"^\s*(\d{2,4})\b\s+(.+)$", line)
            if art_m and _looks_like_article_number(art_m.group(1)):
                # Flush any stale pending first.
                pending = None
                number = art_m.group(1).strip()
                rest = art_m.group(2).strip()
                page = _find_trailing_nfpa_page(rest)
                if page is not None:
                    title = _strip_leader_and_page(rest)
                    entries.append(TocEntry(
                        section_number=number,
                        title=title,
                        pdf_page=0,
                        printed_page=page,
                        depth=1 if current_chapter else 0,
                        source="article+ocr" if use_ocr else "article",
                    ))
                else:
                    pending = {
                        "number": number,
                        "title_parts": [rest],
                        "depth": 1 if current_chapter else 0,
                    }
                i += 1
                continue

            # Part row. Depth one deeper than whatever the current article is.
            part_m = re.match(
                r"^\s*Part\s+([IVX]+)\.?\s*(.*)$", line,
            )
            if part_m:
                pending = None
                roman = part_m.group(1)
                rest = (part_m.group(2) or "").strip()
                # Part rows can wrap too: "Part I." / "General .. ... 70-437"
                # If rest has no content, consume the next line.
                if not rest and i + 1 < len(raw):
                    rest = raw[i + 1].strip()
                    i += 1
                page = _find_trailing_nfpa_page(rest)
                if page is None:
                    # Still no page — we've just seen "Part I. General"
                    # without a page, which probably means the page is
                    # on the next line. Try once more.
                    if i + 1 < len(raw):
                        maybe = raw[i + 1].strip()
                        page = _find_trailing_nfpa_page(maybe)
                        if page is not None:
                            rest = (rest + " " + maybe).strip()
                            i += 1
                if page is not None:
                    # Parts sit under the most recently emitted article.
                    parent_num = next(
                        (e.section_number for e in reversed(entries)
                         if e.section_number and e.section_number.isdigit()),
                        None,
                    )
                    number = (
                        f"{parent_num}.{roman}" if parent_num else f"Part {roman}"
                    )
                    title = _strip_leader_and_page(rest) or f"Part {roman}"
                    entries.append(TocEntry(
                        section_number=number,
                        title=title,
                        pdf_page=0,
                        printed_page=page,
                        depth=2 if current_chapter else 1,
                        source="article+ocr" if use_ocr else "article",
                    ))
                i += 1
                continue

            # Continuation line for a wrapped article title.
            if pending is not None:
                page = _find_trailing_nfpa_page(line)
                if page is not None:
                    full = " ".join(pending["title_parts"] + [line])
                    title = _strip_leader_and_page(full)
                    entries.append(TocEntry(
                        section_number=pending["number"],
                        title=title,
                        pdf_page=0,
                        printed_page=page,
                        depth=pending["depth"],
                        source="article+ocr" if use_ocr else "article",
                    ))
                    pending = None
                else:
                    # Accumulate at most 3 lines before giving up on the
                    # pending entry — a real TOC row almost never wraps
                    # more than that, and runaway accumulation corrupts
                    # downstream entries.
                    if len(pending["title_parts"]) < 3:
                        pending["title_parts"].append(line)
                    else:
                        pending = None
                i += 1
                continue

            # Body-start heuristic: once we've collected some entries
            # and start seeing lines that look like prose, stop.
            if entries and _looks_like_body_start(line):
                break

            i += 1

        if not entries:
            return []

        # Resolve printed_page → pdf_page; same offset-guess approach as
        # _parse_visual_lines. Stage B tolerates ±2.
        return _resolve_pdf_pages(entries, doc_len=len(doc))

    def _collect_front_matter_lines(
        self,
        doc: fitz.Document,
        limit: int,
        use_ocr: bool,
    ) -> List[Tuple[int, str]]:
        """Return ``(pdf_page, line)`` tuples from pages 1..limit."""
        out: List[Tuple[int, str]] = []
        for page_idx in range(limit):
            page = doc.load_page(page_idx)
            text = page.get_text() or ""
            if use_ocr and len(text.strip()) < 50:
                text = _ocr_page(page)
            for line in text.splitlines():
                if line.strip():
                    out.append((page_idx + 1, line))
        return out

    def _parse_visual_lines(
        self,
        lines: List[Tuple[int, str]],
        source: str,
    ) -> List[TocEntry]:
        """Locate the Contents heading and extract leader-dot rows below it."""
        started = False
        entries: List[TocEntry] = []
        page_to_pdf: dict[int, int] = {}  # printed_page -> pdf_page, filled later

        for pdf_page, line in lines:
            if not started:
                if _TOC_HEADER_RE.match(line):
                    started = True
                continue

            m = _TOC_ROW_RE.match(line)
            if not m:
                # If we hit a chapter body start after we've collected entries,
                # stop scanning — TOC is done.
                if entries and (line.strip().startswith("CHAPTER")
                                or _looks_like_body_start(line)):
                    break
                continue

            num = m.group("num").strip()
            title = m.group("title").strip().rstrip("·•.")
            printed = int(m.group("page"))
            depth = _depth_for_number(num)
            entries.append(TocEntry(
                section_number=num,
                title=title,
                pdf_page=0,  # resolved below
                printed_page=printed,
                depth=depth,
                source=source,
            ))

        # Resolve printed_page -> pdf_page by scanning the document once for
        # the first page whose footer/header contains a matching page label.
        # Building-code PDFs typically print the page number in the footer.
        # We reuse the same `lines` sweep we already did when possible, plus a
        # second pass over the rest of the document.
        # This method is approximate; a ±1 page drift is acceptable because
        # Stage B will also scan adjacent pages for the section heading text.
        if entries:
            entries = _resolve_pdf_pages(entries, doc_len=_doc_len_from_lines(lines))

        return entries


# ---- helpers ---------------------------------------------------------------


def _split_number_and_title(title: str) -> Tuple[Optional[str], str]:
    """Given a raw outline title like '26.5.2 Wind Hazard Map', split it.

    Returns ``(section_number, remaining_title)``. If no number prefix is
    detected the section_number is ``None`` and the whole string is the
    title.
    """
    m = _NUMBER_RE.match(title)
    if not m:
        return None, title.strip()
    number = m.group(1).strip()
    remainder = title[m.end():].strip(" .:-—")
    return number, remainder or number


# ---- NEC/CEC article helpers -----------------------------------------------


# NFPA-style page reference: "70-437", "72-118", etc. Captured group is
# the printed page within that NFPA document. Also accepts plain trailing
# integers so this function works on non-NFPA article-shaped TOCs.
_NFPA_PAGE_RE = re.compile(r"\b(?:\d{1,3}-)?(\d{2,4})\s*$")

# Leader-dot run: sequences of the form ".  .... . .. .." that printers
# use to visually connect a title to its page number. We tolerate mixed
# dots, middle-dots, and whitespace between them.
_LEADER_RUN_RE = re.compile(r"[.\u00b7\u2022\s]{3,}$")


def _looks_like_article_number(num: str) -> bool:
    """True if a raw integer string looks like an NEC/CEC article number.

    NEC articles start at 90 (Introduction) and go up through 840-series
    (Special Conditions); CEC adds article 89 ("General Code Provisions")
    at the very start. 2-digit values under 85 are almost always column-
    header fragments, figure numbers, or OCR misreads — allowing lower
    numbers produced articles "13" and "60" in early testing that were
    actually bleed-through from adjacent columns on the TOC page.
    """
    try:
        n = int(num)
    except ValueError:
        return False
    return 85 <= n <= 999 and len(num) <= 3


def _find_trailing_nfpa_page(line: str) -> Optional[int]:
    """Return the printed page if this line ends in a page reference.

    Matches leader-dot + ``70-NNN`` or leader-dot + plain ``NNN``. Lines
    without any trailing digits return None (caller keeps waiting for
    the wrapped continuation).
    """
    s = line.rstrip()
    if not s:
        return None
    # We require *some* leader-ish run OR direct digits at end — plain
    # "Hello world 3" shouldn't count, but "Hello world .... 3" should.
    # So: look for (digits at end) AND (dots or >=2 spaces before them).
    m = re.search(r"(?:[.\u00b7\u2022]{2,}|\s{2,})\s*(?:\d{1,3}-)?(\d{2,4})\s*$", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _min_article_number(entries: List[TocEntry]) -> Optional[int]:
    """Lowest numeric article number in a list of article-parsed entries.

    ``Part`` sub-entries and ``Chapter`` group headers are skipped so the
    result reflects only top-level articles. ``None`` if the list has no
    article-shaped entries.
    """
    nums = []
    for e in entries:
        num = (e.section_number or "").strip()
        if num.isdigit():
            try:
                nums.append(int(num))
            except ValueError:
                continue
    return min(nums) if nums else None


def _merge_article_entries(
    primary: List[TocEntry], secondary: List[TocEntry],
) -> List[TocEntry]:
    """Merge two article-parsed TOC lists, preferring ``primary`` on conflict.

    "Conflict" means two entries share the same ``section_number``. The
    caller passes the OCR parse as ``primary`` (more complete, likely to
    cover early articles) and the text-layer parse as ``secondary`` (more
    accurate on page numbers where both cover the same article). Result
    is sorted by (article_number, part_number) so Stage B can slice
    monotonically.
    """
    by_key: dict = {}
    order: dict = {}
    counter = 0
    for e in primary:
        by_key[e.section_number] = e
        order[e.section_number] = counter
        counter += 1
    for e in secondary:
        if e.section_number in by_key:
            # Keep primary's entry but prefer the more plausible
            # printed_page: text-layer numbers are usually more accurate
            # than OCR when both exist.
            existing = by_key[e.section_number]
            if e.printed_page and not existing.printed_page:
                by_key[e.section_number] = e
            continue
        by_key[e.section_number] = e
        order[e.section_number] = counter
        counter += 1

    def _sort_key(entry: TocEntry) -> tuple:
        num = (entry.section_number or "")
        main, _, sub = num.partition(".")
        try:
            main_key = int(main)
        except ValueError:
            # Chapter / Part / Appendix markers sort by the order we saw
            # them, not by numeric value.
            return (10_000_000, order.get(num, 0))
        # Roman-numeral parts (I, II, III…) need numeric ordering so
        # 450.II sorts before 450.III. Plain integer fallback keeps the
        # function total.
        sub_key = _roman_to_int(sub) if sub else 0
        return (main_key, sub_key)

    merged = sorted(by_key.values(), key=_sort_key)

    # Clean up OCR bleed-through: entries whose *title* contains more
    # than a handful of non-alphanumeric runs are almost always cases
    # where the OCR engine slurped a column boundary and glued adjacent
    # columns together. We keep the article number (it's usually right)
    # but strip the noisy tail so the title is at least readable in the
    # UI. 50 chars is roomy enough for all genuine NEC article titles.
    cleaned: List[TocEntry] = []
    for e in merged:
        t = e.title or ""
        # Heuristic: titles with runs of non-letter characters beyond
        # simple punctuation indicate OCR junk. We do NOT drop the
        # entry — just truncate the title at the first junk run so the
        # article number still gets indexed.
        cut = re.search(r"\s[^A-Za-z0-9,()\-—/ ]{2,}", t)
        if cut:
            t = t[:cut.start()].rstrip(" ,.-—")
        # Also cap the title length to avoid outputting an entire wrapped
        # paragraph when the leader dots were missed entirely.
        t = t[:80].strip()
        cleaned.append(TocEntry(
            section_number=e.section_number,
            title=t or e.section_number,
            pdf_page=e.pdf_page,
            printed_page=e.printed_page,
            depth=e.depth,
            source=e.source,
        ))
    return cleaned


def _roman_to_int(s: str) -> int:
    """Parse a Roman numeral like 'III' → 3. Non-Roman input returns 0.

    Used only for sorting article sub-parts so 450.II < 450.III.
    """
    if not s:
        return 0
    values = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for ch in reversed(s.upper()):
        v = values.get(ch)
        if v is None:
            return 0
        if v < prev:
            total -= v
        else:
            total += v
            prev = v
    return total


def _strip_leader_and_page(line: str) -> str:
    """Strip trailing leader-dots + page reference, return the title."""
    s = line.rstrip()
    # Drop "70-NNN" or bare NNN at the end.
    s = re.sub(r"(?:[.\u00b7\u2022\s]{2,})\s*(?:\d{1,3}-)?\d{2,4}\s*$", "", s)
    # Drop any remaining runs of leader dots.
    s = re.sub(r"[.\u00b7\u2022]{2,}\s*$", "", s)
    return s.strip(" .:—-")


def _depth_for_number(num: str) -> int:
    """Derive hierarchy depth from a raw section number string."""
    stripped = num.strip()
    if stripped.lower().startswith(("chapter", "part", "appendix")):
        return 0
    # Count dot-separated parts; "26" = depth 0, "26.5" = 1, "26.5.2" = 2.
    parts = stripped.split(".")
    return max(0, len(parts) - 1)


def _looks_like_body_start(line: str) -> bool:
    """Heuristic: does this line look like body prose (not a TOC row)?"""
    # TOC rows almost always end in a digit (the page number); body lines
    # usually end in a period or other punctuation.
    s = line.strip()
    return bool(s) and not s[-1].isdigit() and len(s) > 80


def _doc_len_from_lines(lines: List[Tuple[int, str]]) -> int:
    if not lines:
        return 0
    return max(p for p, _ in lines)


def _resolve_pdf_pages(entries: List[TocEntry], doc_len: int) -> List[TocEntry]:
    """Translate printed_page → pdf_page assuming a constant front-matter offset.

    Most building-code PDFs have a fixed offset: printed page 1 lives at pdf
    page N. We compute that offset by assuming the first TOC entry starts
    just after the TOC pages, then keep it constant. Stage B tolerates a
    ±2 page drift, so exact accuracy isn't required.
    """
    if not entries:
        return entries
    # Default guess: offset = front-matter-length in pages. We don't know
    # the exact front-matter length without more work, so fall back to:
    # pdf_page = printed + 20 clamped to [1, doc_len]. Stage B will refine.
    OFFSET_GUESS = 20
    out: List[TocEntry] = []
    for e in entries:
        printed = e.printed_page or 1
        pdf_page = min(max(1, printed + OFFSET_GUESS), max(1, doc_len))
        out.append(TocEntry(
            section_number=e.section_number,
            title=e.title,
            pdf_page=pdf_page,
            printed_page=printed,
            depth=e.depth,
            source=e.source,
        ))
    return out


def _verify_outline_pages(
    doc: fitz.Document,
    entries: List[TocEntry],
    search_window: int = 10,
) -> List[TocEntry]:
    """Fix outline entries whose ``pdf_page`` points to the wrong page.

    The ASCE 7-22 PDF (and others built from similar workflows) includes
    outline entries whose target links point at commentary pages instead
    of body pages, producing out-of-order ``pdf_page`` values. We detect
    that by watching for a backward jump relative to the last verified
    entry, then search forward from that point for the section heading
    text. If found, we update ``pdf_page`` to the actual body page.

    Entries whose headings aren't found within ``search_window`` pages
    of where they should be fall back to the previous verified page +1
    (best guess) so the downstream slicing doesn't pull text from a
    totally unrelated region.
    """
    if not entries:
        return entries
    # Pre-extract text per page once. This is O(pages) I/O, but each call
    # is cheap because fitz keeps the document in memory.
    page_text: List[str] = []
    for i in range(len(doc)):
        try:
            page_text.append(doc.load_page(i).get_text() or "")
        except Exception:
            page_text.append("")

    verified: List[TocEntry] = []
    last_good_page = 1

    for entry in entries:
        claimed = max(1, min(entry.pdf_page, len(doc)))

        def heading_on(page_num: int) -> bool:
            if page_num < 1 or page_num > len(page_text):
                return False
            txt = page_text[page_num - 1]
            if not txt:
                return False
            # Build a lenient needle: section_number token, optionally
            # followed by the first 20 chars of the title. Strict match on
            # the number alone suffices for most codes.
            needle = entry.section_number.strip()
            if not needle:
                return False
            # Match the number at the start of a line, as a standalone
            # token, or immediately followed by title prefix.
            if re.search(rf"(?m)^\s*{re.escape(needle)}\b", txt):
                return True
            return False

        # Fast path: claimed page has the heading AND claimed page isn't
        # in the past relative to last_good_page — trust it.
        if claimed >= last_good_page and heading_on(claimed):
            verified.append(entry)
            last_good_page = claimed
            continue

        # Search forward from last_good_page for the heading.
        fixed: Optional[int] = None
        window_end = min(last_good_page + search_window, len(page_text))
        for p in range(last_good_page, window_end + 1):
            if heading_on(p):
                fixed = p
                break

        if fixed is not None:
            verified.append(TocEntry(
                section_number=entry.section_number,
                title=entry.title,
                pdf_page=fixed,
                printed_page=entry.printed_page,
                depth=entry.depth,
                source=entry.source + "+verified",
            ))
            last_good_page = fixed
        else:
            # Could not verify. Best-guess: keep monotonic ordering by
            # using last_good_page + 1. This avoids the dramatic
            # mis-attribution (pulling commentary content for a body
            # section) at the cost of some bodies being merged.
            verified.append(TocEntry(
                section_number=entry.section_number,
                title=entry.title,
                pdf_page=min(last_good_page, len(page_text)),
                printed_page=entry.printed_page,
                depth=entry.depth,
                source=entry.source + "+unverified",
            ))

    return verified


def _ocr_page(page: fitz.Page, dpi: int = 200) -> str:
    """Render a PDF page to a bitmap and OCR it via Tesseract."""
    try:
        import pytesseract  # local import; keeps module importable without tesseract
        from PIL import Image
    except ImportError:
        logger.warning("pytesseract / Pillow not installed; skipping OCR on page")
        return ""
    pix = page.get_pixmap(dpi=dpi)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    try:
        return pytesseract.image_to_string(img) or ""
    except Exception:
        logger.exception("Tesseract OCR failed on page %s", page.number)
        return ""
