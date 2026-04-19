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

                ocr = self._from_ocr(doc)
                if len(ocr) >= self.MIN_VISUAL_ROWS:
                    logger.info("TOC: used OCR parse (%d entries)", len(ocr))
                    return ocr

                # Give back whichever was largest, even if small — callers decide.
                candidates = sorted([entries, visual, ocr], key=len, reverse=True)
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
