"""Document extractor — Stages B + C of the TOC-driven ingestion pipeline.

Given the list of ``TocEntry`` returned by ``TocExtractor``, this module
extracts the body text for each section in the document and emits
``ParsedSection`` objects compatible with the existing import pipeline
(``backend/services/import_service.py``).

Design choices:

1. **Body-text extraction is driven entirely by the TOC.** We never scan body
   text looking for "section headers" — that's what the old parser did, and
   it mis-identified table-cell decimals as sections. Instead, each TOC
   entry defines the start of a section, and the *next* TOC entry defines
   its end.

2. **PyMuPDF text layer first, OCR second.** Most modern engineering-code
   PDFs (ASCE 7-22 included) ship with a high-quality text layer. We only
   invoke Tesseract OCR on pages where the text layer is nearly empty (<50
   chars), which is the classic signature of a scanned page.

3. **Running header/footer stripping** is frequency-based. Lines that appear
   on > 25% of pages with identical text are treated as headers/footers and
   removed before emission. This catches things like
   "Minimum Design Loads and Associated Criteria for Buildings and Other Structures"
   which the current parser happily includes in every section's full_text.

4. **Skip-list** removes non-essential front/back matter: Preface, Index,
   Notation, Commentary, References sections. These are identified by
   matching the TOC entry title, not by scanning pages.

5. **Fallback mode**: if the caller passes ``use_docling=True`` we route
   through ``docling.DocumentConverter`` instead of fitz. Docling is slower
   but handles multi-column layouts and embedded tables more robustly. For
   ASCE 7-22 the fitz path is sufficient; Docling is kept as a flag for
   future PDFs that need it.

The returned ``ParsedSection`` dataclass matches the shape that
``import_service.py`` expects today, so we don't have to touch the
orchestrator's insert loop.
"""

from __future__ import annotations

import io
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import fitz  # PyMuPDF

from parsers.text_normalizer import normalize as _normalize_text
from parsers.toc_extractor import TocEntry, TocExtractor, _ocr_page

logger = logging.getLogger(__name__)


# Titles of TOC entries that are not code sections and should not be indexed
# as one. These still get a stub row (so users can see they exist), but with
# section_type set appropriately so the Browser panel can filter them out.
_NON_CODE_TITLES = re.compile(
    r"""^(
        Preface | Foreword | Acknowledgments | Acknowledgements |
        Notation | Commentary\b.* | Index | References? |
        About\s+the\s+Authors? | Contributors? | Dedication |
        Copyright | Front\s+Matter | End\s+Matter
    )$""",
    re.IGNORECASE | re.VERBOSE,
)


@dataclass
class ParsedSection:
    """Section record compatible with ``import_service.import_pdf``.

    ``full_text``     — normalized prose (soft wraps collapsed, lists kept).
                         This is what search, embeddings, and the UI read.
    ``full_text_raw`` — untouched text as extracted from the PDF, for audit.

    Fields mirror those used by the existing insert loop. Added vs. the old
    parser: ``canonical_hash`` (normalized full-text hash used for dedup).
    """
    section_number: str
    section_title: str
    full_text: str
    full_text_raw: Optional[str] = None
    chapter: Optional[str] = None
    depth: int = 0
    path: Optional[str] = None
    has_ca_amendment: bool = False
    amendment_agency: Optional[str] = None
    section_type: str = "section"
    page_number: Optional[int] = None
    canonical_hash: Optional[str] = None


@dataclass
class ExtractedDocument:
    sections: List[ParsedSection] = field(default_factory=list)
    ocr_flagged_pages: List[int] = field(default_factory=list)
    toc_entries: List[TocEntry] = field(default_factory=list)
    page_count: int = 0


class DocumentExtractor:
    """Extract structured body text for every section in a PDF."""

    # Pages with less than this many extractable characters are candidates
    # for Tesseract OCR.
    MIN_TEXT_CHARS_PER_PAGE = 50

    # Lines appearing on > this fraction of pages are treated as running
    # header/footer and stripped.
    HEADER_FOOTER_THRESHOLD = 0.25

    # A single section's body may not exceed this many characters. If the
    # TOC has a missing entry so we over-capture, we don't want one "section"
    # to accidentally contain half the book.
    MAX_SECTION_CHARS = 200_000

    def __init__(self, toc: Optional[TocExtractor] = None):
        self.toc = toc or TocExtractor()

    def extract(self, pdf_path: str) -> ExtractedDocument:
        result = ExtractedDocument()

        toc_entries = self.toc.extract(pdf_path)
        result.toc_entries = toc_entries
        if not toc_entries:
            logger.error("document_extractor: no TOC entries; aborting")
            return result

        with fitz.open(pdf_path) as doc:
            result.page_count = len(doc)

            # Build per-page text once. Strip running headers/footers.
            pages = self._extract_all_pages(doc, result)

            # Iterate over TOC entries; slice body text between adjacent entries.
            for i, entry in enumerate(toc_entries):
                end_page = (
                    toc_entries[i + 1].pdf_page - 1
                    if i + 1 < len(toc_entries)
                    else len(doc)
                )
                # TOC rows may arrive out of order or with bad page numbers
                # (visual/OCR modes). Clamp defensively.
                start_page = max(1, min(entry.pdf_page, len(doc)))
                end_page = max(start_page, min(end_page, len(doc)))

                body = self._slice_body(
                    pages=pages,
                    start_page=start_page,
                    end_page=end_page,
                    entry=entry,
                    next_entry=toc_entries[i + 1] if i + 1 < len(toc_entries) else None,
                )

                if not body.strip():
                    continue

                section_type = (
                    "front_matter" if _NON_CODE_TITLES.match(entry.title or "")
                    else self._infer_section_type(entry)
                )

                raw_body = body[: self.MAX_SECTION_CHARS]
                clean_body = _normalize_text(
                    raw_body,
                    section_number=entry.section_number,
                    section_title=entry.title,
                )
                result.sections.append(ParsedSection(
                    section_number=entry.section_number,
                    section_title=entry.title or f"Section {entry.section_number}",
                    full_text=clean_body,
                    full_text_raw=raw_body,
                    chapter=_chapter_for(entry),
                    depth=entry.depth,
                    path=None,  # built later by import_service if needed
                    section_type=section_type,
                    page_number=start_page,
                ))

        logger.info(
            "document_extractor: extracted %d sections, %d OCR-flagged pages",
            len(result.sections), len(result.ocr_flagged_pages),
        )
        return result

    # --- helpers ----------------------------------------------------------

    def _extract_all_pages(
        self,
        doc: fitz.Document,
        result: ExtractedDocument,
    ) -> List[str]:
        """Extract raw text per page, with running headers/footers stripped.

        Pages whose text layer is sparse trigger Tesseract OCR and are
        recorded in ``result.ocr_flagged_pages`` for downstream quarantine.
        """
        raw: List[List[str]] = []
        for page_idx in range(len(doc)):
            page = doc.load_page(page_idx)
            text = page.get_text() or ""
            if len(text.strip()) < self.MIN_TEXT_CHARS_PER_PAGE:
                ocr_text = _ocr_page(page)
                if len(ocr_text.strip()) >= self.MIN_TEXT_CHARS_PER_PAGE:
                    text = ocr_text
                    result.ocr_flagged_pages.append(page_idx + 1)
            raw.append([ln for ln in text.splitlines() if ln.strip()])

        # Build a frequency table of top-3 and bottom-3 lines across pages.
        # A line that appears on > HEADER_FOOTER_THRESHOLD of pages is a
        # running header/footer and gets scrubbed.
        top_counts: Counter = Counter()
        bot_counts: Counter = Counter()
        for lines in raw:
            for ln in lines[:3]:
                top_counts[ln.strip()] += 1
            for ln in lines[-3:]:
                bot_counts[ln.strip()] += 1

        n = max(1, len(raw))
        thresh = max(5, int(n * self.HEADER_FOOTER_THRESHOLD))
        noisy = {ln for ln, c in top_counts.items() if c >= thresh}
        noisy |= {ln for ln, c in bot_counts.items() if c >= thresh}
        # Also drop pure-digit lines (page numbers) as a catch-all.
        def keep(ln: str) -> bool:
            s = ln.strip()
            if not s:
                return False
            if s in noisy:
                return False
            if re.fullmatch(r"\d{1,4}", s):
                return False
            return True

        return ["\n".join(ln for ln in lines if keep(ln)) for lines in raw]

    def _slice_body(
        self,
        pages: List[str],
        start_page: int,
        end_page: int,
        entry: TocEntry,
        next_entry: Optional[TocEntry],
    ) -> str:
        """Return the text belonging to ``entry`` only.

        We concatenate pages in the [start, end] range, then:
          * skip everything before the section heading text (entry.title or
            entry.section_number) on the start page;
          * truncate at the next section heading within the captured range.
        """
        chunks: List[str] = []
        for p in range(start_page, end_page + 1):
            idx = p - 1
            if 0 <= idx < len(pages):
                chunks.append(pages[idx])
        blob = "\n".join(chunks)

        blob = _skip_to_heading(blob, entry)
        if next_entry is not None:
            blob = _truncate_at_heading(blob, next_entry)

        # Clean up leading/trailing whitespace, collapse runs of blank lines.
        blob = re.sub(r"\n{3,}", "\n\n", blob).strip()
        return blob

    def _infer_section_type(self, entry: TocEntry) -> str:
        t = (entry.title or "").lower()
        if t.startswith("table "):
            return "table"
        if t.startswith("figure "):
            return "figure"
        if t.startswith("appendix "):
            return "appendix"
        if "definitions" in t:
            return "definition"
        if entry.depth == 0:
            return "chapter"
        return "section"


# ---- module-local helpers --------------------------------------------------


def _chapter_for(entry: TocEntry) -> Optional[str]:
    """Extract a chapter identifier for an entry, if derivable."""
    num = (entry.section_number or "").strip()
    if not num:
        return None
    if num.lower().startswith(("chapter ", "appendix ", "part ")):
        return num
    first = num.split(".")[0]
    return first or None


def _skip_to_heading(blob: str, entry: TocEntry) -> str:
    """Drop text before this section's heading, if we can find it.

    We search for either the section number OR the first ~40 chars of the
    title as a literal on a line of their own. If neither appears, we return
    the blob unchanged — better to keep too much text than to drop body
    content by mistake.
    """
    needles = []
    if entry.section_number:
        needles.append(re.escape(entry.section_number.strip()))
    if entry.title:
        head = entry.title.strip()[:40]
        if head:
            needles.append(re.escape(head))
    if not needles:
        return blob
    pat = re.compile(
        r"^\s*(?:" + "|".join(needles) + r")\b",
        re.MULTILINE,
    )
    m = pat.search(blob)
    if m is None:
        return blob
    return blob[m.start():]


def _truncate_at_heading(blob: str, next_entry: TocEntry) -> str:
    """Cut the blob at the next section's heading, if we can find it."""
    needles = []
    if next_entry.section_number:
        needles.append(re.escape(next_entry.section_number.strip()))
    if next_entry.title:
        head = next_entry.title.strip()[:40]
        if head:
            needles.append(re.escape(head))
    if not needles:
        return blob
    pat = re.compile(
        r"^\s*(?:" + "|".join(needles) + r")\b",
        re.MULTILINE,
    )
    # Skip any occurrence at offset 0 (we may already be sitting on the next
    # heading because of how the slice was aligned).
    for m in pat.finditer(blob):
        if m.start() > 0:
            return blob[: m.start()]
    return blob
