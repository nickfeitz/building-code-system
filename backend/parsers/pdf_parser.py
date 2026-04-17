"""PDF parser for California Title 24 building codes using PyMuPDF (fitz).

Extracts sections with proper hierarchy detection, amendment annotations,
and preservation of table structures.
"""

import bisect
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set, Tuple
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


@dataclass
class ParsedSection:
    """Parsed section from building code PDF."""
    section_number: str
    section_title: str
    full_text: str
    chapter: Optional[str] = None
    depth: int = 0  # 0=chapter, 1=section, 2=subsection, 3=sub-subsection
    path: Optional[str] = None
    has_ca_amendment: bool = False
    amendment_agency: Optional[str] = None
    section_type: str = "section"  # 'section', 'table', 'figure', 'appendix', 'definition'
    # 1-based page in the source PDF where this section starts. Sections
    # that span pages are attributed to their starting page only.
    page_number: Optional[int] = None


class PDFParser:
    """Parse California Title 24 building code PDFs with PyMuPDF."""

    # Patterns for IBC/CBC section numbering: 123, 123.1, 123.1.1, etc.
    SECTION_NUMBER_PATTERN = re.compile(r'\b(\d{3,4}\.\d+(?:\.\d+)*)\b')
    CHAPTER_PATTERN = re.compile(r'^\s*(?:CHAPTER|Chapter)\s+(\d+)', re.MULTILINE)

    # CA amendment agency markers
    CA_AGENCIES = {
        '[HCD]': 'HCD',
        '[SFM]': 'SFM',
        '[DSA]': 'DSA',
        '[OSHPD]': 'OSHPD',
        '[BSC-CG]': 'BSC-CG',
    }

    # Building code term indicators
    CODE_TERMS = {
        'shall', 'shall not', 'shall be', 'shall comply',
        'in accordance with', 'subject to', 'permitted',
        'prohibited', 'regulated', 'approved'
    }

    def __init__(self):
        """Initialize PDF parser."""
        self.current_chapter = None
        self.sections = []

    def parse(self, pdf_path: str) -> List[ParsedSection]:
        """Parse PDF and extract sections.

        Args:
            pdf_path: Path to PDF file

        Returns:
            List of ParsedSection objects
        """
        self.sections = []
        self.current_chapter = None

        try:
            with fitz.open(pdf_path) as doc:
                logger.info(f"Parsing PDF: {pdf_path} ({len(doc)} pages)")
                full_text, page_line_starts = self._extract_full_text(doc)
                self._page_count = len(doc)
                self._extract_sections(full_text, doc, page_line_starts)

        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_path}: {e}", exc_info=True)
            raise

        return self.sections

    def _extract_full_text(self, doc: fitz.Document) -> Tuple[str, List[int]]:
        """Extract full text from PDF, skipping headers/footers.

        Returns:
            ``(joined_text, page_line_starts)`` where ``joined_text`` is the
            concatenation of each page's filtered lines, joined with ``'\\n'``,
            and ``page_line_starts[i]`` is the 0-based line index at which
            page ``i+1`` begins in ``joined_text.split('\\n')``.

            Callers split ``joined_text`` on ``'\\n'``; a section detected
            at line index ``L`` lives on page
            ``bisect_right(page_line_starts, L)`` (1-based).
        """
        per_page_lines: List[List[str]] = []

        for _page_num, page in enumerate(doc):
            text = page.get_text()

            # Remove common headers/footers (simplified approach)
            content_lines: List[str] = []
            for line in text.split('\n'):
                if (line.strip() and
                    not re.match(r'^\s*\d+\s*$', line) and
                    not re.match(r'^\s*[A-Z\s]+\s*\|\s*\d{4}', line)):
                    content_lines.append(line)
            per_page_lines.append(content_lines)

        # Build the joined text the same way the consumer will split it.
        # We join each page with '\n' within the page, and '\n' between
        # pages, which means the page boundary sits exactly at the next
        # line index after the last line of the previous page.
        joined_per_page = ['\n'.join(lines) for lines in per_page_lines]
        joined = '\n'.join(joined_per_page)

        # Derive offsets from the final split — this is the source of truth
        # and keeps the attribution robust to any future edits above.
        all_lines = joined.split('\n')
        page_line_starts: List[int] = []
        cursor = 0
        for i, page_block in enumerate(joined_per_page):
            page_line_starts.append(cursor)
            if i < len(joined_per_page) - 1:
                # Each joined_per_page[i] contributes exactly (count('\n')+1)
                # lines to the final split. The '\n' that `'\n'.join`
                # inserts between pages is also a split boundary, so the
                # next page's first line is at cursor + contributed.
                contributed = page_block.count('\n') + 1
                cursor += contributed
        # Sanity: the final cursor should be <= len(all_lines)
        assert cursor <= len(all_lines), (
            f"page_line_starts accounting drifted: cursor={cursor} vs {len(all_lines)} lines"
        )
        return joined, page_line_starts

    def _extract_sections(
        self,
        full_text: str,
        doc: fitz.Document,
        page_line_starts: Optional[List[int]] = None,
    ) -> None:
        """Extract sections from full text with hierarchy.

        Args:
            full_text: Full text content from PDF
            doc: PyMuPDF document for font analysis
            page_line_starts: 0-based line indices where each page begins
                in ``full_text.split('\\n')``. When provided, each section
                is stamped with the 1-based page number it started on.
        """
        lines = full_text.split('\n')
        current_section = None
        current_text_buffer = []

        def page_for(line_idx: int) -> Optional[int]:
            if not page_line_starts:
                return None
            # bisect_right returns the 1-based page index directly:
            # page_line_starts[p-1] <= line_idx < page_line_starts[p]
            p = bisect.bisect_right(page_line_starts, line_idx)
            return p if p >= 1 else 1

        for line_idx, line in enumerate(lines):
            line_stripped = line.strip()

            if not line_stripped:
                if current_text_buffer:
                    current_text_buffer.append('')
                continue

            # Check for chapter boundary
            chapter_match = self.CHAPTER_PATTERN.match(line_stripped)
            if chapter_match:
                if current_section:
                    self._save_section(current_section, current_text_buffer)
                self.current_chapter = chapter_match.group(1)
                current_section = None
                current_text_buffer = []
                continue

            # Check for section number at start of line
            section_match = self.SECTION_NUMBER_PATTERN.match(line_stripped)
            if section_match and self._is_section_header(line_stripped):
                if current_section:
                    self._save_section(current_section, current_text_buffer)

                section_num = section_match.group(1)
                section_title = self._extract_title(line_stripped, section_num)
                current_section = {
                    'number': section_num,
                    'title': section_title,
                    'has_amendment': False,
                    'amendment_agency': None,
                    'page_number': page_for(line_idx),
                }
                current_text_buffer = [line]
                continue

            # Accumulate text for current section
            if current_section:
                current_text_buffer.append(line)

                # Check for CA amendments
                if not current_section['has_amendment']:
                    for agency_marker in self.CA_AGENCIES:
                        if agency_marker in line:
                            current_section['has_amendment'] = True
                            current_section['amendment_agency'] = self.CA_AGENCIES[agency_marker]
                            break

        # Save last section
        if current_section:
            self._save_section(current_section, current_text_buffer)

    def _is_section_header(self, line: str) -> bool:
        """Detect if line is a section header.

        Args:
            line: Line to check

        Returns:
            True if line appears to be section header
        """
        # Section header typically has number, optional title, and code-like structure
        words = line.split()
        if not words:
            return False

        # First word should be section number
        if not re.match(r'\d{3,4}\.\d+', words[0]):
            return False

        # If followed by text that looks like a title, it's likely a header
        if len(words) > 1:
            # Check for code-like keywords or capitalization
            remaining = ' '.join(words[1:])
            if any(keyword in remaining.lower() for keyword in ['general', 'scope', 'definitions', 'application']):
                return True

        return True

    def _extract_title(self, line: str, section_num: str) -> str:
        """Extract section title from header line.

        Args:
            line: Header line
            section_num: Section number

        Returns:
            Section title
        """
        # Remove section number and extract remaining text
        title = line.replace(section_num, '', 1).strip()
        # Remove trailing punctuation
        title = re.sub(r'[.]*$', '', title)
        return title

    def _save_section(self, section_info: Dict, text_buffer: List[str]) -> None:
        """Save extracted section.

        Args:
            section_info: Section metadata dict
            text_buffer: Text lines for section
        """
        section_num = section_info['number']
        section_title = section_info['title']
        full_text = '\n'.join(text_buffer).strip()

        # Determine depth from section number
        depth = section_num.count('.') - 1

        # Build path from section hierarchy
        path = self._build_path(section_num)

        # Detect section type
        section_type = self._detect_section_type(full_text)

        page_number = section_info.get('page_number')
        if page_number is not None and hasattr(self, '_page_count'):
            # Defensive clamp: bisect should never exceed range, but a
            # wrong page_number would silently mislead the review UI.
            if page_number < 1 or page_number > self._page_count:
                logger.warning(
                    "page_number %s out of range [1, %s] for section %s — dropping",
                    page_number, self._page_count, section_num,
                )
                page_number = None

        parsed = ParsedSection(
            section_number=section_num,
            section_title=section_title or f"Section {section_num}",
            full_text=full_text,
            chapter=self.current_chapter,
            depth=depth,
            path=path,
            has_ca_amendment=section_info['has_amendment'],
            amendment_agency=section_info['amendment_agency'],
            section_type=section_type,
            page_number=page_number,
        )

        self.sections.append(parsed)

    def _build_path(self, section_num: str) -> str:
        """Build materialized path for section.

        Example: "7.706.706.1" for section 706.1 in chapter 7

        Args:
            section_num: Section number

        Returns:
            Path string
        """
        parts = [self.current_chapter or '0'] if self.current_chapter else ['0']
        parts.extend(section_num.split('.'))
        return '.'.join(parts)

    def _detect_section_type(self, text: str) -> str:
        """Detect section type from content.

        Args:
            text: Section text

        Returns:
            Section type: 'section', 'table', 'figure', 'appendix', 'definition'
        """
        text_lower = text.lower()

        if 'table' in text_lower and len(text) < 2000:
            return 'table'
        if 'figure' in text_lower:
            return 'figure'
        if 'appendix' in text_lower:
            return 'appendix'
        if 'definitions' in text_lower or 'defined as' in text_lower:
            return 'definition'

        return 'section'
