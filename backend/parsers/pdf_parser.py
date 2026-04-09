"""PDF parser for California Title 24 building codes using PyMuPDF (fitz).

Extracts sections with proper hierarchy detection, amendment annotations,
and preservation of table structures.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Set
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
                full_text = self._extract_full_text(doc)
                self._extract_sections(full_text, doc)

        except Exception as e:
            logger.error(f"Error parsing PDF {pdf_path}: {e}", exc_info=True)
            raise

        return self.sections

    def _extract_full_text(self, doc: fitz.Document) -> str:
        """Extract full text from PDF, skipping headers/footers.

        Args:
            doc: PyMuPDF document

        Returns:
            Full text content
        """
        full_text = []

        for page_num, page in enumerate(doc):
            text = page.get_text()

            # Remove common headers/footers (simplified approach)
            lines = text.split('\n')
            content_lines = []

            for line in lines:
                # Skip page numbers, common header patterns
                if (line.strip() and
                    not re.match(r'^\s*\d+\s*$', line) and
                    not re.match(r'^\s*[A-Z\s]+\s*\|\s*\d{4}', line)):
                    content_lines.append(line)

            full_text.append('\n'.join(content_lines))

        return '\n'.join(full_text)

    def _extract_sections(self, full_text: str, doc: fitz.Document) -> None:
        """Extract sections from full text with hierarchy.

        Args:
            full_text: Full text content from PDF
            doc: PyMuPDF document for font analysis
        """
        lines = full_text.split('\n')
        current_section = None
        current_text_buffer = []

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
