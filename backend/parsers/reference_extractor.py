"""Extract cross-references from building code text.

Handles 6 reference types: mandatory, informational, exception, table/figure,
external standards, and cross-part references.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExtractedReference:
    """Extracted reference from code text."""
    source_section_number: str
    target_section_number: Optional[str] = None
    external_standard_id: Optional[str] = None
    reference_type: str = "unknown"  # mandatory, informational, exception, table, figure, external_standard, cross_part
    reference_text: str = ""


class ReferenceExtractor:
    """Extract cross-references from building code text."""

    # Pattern for section references: "Section 706", "Section 706.1.1", "Sections 706.1 through 706.5"
    SECTION_PATTERN = re.compile(
        r'(?:Sections?|§)\s+(\d{3,4}(?:\.\d+)*(?:\s+through\s+\d{3,4}(?:\.\d+)?)?)',
        re.IGNORECASE
    )

    # Mandatory reference patterns
    MANDATORY_PATTERNS = [
        re.compile(r'shall comply with (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
        re.compile(r'in accordance with (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
        re.compile(r'(?:shall )?comply with the (?:provisions|requirements) of (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
    ]

    # Informational reference patterns
    INFORMATIONAL_PATTERNS = [
        re.compile(r'see (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
        re.compile(r'see also (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
        re.compile(r'refer to (?:Section|§)\s+(\d{3,4}(?:\.\d+)*)', re.IGNORECASE),
    ]

    # Exception patterns (with optional numbering)
    EXCEPTION_PATTERN = re.compile(
        r'Exception(?:\s+\d+)?(?:\s*\(.*?\))?:\s*(.+?)(?=(?:Exception|$))',
        re.IGNORECASE | re.DOTALL
    )

    # Table and Figure patterns
    TABLE_PATTERN = re.compile(r'(?:Table|see Table)\s+(\d+(?:\.\d+)?)', re.IGNORECASE)
    FIGURE_PATTERN = re.compile(r'(?:Figure|see Figure|as shown in Figure)\s+(\d+(?:\.\d+)?)', re.IGNORECASE)

    # External standard patterns: ASTM E119-18, NFPA 72-2019, etc.
    EXTERNAL_STANDARD_PATTERN = re.compile(
        r'((?:ASTM|NFPA|ACI|ASCE|AISC|IAPMO|ICC|ASHRAE)\s+[A-Z]?\d+[-.]?\d*(?:-\d+)?)',
        re.IGNORECASE
    )

    # Cross-Part references: "Part 2, Section 706" or "Title 24, Part 2"
    CROSS_PART_PATTERN = re.compile(
        r'(?:Title\s+\d+,\s+)?Part\s+(\d+)(?:[,\s]+(?:Section|§)\s+(\d{3,4}(?:\.\d+)*))?',
        re.IGNORECASE
    )

    def __init__(self):
        """Initialize reference extractor."""
        pass

    def extract(self, source_section_number: str, text: str) -> List[ExtractedReference]:
        """Extract all references from section text.

        Args:
            source_section_number: Section number making the reference
            text: Section text to analyze

        Returns:
            List of ExtractedReference objects
        """
        references = []

        # Extract mandatory references
        for pattern in self.MANDATORY_PATTERNS:
            for match in pattern.finditer(text):
                target = match.group(1)
                # Handle range references: "706.1 through 706.5"
                expanded = self._expand_range(target)
                for section_num in expanded:
                    ref_text = match.group(0)
                    references.append(ExtractedReference(
                        source_section_number=source_section_number,
                        target_section_number=section_num,
                        reference_type='mandatory',
                        reference_text=ref_text,
                    ))

        # Extract informational references
        for pattern in self.INFORMATIONAL_PATTERNS:
            for match in pattern.finditer(text):
                target = match.group(1)
                expanded = self._expand_range(target)
                for section_num in expanded:
                    ref_text = match.group(0)
                    references.append(ExtractedReference(
                        source_section_number=source_section_number,
                        target_section_number=section_num,
                        reference_type='informational',
                        reference_text=ref_text,
                    ))

        # Extract exception references
        for match in self.EXCEPTION_PATTERN.finditer(text):
            exception_text = match.group(1)
            ref_text = f"Exception: {exception_text[:100]}"
            references.append(ExtractedReference(
                source_section_number=source_section_number,
                reference_type='exception',
                reference_text=ref_text,
            ))

        # Extract table references
        for match in self.TABLE_PATTERN.finditer(text):
            table_num = match.group(1)
            references.append(ExtractedReference(
                source_section_number=source_section_number,
                reference_type='table',
                reference_text=f"Table {table_num}",
            ))

        # Extract figure references
        for match in self.FIGURE_PATTERN.finditer(text):
            figure_num = match.group(1)
            references.append(ExtractedReference(
                source_section_number=source_section_number,
                reference_type='figure',
                reference_text=f"Figure {figure_num}",
            ))

        # Extract external standards
        for match in self.EXTERNAL_STANDARD_PATTERN.finditer(text):
            standard_id = match.group(1).upper()
            references.append(ExtractedReference(
                source_section_number=source_section_number,
                external_standard_id=standard_id,
                reference_type='external_standard',
                reference_text=standard_id,
            ))

        # Extract cross-part references
        for match in self.CROSS_PART_PATTERN.finditer(text):
            part = match.group(1)
            section = match.group(2) if match.lastindex >= 2 else None
            if section:
                references.append(ExtractedReference(
                    source_section_number=source_section_number,
                    target_section_number=section,
                    reference_type='cross_part',
                    reference_text=f"Part {part}, Section {section}",
                ))

        return references

    def _expand_range(self, section_ref: str) -> List[str]:
        """Expand range references like "706.1 through 706.5".

        Args:
            section_ref: Section reference, possibly with range

        Returns:
            List of individual section numbers
        """
        if 'through' not in section_ref.lower():
            return [section_ref.strip()]

        # Parse range: "706.1 through 706.5"
        parts = re.split(r'\s+through\s+', section_ref, flags=re.IGNORECASE)
        if len(parts) != 2:
            return [section_ref.strip()]

        start_str = parts[0].strip()
        end_str = parts[1].strip()

        try:
            # Extract numeric components
            start_parts = [int(x) for x in start_str.split('.')]
            end_parts = [int(x) for x in end_str.split('.')]

            # Assume expanding the last component
            if len(start_parts) != len(end_parts):
                # Different depths - can't expand reliably
                return [section_ref.strip()]

            expanded = []
            for i in range(start_parts[-1], end_parts[-1] + 1):
                section = start_parts[:-1] + [i]
                expanded.append('.'.join(str(x) for x in section))

            return expanded

        except (ValueError, IndexError):
            # If parsing fails, return original
            return [section_ref.strip()]
