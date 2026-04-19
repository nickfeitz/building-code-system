"""4-layer content validation pipeline for building code sections.

Validates extracted content through format, garbage detection, code structure,
and integrity checks before importing into database.
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of content validation."""
    passed: bool
    layer: Optional[int] = None  # 1-4 if failed, None if passed
    errors: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)
    content_hash: Optional[str] = None


class FormatValidator:
    """Layer 1: Format validation (encoding, empty content, binary detection)."""

    def validate(self, content: str) -> Optional[ValidationResult]:
        """Validate content format.

        Args:
            content: Text content to validate

        Returns:
            None if valid, ValidationResult if failed
        """
        errors = []

        # Check encoding - ensure valid UTF-8
        try:
            content.encode('utf-8').decode('utf-8')
        except (UnicodeDecodeError, UnicodeEncodeError) as e:
            errors.append(f"Encoding error: {e}")
            return ValidationResult(
                passed=False,
                layer=1,
                errors=errors,
            )

        # Check for empty or very short content
        if not content or len(content.strip()) < 20:
            errors.append(f"Content too short ({len(content)} chars)")
            return ValidationResult(
                passed=False,
                layer=1,
                errors=errors,
            )

        # Check for binary-like content
        if self._is_binary(content):
            errors.append("Content appears to be binary")
            return ValidationResult(
                passed=False,
                layer=1,
                errors=errors,
            )

        # Check for mojibake patterns (garbled text)
        if self._detect_mojibake(content):
            errors.append("Content contains mojibake (garbled text)")
            return ValidationResult(
                passed=False,
                layer=1,
                errors=errors,
            )

        return None

    def _is_binary(self, content: str) -> bool:
        """Detect binary-like content.

        Args:
            content: Content to check

        Returns:
            True if content appears binary
        """
        # Check for null bytes or excessive control characters
        null_count = content.count('\x00')
        if null_count > 0:
            return True

        # Check control character ratio
        control_chars = sum(1 for c in content if ord(c) < 32 and c not in '\n\r\t')
        if control_chars / max(len(content), 1) > 0.05:
            return True

        return False

    def _detect_mojibake(self, content: str) -> bool:
        """Detect garbled text patterns.

        Args:
            content: Content to check

        Returns:
            True if mojibake detected
        """
        # Check for excessive unusual Unicode characters
        unusual = sum(1 for c in content if ord(c) > 0x2000 and not self._is_printable_unicode(c))
        if unusual / max(len(content), 1) > 0.1:
            return True

        return False

    def _is_printable_unicode(self, char: str) -> bool:
        """Check if character is printable Unicode.

        Args:
            char: Character to check

        Returns:
            True if printable
        """
        # Whitelist common Unicode ranges
        code = ord(char)
        ranges = [
            (0x0020, 0x007E),  # ASCII
            (0x00A0, 0x00FF),  # Latin-1
            (0x0100, 0x017F),  # Latin Extended-A
            (0x0180, 0x024F),  # Latin Extended-B
            (0x2000, 0x206F),  # General Punctuation
        ]
        return any(start <= code <= end for start, end in ranges)


class GarbageDetector:
    """Layer 2: Detect error pages, CAPTCHAs, paywalls, etc."""

    # Error page indicators
    ERROR_PATTERNS = [
        r'(?:404|403|500|502)',
        r'(?:page not found|access denied|404 not found)',
        r'(?:internal server error|gateway timeout|500 error)',
    ]

    # CAPTCHA indicators
    CAPTCHA_PATTERNS = [
        r'(?:captcha|recaptcha)',
        r'(?:verify you are human)',
        r'(?:robot|bot)\s+(?:check|verification)',
    ]

    # Paywall indicators
    PAYWALL_PATTERNS = [
        r'(?:paywall|premium|membership)',
        r'(?:limited access|restricted content|subscribe\s+to\s+(?:access|view|receive))',
        r'(?:upgrade|sign up|sign in|purchase\s+(?:required|to\s+(?:view|access)))',
    ]

    # ICC-specific garbage signals
    ICC_GARBAGE = [
        r'(?:order|purchase|cart)',
        r'(?:shipping|delivery|address)',
        r'(?:account|login|password)',
    ]

    def validate(
        self,
        content: str,
        source_type: str = "web",
    ) -> Optional[ValidationResult]:
        """Detect garbage content.

        The error-page / CAPTCHA / paywall / ICC-cart patterns only make
        sense for web-scraped content. Applying them to PDF-extracted
        building-code text produces false positives — ASCE 7-22 sections
        legitimately contain words like "order", "address", "purchase",
        and numbers like "404" (e.g. "ASTM 404"), "500" (e.g. "concrete
        strength 500 psi") that would trip these rules.

        Args:
            content: Content to validate
            source_type: "pdf" to skip web-only heuristics, "web" otherwise

        Returns:
            None if valid, ValidationResult if garbage detected
        """
        errors = []
        content_lower = content.lower()
        is_web = source_type != "pdf"

        # Check for error pages (web only — digits like 404/500 appear in
        # ASTM standard numbers and engineering values inside PDFs)
        if is_web:
            for pattern in self.ERROR_PATTERNS:
                if re.search(pattern, content_lower):
                    errors.append(f"Error page pattern detected: {pattern}")
                    return ValidationResult(
                        passed=False,
                        layer=2,
                        errors=errors,
                    )

        # Check for CAPTCHAs (web only)
        if is_web:
            for pattern in self.CAPTCHA_PATTERNS:
                if re.search(pattern, content_lower):
                    errors.append("CAPTCHA detected")
                    return ValidationResult(
                        passed=False,
                        layer=2,
                        errors=errors,
                    )

        # Check for paywalls (web only — "premium" / "subscribe" appear in
        # building-code boilerplate too)
        if is_web:
            for pattern in self.PAYWALL_PATTERNS:
                if re.search(pattern, content_lower):
                    errors.append("Paywall detected")
                    return ValidationResult(
                        passed=False,
                        layer=2,
                        errors=errors,
                    )

        # Check for ICC-specific garbage (web only — every PDF has "order"
        # somewhere in legitimate prose)
        if is_web:
            for pattern in self.ICC_GARBAGE:
                if re.search(pattern, content_lower):
                    errors.append(f"ICC garbage detected: {pattern}")
                    return ValidationResult(
                        passed=False,
                        layer=2,
                        errors=errors,
                    )

        # Check HTML tag ratio
        html_tags = len(re.findall(r'<[^>]+>', content))
        tag_ratio = html_tags / max(len(content.split()), 1)
        if tag_ratio > 0.3:
            errors.append(f"Excessive HTML tags ({tag_ratio:.2%})")
            return ValidationResult(
                passed=False,
                layer=2,
                errors=errors,
            )

        # Check for repetitive content
        if self._is_repetitive(content):
            errors.append("Excessively repetitive content")
            return ValidationResult(
                passed=False,
                layer=2,
                errors=errors,
            )

        return None

    def _is_repetitive(self, content: str) -> bool:
        """Detect repetitive content patterns.

        Args:
            content: Content to check

        Returns:
            True if excessively repetitive
        """
        lines = content.split('\n')
        if len(lines) < 5:
            return False

        # Check for repeated lines
        line_counts = {}
        for line in lines:
            line_clean = line.strip()
            if line_clean:
                line_counts[line_clean] = line_counts.get(line_clean, 0) + 1

        # If any single line appears >20% of the time, it's repetitive
        max_count = max(line_counts.values()) if line_counts else 0
        return max_count / max(len(lines), 1) > 0.2


class CodeStructureValidator:
    """Layer 3: Validate building code structure indicators."""

    # Building code keywords and patterns
    CODE_INDICATORS = {
        'section_numbers': r'\d{3,4}\.\d+(?:\.\d+)*',
        'shall_compliance': r'\b(?:shall|shall not|shall be|shall comply)\b',
        'compliance_language': r'\b(?:in accordance with|subject to|permitted|prohibited|regulated|approved)\b',
        'standard_refs': r'\b(?:ASTM|NFPA|ACI|ASCE|AISC|IAPMO|ICC|ASHRAE)\s+[A-Z]?\d+',
        'code_terms': r'\b(?:section|chapter|appendix|table|figure|exception)\b',
        'definition_markers': r'\b(?:defined as|means|definition|shall be construed)\b',
    }

    MIN_SCORE = 3  # Must match at least 3/7 categories (or 2/7 for short content)

    def validate(self, content: str) -> Optional[ValidationResult]:
        """Validate code structure indicators.

        Args:
            content: Content to validate

        Returns:
            None if valid, ValidationResult if failed
        """
        errors = []
        metadata = {}

        # Count matching categories
        matched = 0
        for category, pattern in self.CODE_INDICATORS.items():
            matches = len(re.findall(pattern, content, re.IGNORECASE))
            metadata[category] = matches
            if matches > 0:
                matched += 1

        metadata['matched_categories'] = matched

        # Dynamic threshold: shorter content (< 200 chars) only needs 2 categories
        min_score = 2 if len(content) < 200 else self.MIN_SCORE

        if matched < min_score:
            errors.append(
                f"Insufficient code structure indicators "
                f"({matched}/{len(self.CODE_INDICATORS)} categories, "
                f"minimum {min_score} required for {len(content)} char content)"
            )
            return ValidationResult(
                passed=False,
                layer=3,
                errors=errors,
                metadata=metadata,
            )

        return None


class IntegrityValidator:
    """Layer 4: Detect duplicates, truncation, format issues."""

    def __init__(self):
        """Initialize integrity validator."""
        pass

    async def validate(
        self,
        content: str,
        db_pool: asyncpg.Pool,
        source_id: int,
    ) -> Optional[ValidationResult]:
        """Validate content integrity.

        Args:
            content: Content to validate
            db_pool: Database connection pool
            source_id: Import source ID

        Returns:
            None if valid, ValidationResult if failed
        """
        errors = []
        metadata = {}

        # Generate content hash
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        metadata['content_hash'] = content_hash

        # Check for duplicate content against LIVE sections only.
        # Superseded rows are being replaced by this very ingest, so
        # matching against them would false-flag every single section
        # on a reindex (where every new row is by design a replacement
        # for an existing superseded row with the same body text).
        try:
            async with db_pool.acquire() as conn:
                existing = await conn.fetchval(
                    '''SELECT COUNT(*) FROM code_sections
                        WHERE source_hash = $1
                          AND superseded_date IS NULL''',
                    content_hash
                )
                if existing:
                    errors.append(f"Duplicate content (hash {content_hash})")
                    return ValidationResult(
                        passed=False,
                        layer=4,
                        errors=errors,
                        metadata=metadata,
                    )
        except Exception as e:
            logger.error(f"Database error in duplicate check: {e}")
            errors.append(f"Database error: {e}")
            return ValidationResult(
                passed=False,
                layer=4,
                errors=errors,
                metadata=metadata,
            )

        # Check for truncation
        if self._detect_truncation(content):
            errors.append("Content appears truncated")
            return ValidationResult(
                passed=False,
                layer=4,
                errors=errors,
                metadata=metadata,
            )

        # Validate section number format (if present)
        section_numbers = re.findall(r'\d{3,4}\.\d+(?:\.\d+)*', content)
        if section_numbers:
            for section_num in section_numbers:
                if not self._validate_section_number(section_num):
                    errors.append(f"Invalid section number format: {section_num}")
                    return ValidationResult(
                        passed=False,
                        layer=4,
                        errors=errors,
                        metadata=metadata,
                    )

        return None

    def _detect_truncation(self, content: str) -> bool:
        """Detect if content appears truncated.

        Args:
            content: Content to check

        Returns:
            True if truncation detected
        """
        # Check for common truncation patterns
        if content.rstrip().endswith('...'):
            return True

        # Check if content ends abruptly (mid-word)
        lines = content.split('\n')
        if lines:
            last_line = lines[-1].strip()
            # If last line is very long and doesn't end with punctuation, might be truncated
            if len(last_line) > 100 and not last_line[-1] in '.!?):':
                return True

        return False

    def _validate_section_number(self, section_num: str) -> bool:
        """Validate section number format.

        Args:
            section_num: Section number to validate

        Returns:
            True if valid
        """
        parts = section_num.split('.')

        # First part should be 3-4 digits
        if not (3 <= len(parts[0]) <= 4 and parts[0].isdigit()):
            return False

        # Subsequent parts should be 1-3 digits
        for part in parts[1:]:
            if not (1 <= len(part) <= 3 and part.isdigit()):
                return False

        return True


class ContentValidator:
    """Orchestrator for 4-layer content validation pipeline."""

    def __init__(self, db_pool: asyncpg.Pool):
        """Initialize content validator.

        Args:
            db_pool: Database connection pool
        """
        self.db_pool = db_pool
        self.format_validator = FormatValidator()
        self.garbage_detector = GarbageDetector()
        self.code_structure_validator = CodeStructureValidator()
        self.integrity_validator = IntegrityValidator()

    async def validate(
        self,
        content: str,
        source_id: int,
        source_type: str = "web",
    ) -> ValidationResult:
        """Run 4-layer validation pipeline.

        Args:
            content: Content to validate
            source_id: Import source ID
            source_type: "pdf" or "web". Controls which Layer-2 patterns
                are applied — web-scrape garbage heuristics fire false
                positives on legitimate PDF body text.

        Returns:
            ValidationResult with passed/failed status
        """
        # Layer 1: Format validation
        result = self.format_validator.validate(content)
        if result:
            logger.warning(f"Content failed Layer 1 validation: {result.errors}")
            await self._quarantine(result, content, source_id)
            return result

        # Layer 2: Garbage detection
        result = self.garbage_detector.validate(content, source_type=source_type)
        if result:
            logger.warning(f"Content failed Layer 2 validation: {result.errors}")
            await self._quarantine(result, content, source_id)
            return result

        # Layer 3: Code structure validation
        result = self.code_structure_validator.validate(content)
        if result:
            logger.warning(f"Content failed Layer 3 validation: {result.errors}")
            # Note: Layer 3 doesn't hard-fail, we continue to Layer 4

        # Layer 4: Integrity validation
        result = await self.integrity_validator.validate(content, self.db_pool, source_id)
        if result:
            logger.warning(f"Content failed Layer 4 validation: {result.errors}")
            await self._quarantine(result, content, source_id)
            return result

        # All layers passed
        return ValidationResult(passed=True)

    async def _quarantine(
        self,
        result: ValidationResult,
        content: str,
        source_id: int,
    ) -> None:
        """Quarantine failed content.

        Args:
            result: Validation result
            content: Content that failed
            source_id: Import source ID
        """
        import json
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO content_quarantine
                       (source_id, validation_layer, error_message, raw_content, metadata)
                       VALUES ($1, $2, $3, $4, $5)''',
                    source_id,
                    result.layer,
                    ' | '.join(result.errors),
                    content[:5000],  # Limit stored content
                    # metadata is a JSONB column — json.dumps, not str().
                    # str() produces Python repr ("{'key': 'val'}") which
                    # postgres rejects as invalid JSON.
                    json.dumps(result.metadata) if result.metadata else '{}',
                )
        except Exception as e:
            logger.error(f"Error quarantining content: {e}")
