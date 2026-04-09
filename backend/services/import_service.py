"""Import orchestration service for building code PDFs.

Handles PDF parsing, validation, database insertion, embedding generation,
and cross-reference extraction.
"""

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional
import asyncpg
import httpx

from parsers.pdf_parser import PDFParser
from parsers.reference_extractor import ReferenceExtractor
from validators.content_validator import ContentValidator

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """Result of PDF import operation."""
    imported: int = 0
    quarantined: int = 0
    references_found: int = 0
    errors: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


async def import_pdf(
    file_path: str,
    code_book_id: int,
    db_pool: asyncpg.Pool,
    embedding_url: str,
    source_id: Optional[int] = None,
) -> ImportResult:
    """Import PDF and insert code sections into database.

    Args:
        file_path: Path to PDF file
        code_book_id: Code book ID from database
        db_pool: Database connection pool
        embedding_url: URL of embedding service (e.g., http://embedding-service:8000)
        source_id: Import source ID (optional)

    Returns:
        ImportResult with counts and errors
    """
    result = ImportResult()

    try:
        # Step 1: Parse PDF
        logger.info(f"Parsing PDF: {file_path}")
        parser = PDFParser()
        parsed_sections = parser.parse(file_path)
        logger.info(f"Extracted {len(parsed_sections)} sections")

        # Get or create import source if not provided
        async with db_pool.acquire() as conn:
            if not source_id:
                source_id = await conn.fetchval(
                    '''INSERT INTO import_sources
                       (source_name, source_type, code_book_id, status)
                       VALUES ($1, $2, $3, $4)
                       RETURNING id''',
                    file_path,
                    'pdf_parse',
                    code_book_id,
                    'crawling',
                )

            # Create import log entry
            import_log_id = await conn.fetchval(
                '''INSERT INTO import_logs
                   (source_id, status)
                   VALUES ($1, $2)
                   RETURNING id''',
                source_id,
                'processing',
            )

        # Step 2: Validate and insert sections
        content_validator = ContentValidator(db_pool)
        reference_extractor = ReferenceExtractor()

        for section in parsed_sections:
            try:
                # Validate content
                validation = await content_validator.validate(section.full_text, source_id)
                if not validation.passed:
                    result.quarantined += 1
                    logger.warning(
                        f"Section {section.section_number} failed validation: {validation.errors}"
                    )
                    continue

                # Insert section into database
                section_hash = hashlib.sha256(section.full_text.encode()).hexdigest()

                async with db_pool.acquire() as conn:
                    section_id = await conn.fetchval(
                        '''INSERT INTO code_sections
                           (code_book_id, chapter, section_number, section_title,
                            full_text, section_type, depth, path, has_ca_amendment,
                            amendment_agency, source_hash)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                           RETURNING id''',
                        code_book_id,
                        section.chapter,
                        section.section_number,
                        section.section_title,
                        section.full_text,
                        section.section_type,
                        section.depth,
                        section.path,
                        section.has_ca_amendment,
                        section.amendment_agency,
                        section_hash,
                    )

                    # Generate embedding
                    embedding = await generate_embedding(
                        section.full_text,
                        embedding_url,
                    )

                    if embedding:
                        # Update embedding column
                        await conn.execute(
                            '''UPDATE code_sections
                               SET embedding = $1
                               WHERE id = $2''',
                            embedding,
                            section_id,
                        )

                    # Create version record
                    await conn.execute(
                        '''INSERT INTO code_section_versions
                           (code_section_id, version_number, content, changed_by, change_reason)
                           VALUES ($1, $2, $3, $4, $5)''',
                        section_id,
                        1,
                        section.full_text,
                        'import_service',
                        f'Initial import from {file_path}',
                    )

                    # Extract and insert cross-references
                    references = reference_extractor.extract(
                        section.section_number,
                        section.full_text,
                    )

                    for ref in references:
                        # Try to find target section
                        target_id = None
                        if ref.target_section_number:
                            target_id = await conn.fetchval(
                                '''SELECT id FROM code_sections
                                   WHERE section_number = $1 AND code_book_id = $2''',
                                ref.target_section_number,
                                code_book_id,
                            )

                        # Try to find external standard
                        external_id = None
                        if ref.external_standard_id:
                            external_id = await conn.fetchval(
                                '''SELECT id FROM external_standards
                                   WHERE standard_id = $1''',
                                ref.external_standard_id,
                            )

                        # Insert reference
                        await conn.execute(
                            '''INSERT INTO code_references
                               (source_section_id, target_section_id, external_standard_id,
                                reference_type, reference_text)
                               VALUES ($1, $2, $3, $4, $5)''',
                            section_id,
                            target_id,
                            external_id,
                            ref.reference_type,
                            ref.reference_text,
                        )

                        result.references_found += 1

                result.imported += 1
                logger.info(f"Imported section {section.section_number}")

            except Exception as e:
                logger.error(f"Error importing section {section.section_number}: {e}", exc_info=True)
                result.errors.append(f"Section {section.section_number}: {e}")

        # Step 3: Update import log and source
        async with db_pool.acquire() as conn:
            await conn.execute(
                '''UPDATE import_logs
                   SET status = $1, records_processed = $2, records_imported = $3,
                       records_failed = $4, completed_at = NOW()
                   WHERE id = $5''',
                'completed',
                len(parsed_sections),
                result.imported,
                result.quarantined,
                import_log_id,
            )

            await conn.execute(
                '''UPDATE import_sources
                   SET status = $1, sections_imported = $2, last_crawled = NOW()
                   WHERE id = $3''',
                'completed',
                result.imported,
                source_id,
            )

        logger.info(
            f"Import complete: {result.imported} imported, "
            f"{result.quarantined} quarantined, {result.references_found} references"
        )

    except Exception as e:
        logger.error(f"Error importing PDF {file_path}: {e}", exc_info=True)
        result.errors.append(f"Import failed: {e}")

        # Update import log with error
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE import_logs
                       SET status = $1, error_message = $2, completed_at = NOW()
                       WHERE id = $3''',
                    'error',
                    str(e),
                    import_log_id,
                )
                if source_id:
                    await conn.execute(
                        '''UPDATE import_sources
                           SET status = $1, error_message = $2
                           WHERE id = $3''',
                        'error',
                        str(e),
                        source_id,
                    )
        except Exception as db_error:
            logger.error(f"Error updating import status: {db_error}")

    return result


async def generate_embedding(text: str, embedding_url: str) -> Optional[str]:
    """Generate embedding via HTTP call to embedding service.

    Args:
        text: Text to embed
        embedding_url: Base URL of embedding service (e.g., http://embedding-service:8000)

    Returns:
        Embedding vector as string, or None if service unavailable
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{embedding_url}/embed",
                json={"text": text},
            )
            response.raise_for_status()

            data = response.json()
            embedding = data.get('embedding')

            if embedding:
                # Convert list to PostgreSQL vector format
                # Format: "[0.1,0.2,0.3,...]"
                vector_str = '[' + ','.join(str(x) for x in embedding) + ']'
                return vector_str

            return None

    except Exception as e:
        logger.error(f"Error generating embedding: {e}")
        return None
