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
    import_log_id: Optional[int] = None,
) -> ImportResult:
    """Import PDF and insert code sections into database.

    Args:
        file_path: Path to PDF file
        code_book_id: Code book ID from database
        db_pool: Database connection pool
        embedding_url: URL of embedding service
        source_id: Import source ID (optional — will be created if omitted)
        import_log_id: Existing import_logs.id to mutate. When the HTTP
            upload endpoint prepares the log row at request time, it passes
            that id here so the worker updates the *same* row instead of
            creating a second orphan. If None, we create one.

    Returns:
        ImportResult with counts and errors
    """
    result = ImportResult()

    async def _set_phase(phase: str, **extra):
        """Helper: update phase + arbitrary counters + bump updated_at."""
        if import_log_id is None:
            return
        parts = ["phase = $2", "updated_at = NOW()"]
        params: list = [import_log_id, phase]
        for i, (k, v) in enumerate(extra.items(), start=3):
            parts.append(f"{k} = ${i}")
            params.append(v)
        sql = f"UPDATE import_logs SET {', '.join(parts)} WHERE id = $1"
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(sql, *params)
        except Exception as e:
            logger.warning(f"Failed to update import_log {import_log_id}: {e}")

    try:
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

            if import_log_id is None:
                import_log_id = await conn.fetchval(
                    '''INSERT INTO import_logs
                       (source_id, status, code_book_id, phase)
                       VALUES ($1, $2, $3, $4)
                       RETURNING id''',
                    source_id, 'processing', code_book_id, 'queued',
                )

        # Phase 1: PDF parse
        await _set_phase('parsing', status='processing')
        logger.info(f"Parsing PDF: {file_path}")
        parser = PDFParser()
        parsed_sections = parser.parse(file_path)
        logger.info(f"Extracted {len(parsed_sections)} sections")
        await _set_phase('indexing',
                         records_total=len(parsed_sections),
                         records_processed=0)

        # Step 2: Validate and insert sections
        content_validator = ContentValidator(db_pool)
        reference_extractor = ReferenceExtractor()

        total = len(parsed_sections)
        PROGRESS_EVERY = max(1, total // 50)  # ~50 progress ticks per job

        for idx, section in enumerate(parsed_sections):
            # Periodic heartbeat so the UI can show progress.
            if idx % PROGRESS_EVERY == 0 and import_log_id is not None:
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            '''UPDATE import_logs
                               SET records_processed = $1,
                                   records_imported = $2,
                                   records_failed = $3,
                                   updated_at = NOW()
                               WHERE id = $4''',
                            idx, result.imported, result.quarantined,
                            import_log_id,
                        )
                except Exception:
                    pass  # progress update failures must not break the import
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
                            amendment_agency, source_hash, page_number)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
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
                        section.page_number,
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
                   SET status = $1, phase = $2,
                       records_processed = $3, records_imported = $4,
                       records_failed = $5, completed_at = NOW(),
                       updated_at = NOW()
                   WHERE id = $6''',
                'completed',
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
                       SET status = $1, phase = $2, error_message = $3,
                           completed_at = NOW(), updated_at = NOW()
                       WHERE id = $4''',
                    'error',
                    'failed',
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
