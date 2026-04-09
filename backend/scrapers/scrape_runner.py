"""Orchestration service for ICC web scraper.

Handles scraping, validation, database insertion, embedding generation,
and cross-reference extraction for web-scraped content.
"""

import hashlib
import logging
from dataclasses import dataclass
from typing import Optional
import asyncpg
import httpx

from parsers.reference_extractor import ReferenceExtractor
from validators.content_validator import ContentValidator
from scrapers.icc_scraper import ICCScraper

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """Result of web scrape import operation."""
    imported: int = 0
    quarantined: int = 0
    references_found: int = 0
    errors: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


async def run_icc_import(
    code_url: str,
    code_book_id: int,
    db_pool: asyncpg.Pool,
    embedding_url: str,
    source_id: int,
) -> ImportResult:
    """Run ICC web scraper and import sections into database.

    Args:
        code_url: URL to ICC code (e.g., https://codes.iccsafe.org/content/IBC2021P7)
        code_book_id: Code book ID from database
        db_pool: Database connection pool
        embedding_url: URL of embedding service
        source_id: Import source ID

    Returns:
        ImportResult with counts and errors
    """
    result = ImportResult()

    try:
        # Step 1: Create/update import log entry
        async with db_pool.acquire() as conn:
            import_log_id = await conn.fetchval(
                '''INSERT INTO import_logs
                   (source_id, status)
                   VALUES ($1, $2)
                   RETURNING id''',
                source_id,
                'processing',
            )

        # Step 2: Scrape code sections
        logger.info(f"Starting ICC scrape: {code_url}")
        scraper = ICCScraper()
        parsed_sections = await scraper.scrape_code(code_url, code_book_id)
        logger.info(f"Scraped {len(parsed_sections)} sections")

        # Step 3: Validate and insert sections
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
                            full_text, section_type, depth, path, source_hash)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                           RETURNING id''',
                        code_book_id,
                        section.chapter,
                        section.section_number,
                        section.section_title,
                        section.full_text,
                        section.section_type,
                        section.depth,
                        section.path,
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
                        'icc_scraper',
                        f'Initial import from {code_url}',
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
                result.quarantined += 1

        # Step 4: Update import log and source
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
            f"ICC import complete: {result.imported} imported, "
            f"{result.quarantined} quarantined, {result.references_found} references"
        )

    except Exception as e:
        logger.error(f"Error running ICC import: {e}", exc_info=True)
        result.errors.append(f"Import failed: {e}")

        # Update import log with error
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE import_logs
                       SET status = $1, error_message = $2, completed_at = NOW()
                       WHERE source_id = $3''',
                    'error',
                    str(e),
                    source_id,
                )
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
