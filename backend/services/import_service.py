"""Import orchestration service for building code PDFs.

Handles PDF parsing, validation, database insertion, embedding generation,
and cross-reference extraction.
"""

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Optional
import asyncpg
import httpx

from parsers.document_extractor import DocumentExtractor
from parsers.reference_extractor import ReferenceExtractor
from parsers.text_normalizer import has_glyph_artifacts
from validators.content_validator import ContentValidator


def _normalize_for_dedup(text: str) -> str:
    """Whitespace-collapsed lowercase body text used for canonical-section dedup.

    Two sections from different codes that carry the same boilerplate
    (e.g. identical scope paragraphs) will hash to the same value and get
    linked via canonical_section_id instead of being inserted twice.
    """
    return re.sub(r"\s+", " ", (text or "").lower()).strip()

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
    source_pdf_id: Optional[int] = None,
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
        source_pdf_id: ``code_book_pdfs.id`` this parse came from. Stored
            on every inserted section so the review UI can link back and
            so future uploads know which rows they're replacing.

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

        # Phase 1: PDF parse (TOC-driven extraction). Runs on a worker
        # thread via asyncio.to_thread so the event loop stays responsive
        # during multi-minute OCR-heavy PDFs; a thread-safe progress
        # callback schedules import_logs updates back on this loop so the
        # UI bar moves while extraction runs instead of freezing on the
        # "parsing" phase label.
        await _set_phase('parsing', status='processing',
                         records_total=0, records_processed=0)
        logger.info(f"Parsing PDF: {file_path}")

        loop = asyncio.get_running_loop()

        async def _record_extraction_progress(done: int, total: int) -> None:
            if import_log_id is None:
                return
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE import_logs
                           SET records_total = $1,
                               records_processed = $2,
                               phase = 'parsing',
                               updated_at = NOW()
                         WHERE id = $3''',
                        total, done, import_log_id,
                    )
            except Exception:
                # UI-only update; never fail the ingest over it.
                logger.exception("parsing progress update failed")

        def _sync_progress_cb(done: int, total: int) -> None:
            asyncio.run_coroutine_threadsafe(
                _record_extraction_progress(done, total),
                loop,
            )

        extractor = DocumentExtractor()
        extracted = await asyncio.to_thread(
            extractor.extract, file_path, _sync_progress_cb,
        )
        parsed_sections = extracted.sections
        logger.info(
            f"Extracted {len(parsed_sections)} sections from "
            f"{len(extracted.toc_entries)} TOC entries; "
            f"{len(extracted.ocr_flagged_pages)} OCR-flagged pages"
        )

        # Push OCR-flagged pages into quarantine so the review UI surfaces
        # them for human eyes.
        if extracted.ocr_flagged_pages:
            import json as _json
            async with db_pool.acquire() as conn:
                for page_num in extracted.ocr_flagged_pages:
                    await conn.execute(
                        '''INSERT INTO content_quarantine
                           (source_id, validation_layer, error_message,
                            raw_content, metadata)
                           VALUES ($1, $2, $3, $4, $5)''',
                        source_id,
                        1,  # layer 1 = format/OCR
                        'Page text layer too sparse; OCR fallback invoked',
                        f'page {page_num} of {file_path}',
                        _json.dumps({
                            'reason': 'ocr_needed',
                            'pdf_page': page_num,
                            'source_pdf_id': source_pdf_id,
                        }),
                    )

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
                # Validate content. source_type="pdf" disables the
                # web-scrape Layer-2 heuristics that produce false
                # positives on legitimate building-code prose.
                validation = await content_validator.validate(
                    section.full_text, source_id, source_type="pdf",
                )
                if not validation.passed:
                    result.quarantined += 1
                    logger.warning(
                        f"Section {section.section_number} failed validation: {validation.errors}"
                    )
                    continue

                # Hash body text two ways:
                #   source_hash      — exact hash of the body as extracted
                #                      (used to detect "same PDF re-ingested")
                #   normalized_hash  — whitespace-collapsed lowercase
                #                      (used to link canonical cross-code
                #                       duplicates via canonical_section_id)
                section_hash = hashlib.sha256(section.full_text.encode()).hexdigest()
                normalized_hash = hashlib.sha256(
                    _normalize_for_dedup(section.full_text).encode()
                ).hexdigest()

                async with db_pool.acquire() as conn:
                    # If another section in *any* code has the same
                    # normalized body, link to it as the canonical version
                    # so we're not storing the same paragraph twice across
                    # the corpus. Same code_book re-imports don't match
                    # themselves because we just cleared this book's rows
                    # before reindex.
                    canonical_id = await conn.fetchval(
                        '''SELECT id FROM code_sections
                           WHERE normalized_hash = $1
                             AND code_book_id <> $2
                           LIMIT 1''',
                        normalized_hash,
                        code_book_id,
                    )

                    section_id = await conn.fetchval(
                        '''INSERT INTO code_sections
                           (code_book_id, chapter, section_number, section_title,
                            full_text, full_text_raw, section_type, depth, path,
                            has_ca_amendment, amendment_agency, source_hash,
                            page_number, source_pdf_id, normalized_hash,
                            canonical_section_id)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                                   $12, $13, $14, $15, $16)
                           RETURNING id''',
                        code_book_id,
                        section.chapter,
                        section.section_number,
                        section.section_title,
                        section.full_text,
                        # Preserve the untouched PDF extraction next to
                        # the normalized version for auditability and
                        # future reflow-logic regressions.
                        getattr(section, "full_text_raw", None),
                        section.section_type,
                        section.depth,
                        section.path,
                        section.has_ca_amendment,
                        section.amendment_agency,
                        section_hash,
                        section.page_number,
                        source_pdf_id,
                        normalized_hash,
                        canonical_id,
                    )

                    # Glyph-suspect audit: after normalization the body
                    # should be free of PDF custom-font artifacts. If any
                    # slipped through, flag this section for human review
                    # without blocking the insert — the reader can still
                    # see what we extracted.
                    if has_glyph_artifacts(section.full_text):
                        try:
                            import json as _json
                            await conn.execute(
                                '''INSERT INTO content_quarantine
                                   (source_id, validation_layer, error_message,
                                    raw_content, metadata)
                                   VALUES ($1, $2, $3, $4, $5)''',
                                source_id,
                                2,  # layer 2 = garbage / encoding
                                'Glyph-encoding artifacts remain after normalization',
                                section.full_text[:5000],
                                _json.dumps({
                                    'reason': 'glyph_encoding_suspect',
                                    'section_id': section_id,
                                    'section_number': section.section_number,
                                }),
                            )
                        except Exception as _e:
                            logger.warning(
                                "glyph-suspect quarantine failed for %s: %s",
                                section.section_number, _e,
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
