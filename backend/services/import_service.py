"""Import orchestration service for building code PDFs.

Handles PDF parsing, validation, database insertion, embedding generation,
and cross-reference extraction.
"""

import asyncio
import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
import asyncpg
import httpx


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

from parsers.document_extractor import DocumentExtractor
from parsers.reference_extractor import ReferenceExtractor
from parsers.text_normalizer import has_glyph_artifacts
from services.identity_check import check_identity
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
    ollama_url: Optional[str] = None,
    ollama_model: Optional[str] = None,
    ollama_num_ctx: int = 8192,
    skip_identity_check: bool = False,
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
        ollama_url: Base URL for the Ollama server used by the pre-flight
            identity check (``services/identity_check.py``). When None,
            the identity check is skipped entirely — primarily so tests
            and one-off scripts don't need a running LLM.
        ollama_model: Model name to ask for the identity-check JSON verdict.
        ollama_num_ctx: KV cache size forwarded to Ollama. Cover-page prompts
            are small, so the default 8 k is plenty.
        skip_identity_check: Operator escape hatch. Useful when an operator
            has manually verified that the PDF matches, or when re-running
            a failed extraction against a known-good pdf_id via /retry.

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

        # Phase 0 — identity check. Verify the PDF actually corresponds
        # to the selected code book before spending minutes parsing it
        # into the wrong place. See services/identity_check.py for the
        # cascade (heuristic → LLM → skip on infra failure).
        if not skip_identity_check and ollama_url and ollama_model:
            await _set_phase(
                'verifying_identity',
                stage_detail='Reading cover + outline to verify this PDF matches the selected book…',
            )
            book_row = None
            try:
                async with db_pool.acquire() as conn:
                    book_row = await conn.fetchrow(
                        '''SELECT cb.code_name, cb.abbreviation, cb.part_number,
                                  cb.base_code_year, cc.name AS cycle_name
                             FROM code_books cb
                             LEFT JOIN code_cycles cc ON cc.id = cb.cycle_id
                            WHERE cb.id = $1''',
                        code_book_id,
                    )
            except Exception as e:
                logger.warning("identity_check: failed to load selected book: %s", e)

            if book_row is not None:
                try:
                    id_result = await check_identity(
                        pdf_path=file_path,
                        selected_book=dict(book_row),
                        ollama_url=ollama_url,
                        ollama_model=ollama_model,
                        ollama_num_ctx=ollama_num_ctx,
                    )
                except Exception as e:
                    # Defensive: identity_check already traps its own
                    # errors, but if something escapes we log and skip
                    # rather than failing the import.
                    logger.warning("identity_check: unexpected error: %s", e)
                    id_result = None

                if id_result is not None and id_result.rejected:
                    msg = (
                        f"Identity check rejected this upload. {id_result.reason} "
                        f"The PDF was stored as code_book_pdfs.id={source_pdf_id} "
                        f"and can be retargeted/retried after the operator confirms "
                        f"the correct code book."
                    )
                    logger.error("import aborted — identity mismatch: %s", msg)
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            '''UPDATE import_logs
                               SET status = 'error', phase = 'rejected_identity_mismatch',
                                   records_total = 0, records_imported = 0,
                                   records_failed = 0,
                                   error_message = $1,
                                   stage_detail = 'Rejected: PDF does not match selected book.',
                                   identity_title = $2,
                                   identity_confidence = $3,
                                   identity_notes = $4,
                                   completed_at = NOW(), updated_at = NOW()
                             WHERE id = $5''',
                            msg, id_result.extracted_title,
                            id_result.confidence, id_result.to_notes_json(),
                            import_log_id,
                        )
                        if source_id:
                            await conn.execute(
                                '''UPDATE import_sources
                                   SET status = 'error', error_message = $1
                                   WHERE id = $2''',
                                'Identity mismatch at pre-flight check.',
                                source_id,
                            )
                    result.errors.append(msg)
                    return result

                # Accepted / skipped — persist what we learned so operators
                # can audit without re-running the check.
                if id_result is not None:
                    try:
                        async with db_pool.acquire() as conn:
                            await conn.execute(
                                '''UPDATE import_logs
                                   SET identity_title = $1,
                                       identity_confidence = $2,
                                       identity_notes = $3,
                                       updated_at = NOW()
                                 WHERE id = $4''',
                                id_result.extracted_title,
                                id_result.confidence,
                                id_result.to_notes_json(),
                                import_log_id,
                            )
                    except Exception as e:
                        logger.warning("identity_check: note persist failed: %s", e)

        # Phase 1: PDF parse (TOC-driven extraction). Runs on a worker
        # thread via asyncio.to_thread so the event loop stays responsive
        # during multi-minute OCR-heavy PDFs; a thread-safe progress
        # callback schedules import_logs updates back on this loop so the
        # UI bar moves while extraction runs instead of freezing on the
        # "parsing" phase label.
        await _set_phase(
            'parsing',
            status='processing',
            records_total=0, records_processed=0,
            current_page=0, total_pages=0, ocr_pages_count=0,
            stage_detail='Opening PDF and scanning pages…',
            started_parsing_at=_now_utc(),
        )
        logger.info(f"Parsing PDF: {file_path}")

        loop = asyncio.get_running_loop()

        # Throttle: the extractor fires once per page, but one SQL UPDATE
        # per page is wasteful on 1000-page books. We coalesce to ~2 Hz
        # plus always flush on OCR events (those are the ones users most
        # want to see animate).
        last_push_ts: dict = {"t": 0.0}

        async def _record_extraction_progress(
            done: int, total: int, ocr_count: int, did_ocr: bool,
        ) -> None:
            if import_log_id is None:
                return
            detail = (
                f"OCR'ing page {done} of {total}"
                if did_ocr
                else f"Extracting text from page {done} of {total}"
            )
            try:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        '''UPDATE import_logs
                           SET current_page = $1,
                               total_pages = $2,
                               ocr_pages_count = $3,
                               stage_detail = $4,
                               phase = 'parsing',
                               updated_at = NOW()
                         WHERE id = $5''',
                        done, total, ocr_count, detail, import_log_id,
                    )
            except Exception:
                # UI-only update; never fail the ingest over it.
                logger.exception("parsing progress update failed")

        def _sync_progress_cb(
            done: int, total: int, ocr_count: int, did_ocr: bool,
        ) -> None:
            import time as _time
            now = _time.monotonic()
            # Always push OCR events + first/last page; otherwise throttle.
            first_or_last = done == 1 or done == total
            if not (did_ocr or first_or_last or now - last_push_ts["t"] >= 0.5):
                return
            last_push_ts["t"] = now
            asyncio.run_coroutine_threadsafe(
                _record_extraction_progress(done, total, ocr_count, did_ocr),
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

        # Hard-fail on empty extraction instead of marking the log
        # "completed" with zero rows. An extractor that produces no TOC
        # entries means either: (a) the PDF uses a section-numbering scheme
        # the current strategies don't recognize (NEC/CEC-style
        # "Article 210" rather than "1.1.1"), (b) the front matter is
        # image-scanned and Tesseract didn't hit the row threshold, or (c)
        # the PDF is not a code book at all. In every case the right
        # behaviour is a loud error the operator can action, not a silent
        # green checkmark. The PDF row is kept so /retry can re-run after
        # the extractor is improved.
        if not parsed_sections:
            pages_tried = extracted.page_count or 0
            toc_found = len(extracted.toc_entries)
            msg = (
                f"Extractor returned 0 sections (TOC entries: {toc_found}, "
                f"pages: {pages_tried}). The PDF was stored as "
                f"code_book_pdfs.id={source_pdf_id} and can be retried via "
                f"POST /api/imports/{import_log_id}/retry once the extractor "
                f"is updated for this PDF's TOC format."
            )
            logger.error("import aborted — empty extraction: %s", msg)
            async with db_pool.acquire() as conn:
                await conn.execute(
                    '''UPDATE import_logs
                       SET status = 'error', phase = 'empty_extraction',
                           records_total = 0, records_imported = 0,
                           records_failed = 0,
                           toc_entries_count = $1,
                           total_pages = $2,
                           error_message = $3,
                           stage_detail = $4,
                           completed_at = NOW(), updated_at = NOW()
                     WHERE id = $5''',
                    toc_found, pages_tried or None, msg,
                    'Empty extraction — no TOC entries detected.',
                    import_log_id,
                )
                if source_id:
                    await conn.execute(
                        '''UPDATE import_sources
                           SET status = 'error', error_message = $1
                           WHERE id = $2''',
                        'Empty extraction — TOC extractor found 0 entries',
                        source_id,
                    )
            result.errors.append(msg)
            return result

        await _set_phase(
            'parsing',
            toc_entries_count=len(extracted.toc_entries),
            ocr_pages_count=len(extracted.ocr_flagged_pages),
            total_pages=extracted.page_count or None,
            current_page=extracted.page_count or None,
            stage_detail=(
                f"Parsed {len(parsed_sections)} sections from "
                f"{len(extracted.toc_entries)} TOC entries · "
                f"{len(extracted.ocr_flagged_pages)} OCR page(s)"
            ),
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

        await _set_phase(
            'indexing',
            records_total=len(parsed_sections),
            records_processed=0,
            started_indexing_at=_now_utc(),
            stage_detail='Validating & embedding sections…',
        )

        # Step 2: Validate and insert sections
        content_validator = ContentValidator(db_pool)
        reference_extractor = ReferenceExtractor()

        total = len(parsed_sections)
        # Heartbeat every 2% (min 1) so the UI bar moves smoothly on big
        # books without overwhelming Postgres with per-section UPDATEs.
        PROGRESS_EVERY = max(1, total // 50)

        for idx, section in enumerate(parsed_sections):
            # Periodic heartbeat so the UI can show progress.
            if idx % PROGRESS_EVERY == 0 and import_log_id is not None:
                detail = (
                    f"Indexing section {section.section_number}"
                    f" ({idx + 1}/{total})"
                )
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            '''UPDATE import_logs
                               SET records_processed = $1,
                                   records_imported = $2,
                                   records_failed = $3,
                                   current_section_number = $4,
                                   references_found = $5,
                                   stage_detail = $6,
                                   updated_at = NOW()
                               WHERE id = $7''',
                            idx, result.imported, result.quarantined,
                            section.section_number,
                            result.references_found,
                            detail,
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

        # Step 3: Update import log and source.
        #
        # Distinguish three terminal outcomes so the UI can stop showing
        # "completed ✓" on runs that actually produced zero usable output:
        #   completed            — at least one section reached code_sections
        #   error/all_quarantined — every candidate section was rejected by
        #                           the content validator (Layer 2 garbage
        #                           detection etc.); operator needs to look
        #                           at content_quarantine
        #   error/no_candidates   — parser emitted TOC entries but none
        #                           produced body text (rare; usually a sign
        #                           of bad TOC page alignment)
        if result.imported > 0:
            final_status, final_phase = 'completed', 'completed'
            final_err = None
            detail = (
                f"Imported {result.imported} sections · "
                f"{result.references_found} references · "
                f"{result.quarantined} quarantined"
            )
        elif result.quarantined > 0:
            final_status, final_phase = 'error', 'all_quarantined'
            final_err = (
                f"All {result.quarantined} parsed sections failed content "
                f"validation and were quarantined; nothing indexed. Review "
                f"content_quarantine rows for source_id={source_id}."
            )
            detail = f"All {result.quarantined} sections quarantined — nothing indexed."
        else:
            final_status, final_phase = 'error', 'no_candidates'
            final_err = (
                f"Parsed {len(parsed_sections)} TOC entries but produced 0 "
                f"section bodies. Likely TOC page-alignment drift; retry via "
                f"/api/imports/{import_log_id}/retry after reviewing logs."
            )
            detail = f"0 of {len(parsed_sections)} TOC entries produced body text."

        async with db_pool.acquire() as conn:
            await conn.execute(
                '''UPDATE import_logs
                   SET status = $1, phase = $2,
                       records_processed = $3, records_imported = $4,
                       records_failed = $5,
                       references_found = $6,
                       current_section_number = NULL,
                       stage_detail = $7,
                       error_message = $8,
                       completed_at = NOW(),
                       updated_at = NOW()
                   WHERE id = $9''',
                final_status,
                final_phase,
                len(parsed_sections),
                result.imported,
                result.quarantined,
                result.references_found,
                detail,
                final_err,
                import_log_id,
            )

            await conn.execute(
                '''UPDATE import_sources
                   SET status = $1, sections_imported = $2, last_crawled = NOW()
                   WHERE id = $3''',
                final_status,
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
                           stage_detail = $4,
                           completed_at = NOW(), updated_at = NOW()
                       WHERE id = $5''',
                    'error',
                    'failed',
                    str(e),
                    f"Failed: {str(e)[:200]}",
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
