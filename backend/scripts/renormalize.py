"""Backfill: move existing `full_text` into `full_text_raw`, re-normalize, re-embed.

One-shot migration for rows that were ingested before migration 002 /
`text_normalizer` landed. For each row in the target book(s):

    1. If `full_text_raw` is null, copy current `full_text` into it.
       (Any subsequent re-run is a no-op for that row.)
    2. Compute `normalize(full_text_raw, section_number)` and write it
       back to `full_text`.
    3. Recompute the `normalized_hash` dedup key.
    4. Optionally re-embed via the embedding service.

Usage (inside the backend container):

    # One book at a time:
    docker exec backend python -m scripts.renormalize --book-id 148

    # All books that still have a NULL full_text_raw:
    docker exec backend python -m scripts.renormalize --all-stale

    # Skip re-embed (fast dry-run of text changes):
    docker exec backend python -m scripts.renormalize --book-id 148 --no-embed

Safe to re-run. Rows that already have `full_text_raw` populated are
skipped unless ``--force`` is passed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys

import asyncpg
import httpx

# Allow `python -m scripts.renormalize` when run via
# `docker exec backend python -m scripts.renormalize ...` (workdir /app).
sys.path.insert(0, "/app")

from parsers.text_normalizer import normalize  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("renormalize")


def _norm_hash(text: str) -> str:
    """Whitespace-collapsed lowercase hash used for cross-code dedup."""
    norm = re.sub(r"\s+", " ", (text or "").lower()).strip()
    return hashlib.sha256(norm.encode()).hexdigest()


async def _embed(text: str, url: str, client: httpx.AsyncClient) -> str | None:
    try:
        r = await client.post(f"{url}/embed", json={"text": text})
        r.raise_for_status()
        emb = r.json().get("embedding")
        if not emb:
            return None
        return "[" + ",".join(str(x) for x in emb) + "]"
    except Exception as e:
        log.warning("embed failed: %s", e)
        return None


async def run(
    *,
    book_id: int | None,
    all_stale: bool,
    force: bool,
    do_embed: bool,
) -> None:
    pool = await asyncpg.create_pool(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DB"],
        min_size=1,
        max_size=5,
    )
    embed_url = os.environ.get(
        "EMBEDDING_SERVICE_URL", "http://embedding-service:8011"
    )

    try:
        # Build the row selector.
        where: list[str] = ["superseded_date IS NULL"]
        params: list = []
        if book_id is not None:
            where.append(f"code_book_id = ${len(params) + 1}")
            params.append(book_id)
        if not force:
            where.append("full_text_raw IS NULL")
        if all_stale and book_id is None:
            # no extra filter; --all-stale just means "don't require a book_id"
            pass

        sql = (
            "SELECT id, section_number, section_title, full_text, full_text_raw "
            f"FROM code_sections WHERE {' AND '.join(where)}"
        )

        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        log.info("loaded %d rows to renormalize", len(rows))

        async with httpx.AsyncClient(timeout=30.0) as http:
            for i, row in enumerate(rows, 1):
                # If we're not forcing, full_text_raw was null by filter,
                # so the current full_text is the untouched PDF text. Keep it.
                raw = row["full_text_raw"] or row["full_text"]
                clean = normalize(
                    raw,
                    section_number=row["section_number"],
                    section_title=row["section_title"],
                )
                norm_hash = _norm_hash(clean)

                embedding = None
                if do_embed:
                    embedding = await _embed(clean, embed_url, http)

                async with pool.acquire() as conn:
                    if embedding is not None:
                        await conn.execute(
                            """UPDATE code_sections
                               SET full_text_raw = COALESCE(full_text_raw, $1),
                                   full_text = $2,
                                   normalized_hash = $3,
                                   embedding = $4::vector,
                                   updated_at = CURRENT_TIMESTAMP
                             WHERE id = $5""",
                            raw, clean, norm_hash, embedding, row["id"],
                        )
                    else:
                        await conn.execute(
                            """UPDATE code_sections
                               SET full_text_raw = COALESCE(full_text_raw, $1),
                                   full_text = $2,
                                   normalized_hash = $3,
                                   updated_at = CURRENT_TIMESTAMP
                             WHERE id = $4""",
                            raw, clean, norm_hash, row["id"],
                        )

                if i % 100 == 0:
                    log.info("progress: %d / %d", i, len(rows))

        log.info("done")

    finally:
        await pool.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--book-id", type=int, help="Renormalize a single code book")
    g.add_argument(
        "--all-stale",
        action="store_true",
        help="Renormalize every row with full_text_raw IS NULL",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-process rows even if full_text_raw already populated",
    )
    p.add_argument(
        "--no-embed",
        action="store_true",
        help="Skip embedding-service calls (dry-run of text changes)",
    )
    args = p.parse_args()

    asyncio.run(
        run(
            book_id=args.book_id,
            all_stale=args.all_stale,
            force=args.force,
            do_embed=not args.no_embed,
        )
    )


if __name__ == "__main__":
    main()
