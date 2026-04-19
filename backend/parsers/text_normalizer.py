"""Normalize PDF-extracted section text into flowing prose.

PyMuPDF's text layer preserves visual line wraps — every line break in
the output is typically where the PDF wrapped to fit the page, not where
the author intended a paragraph. That kind of text makes full-text search
("wind loads for buildings") miss hits that straddle a wrap, wastes LLM
tokens, and produces slightly noisier embeddings.

This module collapses soft wraps into spaces while preserving the
structural boundaries that actually matter:

    - blank lines → paragraph breaks
    - list items ("1.", "(a)", "•") → their own paragraphs
    - callouts ("EXCEPTION:", "User Note:", "COMMENTARY:") → their own paragraphs
    - hyphenated wraps ("build-\\ning" → "building")

The output is a single string with ``\\n\\n`` separating paragraphs, so
consumers (search, embedding, LLM context, UI) can treat it as plain
prose. The raw per-line text is preserved separately on the row
(``full_text_raw``) for audit.
"""

from __future__ import annotations

import re

_LIST_ITEM_RE = re.compile(
    r"""^(
        \d+\.\s                          # "1. ", "23. "
      | \(\s*[a-z0-9ivx]+\s*\)           # "(a)", "(i)", "(12)"
      | [•◦·●]\s                         # bullets
      | (EXCEPTION|EXCEPTIONS|User\sNote|Note|COMMENTARY)s?:   # callouts
    )""",
    re.IGNORECASE | re.VERBOSE,
)


def normalize(
    text: str,
    section_number: str | None = None,
    section_title: str | None = None,
) -> str:
    """Return ``text`` with soft line-wraps collapsed to flowing paragraphs.

    Args:
        text: Raw body text as extracted from the PDF.
        section_number: Section number the body is known to start with,
            e.g. ``"26.1.2"``. If the very first token matches, it is
            stripped from the body so the UI can render the heading
            separately without a duplicate in the prose.
        section_title: Section title that typically follows the number,
            e.g. ``"Permitted Procedures"``. Stripped along with the
            number when both are present back-to-back.

    Returns:
        Normalized text with paragraphs separated by ``\\n\\n``. Runs of
        whitespace inside a paragraph collapse to a single space.
    """
    if not text:
        return ""

    work = text
    if section_number:
        num_esc = re.escape(section_number.strip())
        title_stripped = (section_title or "").strip().rstrip(".:—-")
        if title_stripped:
            # Strip "<number> <title>" as a single unit. Use \s+ between so
            # minor whitespace drift doesn't block the match.
            title_esc = re.escape(title_stripped).replace(r"\ ", r"\s+")
            # Case-insensitive title match: body often uses ALL CAPS for
            # the heading ("1.1 SCOPE") while section_title is title-case
            # ("Scope"). Number match stays case-sensitive.
            work = re.sub(
                rf"^\s*{num_esc}\s+{title_esc}\s*",
                "",
                work,
                count=1,
                flags=re.IGNORECASE,
            )
        else:
            # No title known — strip just the leading number token.
            work = re.sub(rf"^\s*{num_esc}\s+", "", work, count=1)

    paragraphs: list[str] = []
    for block in re.split(r"\n\s*\n+", work):
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if not lines:
            continue

        buf = ""

        def flush(buf_in: str) -> str:
            if buf_in:
                paragraphs.append(_collapse(buf_in))
            return ""

        for line in lines:
            if _LIST_ITEM_RE.match(line):
                buf = flush(buf)
                buf = line
            elif buf and buf.endswith("-"):
                # Word split across wrapped lines: drop the hyphen, no space.
                buf = buf[:-1] + line
            elif buf:
                buf += " " + line
            else:
                buf = line

        flush(buf)

    return "\n\n".join(paragraphs)


def _collapse(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s).strip()
