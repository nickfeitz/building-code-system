"""Normalize PDF-extracted section text into flowing prose.

PyMuPDF's text layer preserves visual line wraps — every line break in
the output is typically where the PDF wrapped to fit the page, not where
the author intended a paragraph. On top of that, ASCE-family and IBC-family
PDFs ship with custom fonts whose glyph-to-Unicode tables are wrong,
so the raw extraction contains Latin-alphabet lookalikes where math
symbols belong (e.g. ``þ`` for ``+``, ``ð`` for ``(``, ``Þ`` for ``)``).

This module performs, in order:

    1. Glyph-encoding repair (þ → +, ð → (, Þ → ), 0:7 → 0.7, ligatures).
    2. Leading "<number> <title>" strip so it doesn't duplicate the UI heading.
    3. Line-block reassembly: blank lines separate paragraphs; list items,
       formula items ("1a. D + L"), and callouts stay on their own lines.
    4. Hyphen repair for words split across wraps ("build-\\ning" → "building").
    5. Subscript restoration on formula lines (WT → W_T, Lr → L_r, etc.)
       so the frontend can render proper ``<sub>`` tags.

Output: a single string with ``\\n\\n`` separating paragraphs, ready for
search, embedding, LLM context, and UI rendering. The raw per-line text
is preserved separately on each section row (``code_sections.full_text_raw``)
for audit and re-normalization.
"""

from __future__ import annotations

import re


# --- Glyph-encoding repair -------------------------------------------------
#
# Custom-font codepoints the ASCE/IBC PDFs use where the glyph *shape*
# reads as a math symbol but the Unicode assignment is a Latin letter.
# Safe to substitute globally: none of these codepoints are used in
# legitimate English prose inside building codes.
_GLYPH_FIXES = {
    "þ": "+",   # U+00FE Latin small letter thorn   → plus
    "Þ": ")",   # U+00DE Latin capital letter thorn → close paren
    "ð": "(",   # U+00F0 Latin small letter eth     → open paren
    "Ð": "(",   # U+00D0 seen in some IBC PDFs
    # Common ligatures — cosmetic, not a correctness fix, but make the
    # output searchable ("coefficient", not "coefﬁcient") and render
    # cleanly in any font.
    "ﬁ": "fi", "ﬂ": "fl", "ﬀ": "ff", "ﬃ": "ffi", "ﬄ": "ffl",
}
_GLYPH_TRANS = str.maketrans(_GLYPH_FIXES)

# "0:7S" or "0:6D" — the PDF's custom font encodes the decimal point as
# a colon in some equation contexts. Only rewrite when the colon sits
# between two digits, or between a digit and an ASCII letter that
# immediately follows with no space (a load symbol like "7S", "6D").
_DECIMAL_COLON_RE = re.compile(r"(\d):(?=\d|[A-Z]\b|[A-Z][a-z]?\b)")


# --- Formula / list / callout detection -----------------------------------
#
# Anything matching these starts its own paragraph. We widen the list
# pattern to include formula items like "1a. D", "3a. D + (Lr or 0.7S or R)"
# that ASCE and IBC both use for load combinations; otherwise they jam
# into one unreadable block.
_LIST_ITEM_RE = re.compile(
    r"""^(
        \d+[a-z]?\.\s                    # "1. ", "1a. ", "23b. "
      | \(\s*[a-z0-9ivx]+\s*\)           # "(a)", "(i)", "(12)"
      | [•◦·●]\s                         # bullets
      | (EXCEPTION|EXCEPTIONS|User\sNote|Note|COMMENTARY)s?:   # callouts
    )""",
    re.IGNORECASE | re.VERBOSE,
)

# A paragraph qualifies as a "formula line" (eligible for subscript
# restoration) when it starts with a numbered/lettered formula marker
# AND contains at least one operator token — `+`, `−`, `=`, or a
# parenthesis run. Prose items that merely happen to start with "1."
# are not formulas and skip the subscript pass.
_FORMULA_STARTER_RE = re.compile(r"^\d+[a-z]?\.\s+[A-Z0-9(]")
_FORMULA_OPERATOR_RE = re.compile(r"[+−=]|\(")


# --- Subscript restoration -------------------------------------------------
#
# ASCE 7 uses a fixed vocabulary of subscript-named loads. After the
# glyph fixes collapse the PDF's formatting these come through as
# flat two-letter tokens ("WT", "Lr") that should be "W_T", "L_r".
# The dict is ordered longest-first inside the compiled regex so
# "SDS" wins over "SD".
_SUBSCRIPT_TOKENS = {
    # Snow / roof
    "Lr": "L_r",
    "pf": "p_f", "ps": "p_s", "pg": "p_g",
    # Wind
    "WT": "W_T",
    "Kz": "K_z", "Kd": "K_d", "Kh": "K_h", "Kzt": "K_zt", "Ke": "K_e",
    "qz": "q_z", "qh": "q_h",
    "Cf": "C_f", "Cp": "C_p",
    # Seismic (multi-letter; ordered to match longest-first)
    "SDS": "S_DS", "SD1": "S_D1",
    "SMS": "S_MS", "SM1": "S_M1",
    "PGAM": "PGA_M",
    "MCER": "MCE_R",
    "Cs": "C_s", "Cd": "C_d", "Cu": "C_u",
    "TL": "T_L",
    # Misc
    "Ak": "A_k", "Af": "A_f", "Ag": "A_g",
    "Di": "D_i", "Wi": "W_i",
    "hi": "h_i", "hn": "h_n",
    "Fy": "F_y", "Fu": "F_u",
    "Iz": "I_z", "Ie": "I_e",
    "VS30": "V_S30",
}
# Sort by length desc so longer keys match first inside the alternation.
_SUBSCRIPT_ALTERNATION = "|".join(
    sorted((re.escape(k) for k in _SUBSCRIPT_TOKENS), key=len, reverse=True)
)
# Word-boundary match; never rewrites inside a larger word.
_SUBSCRIPT_RE = re.compile(rf"\b({_SUBSCRIPT_ALTERNATION})\b")


# --- Detection: did normalization leave any suspect glyphs behind? --------
#
# Consumers (validator, Quarantine panel) call ``has_glyph_artifacts`` to
# decide whether a section should be flagged for human review.
_GLYPH_SUSPECT_RE = re.compile(
    r"""
        [þðÞÐﬁﬂﬀﬃﬄ]                       # raw glyph survivors
      | \b\d+:\d                          # decimal-as-colon survivor
    """,
    re.VERBOSE,
)


def has_glyph_artifacts(text: str) -> bool:
    """True when ``text`` looks like it still has PDF font-encoding junk.

    Used by the validator to push a section into ``content_quarantine``
    with ``reason = 'glyph_encoding_suspect'`` for human review. The
    normalizer itself tries to fix everything first; surviving hits
    usually mean a new code family (e.g. NEC) uses a different custom
    font that needs an additional mapping entry.
    """
    return bool(_GLYPH_SUSPECT_RE.search(text or ""))


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

    # 1. Glyph repair — must come BEFORE heading strip, since the leading
    #    header could itself contain a damaged character.
    work = text.translate(_GLYPH_TRANS)
    work = _DECIMAL_COLON_RE.sub(r"\1.", work)

    # 2. Strip the leading "<number> <title>" header from the body.
    if section_number:
        num_esc = re.escape(section_number.strip())
        title_stripped = (section_title or "").strip().rstrip(".:—-")
        if title_stripped:
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
            work = re.sub(rf"^\s*{num_esc}\s+", "", work, count=1)

    # 3. Line-block reassembly.
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

    # 4. Subscript restoration — only on formula-looking paragraphs so
    #    prose like "the WT-stamped plate" (if it existed) wouldn't be
    #    rewritten. The formula detection is conservative.
    out: list[str] = []
    for p in paragraphs:
        if _is_formula_line(p):
            p = _SUBSCRIPT_RE.sub(lambda m: _SUBSCRIPT_TOKENS[m.group(1)], p)
        out.append(p)

    return "\n\n".join(out)


def _is_formula_line(p: str) -> bool:
    """Conservative: formula-marker AND contains an operator."""
    return bool(_FORMULA_STARTER_RE.match(p) and _FORMULA_OPERATOR_RE.search(p))


def _collapse(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s).strip()
