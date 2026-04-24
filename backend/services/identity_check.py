"""Pre-flight identity check for PDF imports.

Motivation — until this module landed, an operator could upload a PDF
into the wrong ``code_book`` (e.g. 2025 California Electrical Code
uploaded under the 2022 CBC dropdown) and the pipeline would happily
parse it and insert sections against the wrong book's id. The code
browser filters on ``code_book_id`` so those sections would appear under
the wrong heading and go unnoticed.

This module runs *before* the TOC extractor and answers one question:
"does the first few pages of this PDF plausibly belong to the selected
code book?" If the answer is "no, with confidence", we abort the import
with phase ``rejected_identity_mismatch`` and keep the PDF row around so
the operator can retarget or discard.

Strategy is layered from cheap to expensive:

1. **Deterministic heuristic.** Pull the first ~5 pages of text, look
   for obvious tokens: the 4-digit cycle year, the code name abbreviation
   (CEC/CBC/CMC/NFPA 70/…), and the part number. If we find the expected
   year AND an abbreviation consistent with the selected book, we accept
   without ever calling the LLM. This handles the ~80% case and keeps
   import latency at +~0.1s.

2. **Ollama LLM fallback.** When the heuristic is ambiguous, call the
   configured Ollama model with a tight JSON-mode prompt that receives
   the extracted front-matter text + embedded outline chapter titles
   and must return:

       {
         "title": "...",
         "edition_year": 2025,
         "publisher": "ICC",
         "discipline": "electrical",
         "match": true,
         "confidence": 0.0..1.0,
         "reason": "..."
       }

3. **Graceful skip on infrastructure failure.** If Ollama is unreachable
   or returns malformed JSON after retries, we log the event and return
   ``skipped=True``. We do NOT block the import on an LLM outage — the
   deterministic layer already ran, and blocking all uploads on a local
   model being down is a worse failure mode than occasionally letting a
   mis-selected PDF through.

The service is called from ``services.import_service.import_pdf`` right
after the PDF + log rows are created and before ``DocumentExtractor``
runs, so a rejection never writes anything to ``code_sections``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, asdict
from typing import Optional

import fitz  # PyMuPDF
import httpx

logger = logging.getLogger(__name__)

# How many front-matter pages to feed into the heuristics + LLM. The cover
# + title page + the "About this edition" / "Preface" pages are almost
# always within the first 5; expanding further risks dragging article
# bodies into the prompt.
FRONT_MATTER_PAGES = 5

# Titles pulled from the embedded outline — up to this many, to keep the
# prompt bounded regardless of how deep the outline goes.
MAX_OUTLINE_TITLES = 30

# Confidence thresholds. The rejection bar is deliberately > the
# acceptance bar: we only reject with high LLM confidence that the books
# don't match, because false rejections stall legitimate uploads.
REJECT_CONFIDENCE = 0.70
ACCEPT_CONFIDENCE = 0.60

# Abbreviation → {family, discipline}. Used by the deterministic layer to
# decide whether an abbreviation found on the cover is consistent with
# the selected code book.
_ABBR_FAMILY = {
    "CBC":     ("california", "building"),
    "CRC":     ("california", "residential"),
    "CAC":     ("california", "administrative"),
    "CEC":     ("california", "electrical"),
    "CMC":     ("california", "mechanical"),
    "CPC":     ("california", "plumbing"),
    "CFC":     ("california", "fire"),
    "CEBC":    ("california", "existing"),
    "CGBSC":   ("california", "green"),
    "CHBC":    ("california", "historical"),
    "CENERGY": ("california", "energy"),
    "CRSC":    ("california", "referenced"),
    "IBC":     ("icc",        "building"),
    "IRC":     ("icc",        "residential"),
    "IMC":     ("icc",        "mechanical"),
    "IPC":     ("icc",        "plumbing"),
    "IFC":     ("icc",        "fire"),
    "IECC":    ("icc",        "energy"),
    "IEBC":    ("icc",        "existing"),
    "NEC":     ("nfpa",       "electrical"),
    "NFPA 70": ("nfpa",       "electrical"),
    "NFPA 13": ("nfpa",       "sprinkler"),
    "NFPA 72": ("nfpa",       "fire-alarm"),
}


@dataclass
class IdentityCheckResult:
    """Outcome of the identity check.

    ``accepted``: proceed with the import pipeline.
    ``rejected``: abort; ``reason`` explains why, surfaced in the UI.
    ``skipped``:  one of the layers errored (Ollama offline, no text
                  layer, etc.); caller's policy is to proceed but log.

    ``extracted_title`` / ``confidence`` / ``source`` travel back to the
    import_logs row for operator review (``identity_title`` column etc.
    from migration 004).
    """
    accepted: bool
    rejected: bool
    skipped: bool
    reason: str
    extracted_title: Optional[str]
    confidence: float
    source: str  # "heuristic" | "llm" | "skipped"
    notes: dict

    def to_notes_json(self) -> str:
        """Serialise ``notes`` + key fields for the identity_notes column."""
        payload = {**self.notes, "source": self.source, "reason": self.reason}
        try:
            return json.dumps(payload, ensure_ascii=False)[:4000]
        except Exception:
            return json.dumps({"source": self.source, "reason": self.reason})


async def check_identity(
    *,
    pdf_path: str,
    selected_book: dict,
    ollama_url: str,
    ollama_model: str,
    ollama_num_ctx: int,
) -> IdentityCheckResult:
    """Run the full identity-check cascade.

    ``selected_book`` is a dict carrying the fields we need off the
    ``code_books`` row the user picked in the upload dropdown:
        code_name, abbreviation, part_number, base_code_year,
        cycle_name (joined from code_cycles.name).

    Returns an ``IdentityCheckResult``. The caller is expected to:
      * accept → continue to DocumentExtractor
      * reject → write status='error', phase='rejected_identity_mismatch'
      * skip   → continue but log notes for later review
    """
    front_text, outline_titles, page_count = _extract_front_matter(pdf_path)
    if not front_text and not outline_titles:
        # A PDF with no readable front matter at all is a signal that
        # either the file is scan-only (OCR will handle it later) or
        # corrupt. Don't block — the extractor will produce its own
        # diagnostic. Skip with a note.
        return IdentityCheckResult(
            accepted=True, rejected=False, skipped=True,
            reason="No front-matter text extractable; skipping identity check.",
            extracted_title=None, confidence=0.0, source="skipped",
            notes={"page_count": page_count, "front_text_chars": 0},
        )

    # Layer 1 — deterministic heuristic. If this answers definitively,
    # we skip the LLM call entirely.
    heur = _heuristic_match(front_text, outline_titles, selected_book)
    if heur["verdict"] == "accept":
        return IdentityCheckResult(
            accepted=True, rejected=False, skipped=False,
            reason=heur["reason"],
            extracted_title=heur.get("extracted_title"),
            confidence=heur["confidence"],
            source="heuristic",
            notes={
                "page_count": page_count,
                "matched_tokens": heur["matched_tokens"],
            },
        )
    if heur["verdict"] == "reject":
        return IdentityCheckResult(
            accepted=False, rejected=True, skipped=False,
            reason=heur["reason"],
            extracted_title=heur.get("extracted_title"),
            confidence=heur["confidence"],
            source="heuristic",
            notes={
                "page_count": page_count,
                "mismatched_tokens": heur["mismatched_tokens"],
            },
        )

    # Layer 2 — LLM verdict. The heuristic was ambiguous (e.g. cover says
    # "Electrical Code" but no year token was found). Ask Ollama.
    try:
        llm = await _llm_verdict(
            front_text=front_text,
            outline_titles=outline_titles,
            selected_book=selected_book,
            ollama_url=ollama_url,
            ollama_model=ollama_model,
            ollama_num_ctx=ollama_num_ctx,
        )
    except Exception as e:
        logger.warning("identity_check: Ollama call failed: %s", e)
        return IdentityCheckResult(
            accepted=True, rejected=False, skipped=True,
            reason=f"Ollama unreachable ({type(e).__name__}); identity check skipped.",
            extracted_title=None, confidence=0.0, source="skipped",
            notes={"page_count": page_count, "llm_error": str(e)[:200]},
        )

    match = bool(llm.get("match"))
    conf = float(llm.get("confidence") or 0.0)
    title = llm.get("title") or None

    if not match and conf >= REJECT_CONFIDENCE:
        return IdentityCheckResult(
            accepted=False, rejected=True, skipped=False,
            reason=(
                f"LLM says the PDF is \"{title}\" (confidence {conf:.2f}) "
                f"which does not match the selected book "
                f"\"{selected_book.get('code_name')}\". "
                f"{llm.get('reason') or ''}".strip()
            ),
            extracted_title=title,
            confidence=conf,
            source="llm",
            notes={"page_count": page_count, "llm": llm},
        )
    if match and conf >= ACCEPT_CONFIDENCE:
        return IdentityCheckResult(
            accepted=True, rejected=False, skipped=False,
            reason=f"LLM confirmed match (confidence {conf:.2f}).",
            extracted_title=title,
            confidence=conf,
            source="llm",
            notes={"page_count": page_count, "llm": llm},
        )

    # Low-confidence result in either direction — don't block, but log
    # so the operator can review via identity_notes.
    return IdentityCheckResult(
        accepted=True, rejected=False, skipped=True,
        reason=(
            f"LLM returned low confidence ({conf:.2f}); accepting with note "
            f"so the parser can run. Extracted title: {title}."
        ),
        extracted_title=title,
        confidence=conf,
        source="llm",
        notes={"page_count": page_count, "llm": llm},
    )


# --- Layer 1: deterministic --------------------------------------------------


def _heuristic_match(
    front_text: str,
    outline_titles: list[str],
    selected_book: dict,
) -> dict:
    """Fast, deterministic pre-check against the selected book.

    Returns ``{"verdict": "accept"|"reject"|"unknown", ...}``. We only
    emit accept/reject when the answer is obvious; anything marginal
    falls through to the LLM layer.
    """
    haystack_raw = (front_text + "\n" + "\n".join(outline_titles))
    haystack = haystack_raw.lower()

    want_abbr = (selected_book.get("abbreviation") or "").strip().upper()
    want_year = selected_book.get("base_code_year")
    cycle_name = (selected_book.get("cycle_name") or "").lower()

    # Pull the "first plausible title" from the front matter — first
    # non-empty line that contains a 4-digit year between 1990 and 2099
    # is a good heuristic for code books. Falls back to the first line.
    extracted_title = None
    for line in front_text.splitlines():
        line = line.strip()
        if not line or len(line) < 8:
            continue
        if re.search(r"\b(19|20)\d{2}\b", line):
            extracted_title = line[:200]
            break
    if extracted_title is None:
        for line in front_text.splitlines():
            line = line.strip()
            if len(line) >= 8:
                extracted_title = line[:200]
                break

    matched = []
    mismatched = []

    # Token 1: year. A clear year match is a strong positive signal
    # because building-code PDFs universally put the cycle year on the
    # cover.
    if want_year:
        years_found = {int(y) for y in re.findall(r"\b(19|20)\d{2}\b", haystack_raw)}
        # re.findall with a group returns the group (e.g. "20") not the
        # whole match; rerun with the full pattern.
        years_found = {int(y) for y in re.findall(r"\b((?:19|20)\d{2})\b", haystack_raw)}
        if want_year in years_found:
            matched.append(f"year={want_year}")
        elif years_found:
            # A different year was present — almost certainly the wrong
            # edition. Years are cheap to verify so we weight this hard.
            mismatched.append(
                f"year selected={want_year} but cover shows {sorted(years_found)[:5]}"
            )

    # Token 2: abbreviation in haystack. "CEC" on the cover strongly
    # implies California Electrical Code. We check in a case-sensitive
    # way on the raw haystack so "cec" embedded in the word "receive"
    # doesn't produce a false positive.
    if want_abbr and len(want_abbr) >= 2:
        abbr_regex = re.compile(rf"\b{re.escape(want_abbr)}\b")
        if abbr_regex.search(haystack_raw):
            matched.append(f"abbreviation={want_abbr}")

    # Token 3: cross-abbreviation check. If the cover screams a
    # *different* family ("NFPA 70" when selected book is CBC, or vice
    # versa), flag it. We keep this conservative: California amended
    # codes frequently reference their ICC base codes, so ICC tokens
    # appearing inside a California book aren't a mismatch.
    for abbr, (family, discipline) in _ABBR_FAMILY.items():
        pat = re.compile(rf"\b{re.escape(abbr)}\b")
        if pat.search(haystack_raw):
            sel_family, sel_disc = _ABBR_FAMILY.get(want_abbr, ("", ""))
            if (
                sel_family and sel_family != family
                and discipline and sel_disc and discipline != sel_disc
                # Don't flag cross-family discipline matches — e.g. NEC
                # mentioned in a California Building Code is fine.
                and abbr != want_abbr
            ):
                # Cross-family AND cross-discipline is a strong mismatch
                # signal only when the cover line itself is this token;
                # we don't want body-text references to reject the book.
                if _appears_on_cover(front_text, abbr):
                    mismatched.append(
                        f"cover mentions {abbr} ({family}/{discipline}); "
                        f"selected is {want_abbr} ({sel_family}/{sel_disc})"
                    )

    # Token 4: cycle/jurisdiction substring. If the cycle has a very
    # specific name ("California Building Standards Code"), prefer seeing
    # it on the cover. This is additive to the year check.
    for keyword in _cycle_keywords(cycle_name):
        if keyword and keyword in haystack:
            matched.append(f"cycle={keyword}")
            break

    # Verdict logic. Require both year AND abbreviation/cycle signal to
    # auto-accept; fall back to LLM on anything messier.
    year_ok = any(tok.startswith("year=") for tok in matched)
    book_ok = any(tok.startswith(("abbreviation=", "cycle=")) for tok in matched)
    year_bad = any(tok.startswith("year ") for tok in mismatched)

    if year_bad:
        return {
            "verdict": "reject",
            "reason": "; ".join(mismatched),
            "confidence": 0.95,
            "extracted_title": extracted_title,
            "matched_tokens": matched,
            "mismatched_tokens": mismatched,
        }
    if year_ok and book_ok:
        return {
            "verdict": "accept",
            "reason": "; ".join(matched),
            "confidence": 0.9,
            "extracted_title": extracted_title,
            "matched_tokens": matched,
            "mismatched_tokens": mismatched,
        }
    return {
        "verdict": "unknown",
        "reason": (
            f"Heuristic inconclusive; matched={matched}, mismatched={mismatched}"
        ),
        "confidence": 0.5,
        "extracted_title": extracted_title,
        "matched_tokens": matched,
        "mismatched_tokens": mismatched,
    }


def _cycle_keywords(cycle_name: str) -> list[str]:
    """Short substrings from a cycle name that, if present on the cover,
    are a strong positive signal. E.g. "California Building Standards
    Code" → "california building standards".
    """
    if not cycle_name:
        return []
    words = [w for w in re.findall(r"[a-z]+", cycle_name) if len(w) >= 4]
    if len(words) >= 3:
        return [" ".join(words[:3])]
    return [" ".join(words)]


def _appears_on_cover(front_text: str, token: str) -> bool:
    """Is ``token`` on one of the first ~20 non-empty lines of the PDF?"""
    pat = re.compile(rf"\b{re.escape(token)}\b")
    n = 0
    for line in front_text.splitlines():
        line = line.strip()
        if not line:
            continue
        n += 1
        if n > 20:
            return False
        if pat.search(line):
            return True
    return False


# --- Front-matter extraction -------------------------------------------------


def _extract_front_matter(pdf_path: str) -> tuple[str, list[str], int]:
    """Return (front_text, outline_titles, page_count).

    Text-layer only — no OCR. Identity check runs before the extractor
    and we don't want to pay Tesseract cost just to verify the cover.
    Scanned PDFs with no text layer fall through to ``skipped=True``
    which still proceeds with the import.
    """
    try:
        with fitz.open(pdf_path) as doc:
            page_count = len(doc)
            chunks: list[str] = []
            limit = min(FRONT_MATTER_PAGES, page_count)
            for i in range(limit):
                try:
                    txt = doc.load_page(i).get_text() or ""
                except Exception:
                    txt = ""
                if txt.strip():
                    chunks.append(txt)
            front_text = "\n".join(chunks).strip()

            outline_titles: list[str] = []
            try:
                raw = doc.get_toc(simple=True)
                for level, title, _page in raw[:MAX_OUTLINE_TITLES]:
                    t = (title or "").strip()
                    if t:
                        outline_titles.append(t)
            except Exception:
                # Some PDFs raise on get_toc when the outline is
                # malformed; treat as "no outline" and move on.
                outline_titles = []

            return front_text, outline_titles, page_count
    except Exception as e:
        logger.warning("identity_check: unable to open PDF for front-matter extract: %s", e)
        return "", [], 0


# --- Layer 2: Ollama JSON-mode call -----------------------------------------


_LLM_SYSTEM_PROMPT = (
    "You verify whether a PDF cover + table of contents matches an "
    "operator-selected building-code book. Respond ONLY with a single "
    "JSON object and no prose. Fields: title (string), edition_year "
    "(integer or null), publisher (string or null), discipline (string: "
    "'building'|'residential'|'electrical'|'mechanical'|'plumbing'|'fire'|"
    "'energy'|'administrative'|'historical'|'existing'|'green'|'referenced'"
    "|'sprinkler'|'fire-alarm'|'other'), match (boolean — does the PDF "
    "correspond to the selected book?), confidence (0..1), reason "
    "(short string)."
)


async def _llm_verdict(
    *,
    front_text: str,
    outline_titles: list[str],
    selected_book: dict,
    ollama_url: str,
    ollama_model: str,
    ollama_num_ctx: int,
) -> dict:
    """Call Ollama /api/generate in JSON mode. Retries up to 2x."""
    # Trim the prompt hard — Ollama with format=json can get slow on big
    # contexts, and 8-12k chars of front matter is more than enough to
    # identify a code book.
    trimmed_text = front_text[:8000]
    outline_blob = "\n".join(f"- {t}" for t in outline_titles[:MAX_OUTLINE_TITLES])
    user_prompt = (
        "Selected book (what the operator picked in the UI):\n"
        f"  code_name:    {selected_book.get('code_name')}\n"
        f"  abbreviation: {selected_book.get('abbreviation')}\n"
        f"  part_number:  {selected_book.get('part_number')}\n"
        f"  edition_year: {selected_book.get('base_code_year')}\n"
        f"  cycle:        {selected_book.get('cycle_name')}\n"
        "\n"
        "PDF front matter (first pages, verbatim):\n"
        "---\n"
        f"{trimmed_text}\n"
        "---\n"
        "\n"
        "Embedded outline titles (if any):\n"
        f"{outline_blob or '(none)'}\n"
        "\n"
        "Does this PDF correspond to the selected book? Return the JSON "
        "object now."
    )

    body = {
        "model": ollama_model,
        "prompt": user_prompt,
        "system": _LLM_SYSTEM_PROMPT,
        "stream": False,
        "format": "json",
        "options": {
            "num_ctx": ollama_num_ctx,
            "temperature": 0.0,
        },
    }

    last_err: Optional[Exception] = None
    for attempt in range(2):
        try:
            timeout = httpx.Timeout(connect=10.0, read=180.0, write=30.0, pool=30.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(f"{ollama_url}/api/generate", json=body)
            if r.status_code != 200:
                raise RuntimeError(f"Ollama {r.status_code}: {r.text[:300]}")
            raw = r.json().get("response") or ""
            return _parse_llm_json(raw)
        except Exception as e:
            last_err = e
            if attempt < 1:
                await asyncio.sleep(2.0)
    assert last_err is not None
    raise last_err


def _parse_llm_json(raw: str) -> dict:
    """Extract the first JSON object from an Ollama response body.

    Ollama with ``format=json`` usually returns a clean object, but some
    models still wrap it in prose or emit an opening fence. Be tolerant.
    """
    s = raw.strip()
    if not s:
        raise ValueError("empty LLM response")
    # Fast path: already valid JSON.
    try:
        return json.loads(s)
    except Exception:
        pass
    # Fallback: scoop the first {...} block.
    m = re.search(r"\{.*\}", s, re.DOTALL)
    if not m:
        raise ValueError(f"no JSON object in LLM output: {s[:200]}")
    return json.loads(m.group(0))


# Re-exported for tests / debugging.
__all__ = [
    "IdentityCheckResult",
    "check_identity",
]

# asdict kept imported for callers who want to serialise result objects
_ = asdict
