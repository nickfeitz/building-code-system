"""Seed the catalog of publishing orgs, code cycles, and code books.

Populates ~10 publishing organizations, ~67 code cycles, and ~178 code books:
    • California Title 24 cycles 2010+ (all ~12 parts each, with Part 2.5/CRC
      from 2010 on and Part 10/CEBC from 2016 on).
    • Upstream model codes each CA Title 24 part is amended from:
        - ICC I-codes  (IBC/IRC/IFC/IEBC/IMC/IPC/IECC/IgCC)
        - NFPA 70 (NEC)
        - IAPMO UPC / UMC
    • Major referenced standards California invokes heavily:
        - ASHRAE 90.1, ASHRAE 189.1
        - ASCE 7
        - NFPA 13, 72, 101, 54, 58

URL policy: digital_access_url is only set when the URL is known to be on
ICC Digital Codes with free public access. Everything else is left NULL;
the Catalog UI surfaces these as "needs URL" and the Import panel's PDF
upload still works as a fallback.

Idempotent: if code_cycles already has rows, the script is a no-op unless
called with --force, which truncates the catalog tables and re-seeds.
The content tables (code_sections, code_references, embeddings) are NOT
touched — only the metadata catalog.

Usage (from inside the backend container):
    python seed_catalog.py            # seed if empty, else no-op
    python seed_catalog.py --force    # wipe catalog tables + reseed
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date
from typing import Optional

import asyncpg


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "building_code")


# --- Publishing orgs -------------------------------------------------------

ORGS: list[tuple[str, str, str]] = [
    ("CBSC", "California Building Standards Commission", "https://www.dgs.ca.gov/BSC"),
    ("ICC", "International Code Council", "https://www.iccsafe.org"),
    ("NFPA", "National Fire Protection Association", "https://www.nfpa.org"),
    ("IAPMO", "International Association of Plumbing and Mechanical Officials", "https://www.iapmo.org"),
    ("ASHRAE", "American Society of Heating, Refrigerating and Air-Conditioning Engineers", "https://www.ashrae.org"),
    ("ASCE", "American Society of Civil Engineers", "https://www.asce.org"),
    ("UL", "Underwriters Laboratories", "https://www.ul.com"),
    ("ASTM", "ASTM International", "https://www.astm.org"),
    ("AWS", "American Welding Society", "https://www.aws.org"),
    ("ACI", "American Concrete Institute", "https://www.concrete.org"),
]


# --- Cycles ----------------------------------------------------------------
# Each row: (adopting_authority, cycle_year, display_name, effective_date,
#            expiration_date_or_None, status, publishing_org_abbr_for_books)
#
# "adopting_authority" is the logical family used in the Catalog UI grouping
# (California, ICC, NFPA 70, IAPMO, ASHRAE 90.1, ASHRAE 189.1, ASCE 7,
#  NFPA 13, NFPA 72, NFPA 101, NFPA 54, NFPA 58). publishing_org_abbr is the
# UNIQUE abbreviation in publishing_orgs used for the code_books rows.

_CYCLES: list[tuple[str, int, str, date, Optional[date], str, str]] = []


def _add_cycle_row(authority: str, year: int, name: str, eff: date,
                   exp: Optional[date], status: str, org: str) -> None:
    _CYCLES.append((authority, year, name, eff, exp, status, org))


# California Title 24 — effective Jan 1 of year+1; superseded by next cycle.
# As of 2026-04-16, the 2025 code is active; 2022 became superseded 2026-01-01.
for yr, eff_yr, exp_yr, status in [
    (2010, 2011, 2014, "superseded"),
    (2013, 2014, 2017, "superseded"),
    (2016, 2017, 2020, "superseded"),
    (2019, 2020, 2023, "superseded"),
    (2022, 2023, 2026, "superseded"),
    (2025, 2026, None, "active"),
]:
    _add_cycle_row(
        "California", yr, f"{yr} California Building Standards Code (Title 24)",
        date(eff_yr, 1, 1),
        date(exp_yr, 1, 1) if exp_yr else None,
        status, "CBSC",
    )

# ICC I-codes — 3-year cycle, published the year named, active ~3 yrs.
# Current active is 2024; 2021 superseded 2024-01-01.
for yr, status in [
    (2009, "superseded"), (2012, "superseded"), (2015, "superseded"),
    (2018, "superseded"), (2021, "superseded"), (2024, "active"),
]:
    _add_cycle_row(
        "ICC", yr, f"{yr} International Codes (I-Codes)",
        date(yr, 1, 1),
        date(yr + 3, 1, 1) if status == "superseded" else None,
        status, "ICC",
    )

# NFPA 70 (NEC) — 3-year cycle; current active is 2023.
for yr, status in [
    (2008, "superseded"), (2011, "superseded"), (2014, "superseded"),
    (2017, "superseded"), (2020, "superseded"), (2023, "active"),
]:
    _add_cycle_row(
        "NFPA 70", yr, f"{yr} National Electrical Code (NFPA 70)",
        date(yr, 1, 1),
        date(yr + 3, 1, 1) if status == "superseded" else None,
        status, "NFPA",
    )

# IAPMO UPC/UMC — 3-year cycle like ICC; current active is 2024.
for yr, status in [
    (2009, "superseded"), (2012, "superseded"), (2015, "superseded"),
    (2018, "superseded"), (2021, "superseded"), (2024, "active"),
]:
    _add_cycle_row(
        "IAPMO", yr, f"{yr} Uniform Plumbing and Mechanical Codes",
        date(yr, 1, 1),
        date(yr + 3, 1, 1) if status == "superseded" else None,
        status, "IAPMO",
    )

# ASHRAE 90.1 — 3-year revision cycle; current active is 2022.
for yr, status in [
    (2010, "superseded"), (2013, "superseded"), (2016, "superseded"),
    (2019, "superseded"), (2022, "active"),
]:
    _add_cycle_row(
        "ASHRAE 90.1", yr, f"ANSI/ASHRAE/IES Standard 90.1-{yr}",
        date(yr, 10, 1),
        date(yr + 3, 10, 1) if status == "superseded" else None,
        status, "ASHRAE",
    )

# ASHRAE 189.1 — 3-year revision cycle; current active is 2023.
for yr, status in [
    (2011, "superseded"), (2014, "superseded"), (2017, "superseded"),
    (2020, "superseded"), (2023, "active"),
]:
    _add_cycle_row(
        "ASHRAE 189.1", yr, f"ANSI/ASHRAE/USGBC/IES Standard 189.1-{yr}",
        date(yr, 10, 1),
        date(yr + 3, 10, 1) if status == "superseded" else None,
        status, "ASHRAE",
    )

# ASCE 7 — 6-year revision cycle; current active is 7-22.
for yr, status in [
    (2010, "superseded"), (2016, "superseded"), (2022, "active"),
]:
    _add_cycle_row(
        "ASCE 7", yr, f"ASCE/SEI 7-{str(yr)[-2:]} Minimum Design Loads",
        date(yr, 6, 1),
        date(yr + 6, 6, 1) if status == "superseded" else None,
        status, "ASCE",
    )

# NFPA fire-life-safety suite — 3-year cycles roughly aligned with CA adoption.
_NFPA_FAMILIES = [
    ("NFPA 13", "Standard for the Installation of Sprinkler Systems",
     [2010, 2013, 2016, 2019, 2022, 2025]),
    ("NFPA 72", "National Fire Alarm and Signaling Code",
     [2010, 2013, 2016, 2019, 2022, 2025]),
    ("NFPA 101", "Life Safety Code",
     [2009, 2012, 2015, 2018, 2021, 2024]),
    ("NFPA 54", "National Fuel Gas Code",
     [2009, 2012, 2015, 2018, 2021, 2024]),
    ("NFPA 58", "Liquefied Petroleum Gas Code",
     [2008, 2011, 2014, 2017, 2020, 2023]),
]
for authority, title, years in _NFPA_FAMILIES:
    for i, yr in enumerate(years):
        status = "active" if i == len(years) - 1 else "superseded"
        nxt = years[i + 1] if i + 1 < len(years) else None
        _add_cycle_row(
            authority, yr, f"{authority}-{yr} {title}",
            date(yr, 1, 1),
            date(nxt, 1, 1) if nxt else None,
            status, "NFPA",
        )


# --- Books ------------------------------------------------------------------
# Book rows are generated per-cycle based on the authority family.

# CA Title 24 parts:
#   (part_number, abbreviation, full_name_suffix, category,
#    base_model_abbr_or_None, min_ca_cycle)
# base_model_abbr is matched to the ICC/NFPA/IAPMO cycle that's one year older.
_CA_PARTS: list[tuple[str, str, str, str, Optional[str], int]] = [
    ("Part 1",    "CAC",       "California Administrative Code",            "Admin",                  None,   2010),
    ("Part 2",    "CBC",       "California Building Code",                   "Building",               "IBC",  2010),
    ("Part 2.5",  "CRC",       "California Residential Code",                "Residential",            "IRC",  2010),
    ("Part 3",    "CEC",       "California Electrical Code",                 "Electrical",             "NEC",  2010),
    ("Part 4",    "CMC",       "California Mechanical Code",                 "Mechanical",             "UMC",  2010),
    ("Part 5",    "CPC",       "California Plumbing Code",                   "Plumbing",               "UPC",  2010),
    ("Part 6",    "CEnergy",   "California Energy Code",                     "Energy",                 None,   2010),
    ("Part 8",    "CHBC",      "California Historical Building Code",        "Building",               None,   2010),
    ("Part 9",    "CFC",       "California Fire Code",                       "Fire",                   "IFC",  2010),
    ("Part 10",   "CEBC",      "California Existing Building Code",          "Existing",               "IEBC", 2016),
    ("Part 11",   "CALGreen",  "California Green Building Standards Code",   "Green",                  None,   2010),
    ("Part 12",   "CRSC",      "California Referenced Standards Code",       "Referenced Standards",   None,   2010),
]


# ICC I-codes published per cycle:
#   (abbreviation, full_name, category, min_icc_cycle)
# IgCC was first published in 2012 -> skip for ICC 2009.
_ICC_BOOKS: list[tuple[str, str, str, int]] = [
    ("IBC",   "International Building Code",                    "Building",    2009),
    ("IRC",   "International Residential Code",                  "Residential", 2009),
    ("IFC",   "International Fire Code",                         "Fire",        2009),
    ("IEBC",  "International Existing Building Code",            "Existing",    2009),
    ("IMC",   "International Mechanical Code",                   "Mechanical",  2009),
    ("IPC",   "International Plumbing Code",                     "Plumbing",    2009),
    ("IECC",  "International Energy Conservation Code",          "Energy",      2009),
    ("IgCC",  "International Green Construction Code",           "Green",       2012),
]


# IAPMO books per cycle: (abbreviation, full_name, category)
_IAPMO_BOOKS = [
    ("UPC", "Uniform Plumbing Code",   "Plumbing"),
    ("UMC", "Uniform Mechanical Code", "Mechanical"),
]


# URL policy: fill digital_access_url only for code books we're confident
# live on ICC Digital Codes with free public access. Everything else: NULL.
# ICC hosts both its own I-Codes and the California Title 24 parts in a
# predictable slug pattern, but older cycles aren't reliably public.
#
# Confident slugs (verified patterns on codes.iccsafe.org as of writing):
#   ICC 2021, 2024 I-codes: e.g. /content/IBC2021, /content/IBC2024
#   CA 2022, 2025 Title 24 (many parts): /content/CBC2022P2V1 etc.
#
# Returning None means the book will be in the catalog but not scannable
# until the user supplies a URL (either via the Catalog panel's edit action
# in a follow-up, or by uploading a PDF via the Import panel).

ICC_BASE = "https://codes.iccsafe.org/content"


def icc_url_for_icode(abbr: str, year: int) -> Optional[str]:
    if year in (2021, 2024):
        return f"{ICC_BASE}/{abbr}{year}"
    return None


def icc_url_for_ca_part(abbr: str, part: str, year: int) -> Optional[str]:
    if year not in (2022, 2025):
        return None
    # ICC's CA slug pattern uses the part number.
    # e.g. /content/CBC2022P2V1, /content/CRC2022P2_5, /content/CFC2022P9
    part_slug = part.replace("Part ", "P").replace(".", "_")
    if abbr == "CBC":
        # Part 2 is split into two volumes on ICC
        return f"{ICC_BASE}/CBC{year}{part_slug}V1"
    return f"{ICC_BASE}/{abbr}{year}{part_slug}"


async def _seed(conn: asyncpg.Connection, force: bool) -> None:
    cycle_count = await conn.fetchval("SELECT count(*) FROM code_cycles")
    if cycle_count and not force:
        print(f"[seed] code_cycles already has {cycle_count} rows; use --force to reset.")
        return

    if force:
        # Safety check: CASCADE on code_sections.code_book_id means deleting
        # code_books would wipe all scraped content. Refuse if any sections exist.
        existing_sections = await conn.fetchval("SELECT count(*) FROM code_sections")
        if existing_sections:
            raise RuntimeError(
                f"--force refused: code_sections has {existing_sections} scraped rows "
                "that would be cascade-deleted. Drop them manually first if this is "
                "truly what you want."
            )
        print("[seed] --force: wiping catalog tables (no scraped sections present)...")
        await conn.execute("DELETE FROM code_books")
        await conn.execute("DELETE FROM code_cycles")
        await conn.execute("DELETE FROM publishing_orgs")

    # --- 1. Publishing orgs -------------------------------------------------
    print(f"[seed] Inserting {len(ORGS)} publishing orgs...")
    org_ids: dict[str, int] = {}
    for abbr, full_name, website in ORGS:
        org_ids[abbr] = await conn.fetchval(
            """INSERT INTO publishing_orgs (abbreviation, full_name, website)
               VALUES ($1, $2, $3) RETURNING id""",
            abbr, full_name, website,
        )

    # --- 2. Cycles ----------------------------------------------------------
    print(f"[seed] Inserting {len(_CYCLES)} code cycles...")
    cycle_ids: dict[tuple[str, int], int] = {}
    cycle_org: dict[tuple[str, int], str] = {}
    for authority, year, name, eff, exp, status, org_abbr in _CYCLES:
        cycle_ids[(authority, year)] = await conn.fetchval(
            """INSERT INTO code_cycles
                   (name, effective_date, expiration_date, status,
                    adopting_authority, notes)
               VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
            name, eff, exp, status, authority,
            f"Published by {org_abbr}; cycle year {year}.",
        )
        cycle_org[(authority, year)] = org_abbr

    # Helper: resolve the ICC/NFPA/IAPMO base-model cycle year matching a
    # California cycle (CA-nnnn uses I-codes from n-1 etc.).
    CA_BASE_YEAR_OFFSET = {
        2010: 2009, 2013: 2012, 2016: 2015,
        2019: 2018, 2022: 2021, 2025: 2024,
    }
    CA_NEC_YEAR = {
        2010: 2008, 2013: 2011, 2016: 2014,
        2019: 2017, 2022: 2020, 2025: 2023,
    }

    # --- 3. Code books ------------------------------------------------------
    inserted = 0

    async def _ins_book(code_name: str, abbreviation: str, part_number: Optional[str],
                        cycle_id: int, base_model_abbr: Optional[str],
                        base_model_code: Optional[str], base_code_year: Optional[int],
                        publishing_org_id: int, category: str,
                        digital_access_url: Optional[str],
                        status: str, effective_date: date,
                        superseded_date: Optional[date]) -> None:
        nonlocal inserted
        await conn.execute(
            """INSERT INTO code_books
                   (code_name, abbreviation, part_number, cycle_id,
                    base_model_code, base_model_abbreviation, base_code_year,
                    publishing_org_id, category, digital_access_url, status,
                    effective_date, superseded_date)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)""",
            code_name, abbreviation, part_number, cycle_id,
            base_model_code, base_model_abbr, base_code_year,
            publishing_org_id, category, digital_access_url, status,
            effective_date, superseded_date,
        )
        inserted += 1

    # California Title 24 parts
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "California":
            continue
        cid = cycle_ids[("California", year)]
        base_yr = CA_BASE_YEAR_OFFSET[year]
        nec_yr = CA_NEC_YEAR[year]
        for part_number, abbr, full_name_suffix, category, base_abbr, min_year in _CA_PARTS:
            if year < min_year:
                continue
            # Pick the correct base model year for NEC vs IBC/IRC etc.
            if base_abbr == "NEC":
                bmyr = nec_yr
                bmcode = f"{nec_yr} National Electrical Code"
            elif base_abbr in ("IBC", "IRC", "IFC", "IEBC", "IMC", "IPC"):
                bmyr = base_yr
                bmcode = f"{base_yr} International {full_name_suffix.split('California ', 1)[-1]}"
            elif base_abbr in ("UMC", "UPC"):
                bmyr = base_yr
                long_map = {"UMC": "Uniform Mechanical Code", "UPC": "Uniform Plumbing Code"}
                bmcode = f"{base_yr} {long_map[base_abbr]}"
            else:
                bmyr = None
                bmcode = None
            full_name = f"{year} {full_name_suffix} — {part_number}"
            url = icc_url_for_ca_part(abbr, part_number, year)
            await _ins_book(
                code_name=full_name,
                abbreviation=abbr,
                part_number=part_number,
                cycle_id=cid,
                base_model_abbr=base_abbr,
                base_model_code=bmcode,
                base_code_year=bmyr,
                publishing_org_id=org_ids["CBSC"],
                category=category,
                digital_access_url=url,
                status=status,
                effective_date=eff,
                superseded_date=exp,
            )

    # ICC I-codes
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "ICC":
            continue
        cid = cycle_ids[("ICC", year)]
        for abbr, full_name, category, min_year in _ICC_BOOKS:
            if year < min_year:
                continue
            url = icc_url_for_icode(abbr, year)
            await _ins_book(
                code_name=f"{year} {full_name}",
                abbreviation=abbr,
                part_number=None,
                cycle_id=cid,
                base_model_abbr=None,
                base_model_code=None,
                base_code_year=None,
                publishing_org_id=org_ids["ICC"],
                category=category,
                digital_access_url=url,
                status=status,
                effective_date=eff,
                superseded_date=exp,
            )

    # NEC
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "NFPA 70":
            continue
        cid = cycle_ids[("NFPA 70", year)]
        await _ins_book(
            code_name=f"{year} National Electrical Code (NFPA 70)",
            abbreviation="NEC",
            part_number=None,
            cycle_id=cid,
            base_model_abbr=None, base_model_code=None, base_code_year=None,
            publishing_org_id=org_ids["NFPA"],
            category="Electrical",
            digital_access_url=None,
            status=status, effective_date=eff, superseded_date=exp,
        )

    # IAPMO UPC + UMC
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "IAPMO":
            continue
        cid = cycle_ids[("IAPMO", year)]
        for abbr, full_name, category in _IAPMO_BOOKS:
            await _ins_book(
                code_name=f"{year} {full_name}",
                abbreviation=abbr,
                part_number=abbr,  # distinguish UPC vs UMC within a cycle (both NULL would dup-protect trip)
                cycle_id=cid,
                base_model_abbr=None, base_model_code=None, base_code_year=None,
                publishing_org_id=org_ids["IAPMO"],
                category=category,
                digital_access_url=None,
                status=status, effective_date=eff, superseded_date=exp,
            )

    # ASHRAE 90.1
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "ASHRAE 90.1":
            continue
        cid = cycle_ids[("ASHRAE 90.1", year)]
        await _ins_book(
            code_name=f"ANSI/ASHRAE/IES Standard 90.1-{year} — Energy Standard for Buildings Except Low-Rise Residential",
            abbreviation="ASHRAE 90.1",
            part_number=None, cycle_id=cid,
            base_model_abbr=None, base_model_code=None, base_code_year=None,
            publishing_org_id=org_ids["ASHRAE"],
            category="Energy",
            digital_access_url=None,
            status=status, effective_date=eff, superseded_date=exp,
        )

    # ASHRAE 189.1
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "ASHRAE 189.1":
            continue
        cid = cycle_ids[("ASHRAE 189.1", year)]
        await _ins_book(
            code_name=f"ANSI/ASHRAE/USGBC/IES Standard 189.1-{year} — Standard for the Design of High-Performance Green Buildings",
            abbreviation="ASHRAE 189.1",
            part_number=None, cycle_id=cid,
            base_model_abbr=None, base_model_code=None, base_code_year=None,
            publishing_org_id=org_ids["ASHRAE"],
            category="Green",
            digital_access_url=None,
            status=status, effective_date=eff, superseded_date=exp,
        )

    # ASCE 7
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority != "ASCE 7":
            continue
        cid = cycle_ids[("ASCE 7", year)]
        await _ins_book(
            code_name=f"ASCE/SEI 7-{str(year)[-2:]} — Minimum Design Loads and Associated Criteria for Buildings and Other Structures",
            abbreviation="ASCE 7",
            part_number=None, cycle_id=cid,
            base_model_abbr=None, base_model_code=None, base_code_year=None,
            publishing_org_id=org_ids["ASCE"],
            category="Building",
            digital_access_url=None,
            status=status, effective_date=eff, superseded_date=exp,
        )

    # NFPA 13/72/101/54/58
    _NFPA_TITLES = {
        "NFPA 13":  ("NFPA 13",  "Fire",     "Standard for the Installation of Sprinkler Systems"),
        "NFPA 72":  ("NFPA 72",  "Fire",     "National Fire Alarm and Signaling Code"),
        "NFPA 101": ("NFPA 101", "Fire",     "Life Safety Code"),
        "NFPA 54":  ("NFPA 54",  "Mechanical", "National Fuel Gas Code"),
        "NFPA 58":  ("NFPA 58",  "Mechanical", "Liquefied Petroleum Gas Code"),
    }
    for authority, year, _name, eff, exp, status, _org in _CYCLES:
        if authority not in _NFPA_TITLES:
            continue
        cid = cycle_ids[(authority, year)]
        abbr, category, title = _NFPA_TITLES[authority]
        await _ins_book(
            code_name=f"{abbr}-{year} — {title}",
            abbreviation=abbr,
            part_number=None, cycle_id=cid,
            base_model_abbr=None, base_model_code=None, base_code_year=None,
            publishing_org_id=org_ids["NFPA"],
            category=category,
            digital_access_url=None,
            status=status, effective_date=eff, superseded_date=exp,
        )

    print(f"[seed] Inserted {inserted} code books.")
    print("[seed] Done.")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--force", action="store_true",
                        help="Wipe catalog tables and re-seed (content tables untouched).")
    args = parser.parse_args()

    conn = await asyncpg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        database=POSTGRES_DB,
    )
    try:
        async with conn.transaction():
            await _seed(conn, force=args.force)
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
