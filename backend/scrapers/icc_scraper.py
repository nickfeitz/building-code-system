"""ICC Digital Codes scraper using Playwright scraper service.

The ICC site is a Vue SPA — all content is JavaScript-rendered.
This scraper delegates rendering to the Playwright scraper service,
then parses the returned HTML with BeautifulSoup.
"""

import asyncio
import logging
import os
import re
import time
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from bs4 import BeautifulSoup
import httpx

from parsers.pdf_parser import ParsedSection

logger = logging.getLogger(__name__)

SCRAPER_SERVICE_URL = os.getenv("SCRAPER_SERVICE_URL", "http://scraper-service:8012")

# Rate limiting between requests to ICC (on top of scraper service's own rate limit)
REQUEST_DELAY_SECONDS = 2.0


@dataclass
class ScrapingStats:
    """Statistics from scraping operation."""
    chapters_scraped: int = 0
    sections_found: int = 0
    tables_found: int = 0
    errors: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def elapsed_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


class ICCScraper:
    """Scraper for ICC Digital Codes website via Playwright service."""

    BASE_URL = "https://codes.iccsafe.org"
    SECTION_NUMBER_PATTERN = re.compile(r'\b(\d{3,4}\.\d+(?:\.\d+)*)\b')

    def __init__(self):
        self.last_request_time = 0.0
        self.stats = ScrapingStats()

    async def scrape_code(
        self,
        code_url: str,
        code_book_id: int,
        current_chapter: Optional[str] = None,
    ) -> List[ParsedSection]:
        """Scrape all chapters of a code.

        Args:
            code_url: URL to code (e.g., https://codes.iccsafe.org/content/IBC2024P1)
            code_book_id: Database code book ID
            current_chapter: Optional chapter number to filter

        Returns:
            List of ParsedSection objects
        """
        self.stats = ScrapingStats()
        self.stats.start_time = time.time()

        try:
            logger.info(f"Starting code scrape: {code_url}")

            # Step 1: Get chapter list via Playwright service
            chapters = await self._get_chapters(code_url)

            # Validate chapters — reject javascript: links or too few results
            valid_chapters = [(t, u) for t, u in chapters if u.startswith("http") and "javascript:" not in u]
            if len(valid_chapters) < 5:
                logger.info(f"Only {len(valid_chapters)} valid chapters from Playwright, using known IBC URLs")
                chapters = self._known_ibc_chapters(code_url)
            else:
                chapters = valid_chapters

            logger.info(f"Found {len(chapters)} chapters")

            # Step 2: Scrape each chapter
            all_sections = []
            for idx, (title, url) in enumerate(chapters, 1):
                if current_chapter and str(idx) != str(current_chapter):
                    continue

                try:
                    logger.info(f"Scraping chapter {idx}/{len(chapters)}: {title}")
                    sections = await self._scrape_chapter(url, idx)
                    all_sections.extend(sections)
                    self.stats.chapters_scraped += 1
                    self.stats.sections_found += len(sections)
                    logger.info(f"  → {len(sections)} sections extracted")

                except Exception as e:
                    logger.error(f"Error scraping chapter {idx}: {e}")
                    self.stats.errors += 1

            logger.info(
                f"Code scrape complete: {self.stats.chapters_scraped} chapters, "
                f"{self.stats.sections_found} sections, {self.stats.tables_found} tables, "
                f"{self.stats.errors} errors in {self.stats.elapsed_seconds:.1f}s"
            )
            return all_sections

        except Exception as e:
            logger.error(f"Fatal error scraping code: {e}", exc_info=True)
            self.stats.errors += 1
            raise
        finally:
            self.stats.end_time = time.time()

    async def _get_chapters(self, code_url: str) -> List[Tuple[str, str]]:
        """Get chapter list via Playwright scraper service."""
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"{SCRAPER_SERVICE_URL}/chapters",
                    json={"code_url": code_url, "wait_ms": 10000},
                )
                if response.status_code == 200:
                    data = response.json()
                    chapters = data.get("chapters", [])
                    return [(ch["title"], ch["url"]) for ch in chapters]
        except Exception as e:
            logger.error(f"Failed to get chapters from scraper service: {e}")
        return []

    async def _scrape_chapter(self, chapter_url: str, chapter_number: int) -> List[ParsedSection]:
        """Scrape a single chapter via Playwright service."""
        await self._rate_limit()

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{SCRAPER_SERVICE_URL}/scrape-chapter",
                    json={"chapter_url": chapter_url, "wait_ms": 12000},
                )
                if response.status_code != 200:
                    logger.error(f"Scraper service returned {response.status_code} for {chapter_url}")
                    return []

                data = response.json()
                html = data.get("html", "")
                sections_hint = data.get("sections_found", 0)

                if not html:
                    logger.warning(f"Empty HTML for {chapter_url}")
                    return []

                logger.info(f"  Received {len(html)} chars, {sections_hint} sections hint")
                return self._parse_html_content(html, chapter_number)

        except httpx.TimeoutException:
            logger.error(f"Timeout scraping chapter {chapter_url}")
            return []
        except Exception as e:
            logger.error(f"Error scraping chapter {chapter_url}: {e}")
            return []

    # ── HTML Parsing ────────────────────────────────────────────────────

    def _parse_html_content(self, html: str, chapter_number: int) -> List[ParsedSection]:
        """Parse rendered HTML to extract code sections."""
        soup = BeautifulSoup(html, 'html.parser')
        sections = []
        current_chapter = str(chapter_number)

        # Strategy 1: Elements with sec_ IDs (ICC's section containers)
        for elem in soup.find_all(id=re.compile(r'^sec_\d+')):
            section = self._parse_section_element(elem, chapter_number)
            if section:
                sections.append(section)

        # Strategy 2: Content sections by class
        if not sections:
            for elem in soup.find_all(class_=re.compile(r'(section|code-section|content-section)')):
                section = self._parse_generic_section(elem, chapter_number)
                if section:
                    sections.append(section)

        # Strategy 3: Headings with section numbers
        if not sections:
            for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
                text = heading.get_text(strip=True)
                match = self.SECTION_NUMBER_PATTERN.search(text)
                if match:
                    section = self._parse_heading_section(heading, match, current_chapter)
                    if section:
                        sections.append(section)

        # Strategy 4: Bold text with section numbers (common ICC format)
        if not sections:
            for bold in soup.find_all(['b', 'strong']):
                text = bold.get_text(strip=True)
                match = self.SECTION_NUMBER_PATTERN.search(text)
                if match and len(text) < 300:
                    content = self._get_sibling_content(bold)
                    if content and len(content) > 30:
                        section_num = match.group(1)
                        title = text.replace(section_num, '', 1).strip().strip('.')
                        sections.append(ParsedSection(
                            section_number=section_num,
                            section_title=title or f"Section {section_num}",
                            full_text=f"{text}\n{content}",
                            chapter=current_chapter,
                            depth=self._calculate_depth(section_num),
                            path=self._build_path(current_chapter, section_num),
                            section_type=self._detect_section_type(content),
                        ))

        # Strategy 5: Paragraph text containing section numbers at start
        if not sections:
            for p in soup.find_all(['p', 'div']):
                text = p.get_text(strip=True)
                if len(text) > 50 and len(text) < 5000:
                    match = re.match(r'^(\d{3,4}\.\d+(?:\.\d+)*)\s+(.+)', text)
                    if match:
                        section_num = match.group(1)
                        rest = match.group(2)
                        # Title is usually first sentence
                        title_match = re.match(r'^([^.]+\.)', rest)
                        title = title_match.group(1) if title_match else rest[:100]
                        sections.append(ParsedSection(
                            section_number=section_num,
                            section_title=title.strip(),
                            full_text=text,
                            chapter=current_chapter,
                            depth=self._calculate_depth(section_num),
                            path=self._build_path(current_chapter, section_num),
                            section_type=self._detect_section_type(text),
                        ))

        logger.info(f"Extracted {len(sections)} sections from chapter {chapter_number}")
        return sections

    def _parse_section_element(self, elem, chapter_number: int) -> Optional[ParsedSection]:
        """Parse a section element (div with id='sec_XXX')."""
        heading = elem.find(['h2', 'h3', 'h4'])
        if not heading:
            return None

        heading_text = heading.get_text(strip=True)
        match = self.SECTION_NUMBER_PATTERN.search(heading_text)
        if not match:
            return None

        section_num = match.group(1)
        section_title = heading_text.replace(section_num, '', 1).strip().strip('.')

        content_parts = []
        for child in elem.children:
            if isinstance(child, str):
                t = child.strip()
                if t:
                    content_parts.append(t)
            elif hasattr(child, 'name') and child.name not in ['script', 'style']:
                if not re.search(r'id=["\']sec_', str(child)[:100]):
                    t = child.get_text(strip=True)
                    if t:
                        content_parts.append(t)

        full_text = ' '.join(content_parts)
        if len(full_text) < 20:
            return None

        tables = elem.find_all('table')
        if tables:
            self.stats.tables_found += len(tables)
            full_text = self._preserve_tables(elem)

        current_chapter = str(chapter_number)
        return ParsedSection(
            section_number=section_num,
            section_title=section_title or f"Section {section_num}",
            full_text=full_text,
            chapter=current_chapter,
            depth=self._calculate_depth(section_num),
            path=self._build_path(current_chapter, section_num),
            section_type=self._detect_section_type(full_text),
        )

    def _parse_generic_section(self, elem, chapter_number: int) -> Optional[ParsedSection]:
        """Parse a generic section element by CSS class."""
        text = elem.get_text(strip=True)
        match = self.SECTION_NUMBER_PATTERN.search(text[:200])
        if not match:
            return None

        section_num = match.group(1)
        first_line = text.split('\n')[0] if '\n' in text else text[:200]
        section_title = first_line.replace(section_num, '', 1).strip().strip('.')

        if len(text) < 30:
            return None

        return ParsedSection(
            section_number=section_num,
            section_title=section_title[:200] or f"Section {section_num}",
            full_text=text,
            chapter=str(chapter_number),
            depth=self._calculate_depth(section_num),
            path=self._build_path(str(chapter_number), section_num),
            section_type=self._detect_section_type(text),
        )

    def _parse_heading_section(self, heading, match, current_chapter: str) -> Optional[ParsedSection]:
        """Parse a section from a heading element."""
        text = heading.get_text(strip=True)
        section_num = match.group(1)
        section_title = text.replace(section_num, '', 1).strip().strip('.')

        content = self._get_section_content(heading)
        if not content or len(content) < 30:
            return None

        return ParsedSection(
            section_number=section_num,
            section_title=section_title or f"Section {section_num}",
            full_text=f"{text}\n{content}",
            chapter=current_chapter,
            depth=self._calculate_depth(section_num),
            path=self._build_path(current_chapter, section_num),
            section_type=self._detect_section_type(content),
        )

    def _get_section_content(self, heading) -> str:
        """Get content after heading until next heading."""
        parts = []
        current = heading.find_next_sibling()
        level = int(heading.name[1]) if heading.name and heading.name[0] == 'h' else 3

        while current:
            if hasattr(current, 'name') and current.name and current.name[0] == 'h':
                try:
                    if int(current.name[1]) <= level:
                        break
                except (ValueError, IndexError):
                    pass
            if hasattr(current, 'get_text'):
                t = current.get_text(strip=True)
                if t:
                    parts.append(t)
            current = current.find_next_sibling()

        return ' '.join(parts)

    def _get_sibling_content(self, elem) -> str:
        """Get text from siblings until next section marker."""
        parts = []
        current = elem.find_next_sibling()

        while current:
            if hasattr(current, 'name') and current.name in ['b', 'strong', 'h1', 'h2', 'h3', 'h4']:
                t = current.get_text(strip=True)
                if self.SECTION_NUMBER_PATTERN.search(t):
                    break
            if hasattr(current, 'get_text'):
                t = current.get_text(strip=True)
                if t:
                    parts.append(t)
            current = current.find_next_sibling()

        return ' '.join(parts)

    # ── Table handling ─────────────────────────────────────────────────

    def _preserve_tables(self, elem) -> str:
        parts = []
        for child in elem.children:
            if hasattr(child, 'name'):
                if child.name == 'table':
                    parts.append(self._table_to_text(child))
                else:
                    t = child.get_text(strip=True)
                    if t:
                        parts.append(t)
            else:
                t = str(child).strip()
                if t:
                    parts.append(t)
        return '\n'.join(parts)

    def _table_to_text(self, table) -> str:
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
            if cells:
                rows.append(' | '.join(cells))
        return ('TABLE:\n' + '\n'.join(rows) + '\nEND TABLE') if rows else ''

    # ── Utility ────────────────────────────────────────────────────────

    def _calculate_depth(self, section_num: str) -> int:
        return section_num.count('.')

    def _build_path(self, chapter: Optional[str], section_num: str) -> str:
        parts = [chapter or '0']
        parts.extend(section_num.split('.'))
        return '.'.join(parts)

    def _detect_section_type(self, text: str) -> str:
        tl = text.lower()
        if 'table' in tl or 'TABLE:' in text:
            return 'table'
        if 'figure' in tl:
            return 'figure'
        if 'appendix' in tl:
            return 'appendix'
        if 'definition' in tl:
            return 'definition'
        return 'section'

    def _known_ibc_chapters(self, code_url: str) -> List[Tuple[str, str]]:
        """Known IBC chapter URL slugs as fallback."""
        base = code_url.rstrip('/')
        chapters = [
            ("Chapter 1 - Scope and Administration", "chapter-1-scope-and-administration"),
            ("Chapter 2 - Definitions", "chapter-2-definitions"),
            ("Chapter 3 - Use and Occupancy Classification", "chapter-3-use-and-occupancy-classification"),
            ("Chapter 4 - Special Detailed Requirements", "chapter-4-special-detailed-requirements-based-on-use-and-occupancy"),
            ("Chapter 5 - General Building Heights and Areas", "chapter-5-general-building-heights-and-areas"),
            ("Chapter 6 - Types of Construction", "chapter-6-types-of-construction"),
            ("Chapter 7 - Fire and Smoke Protection Features", "chapter-7-fire-and-smoke-protection-features"),
            ("Chapter 8 - Interior Finishes", "chapter-8-interior-finishes"),
            ("Chapter 9 - Fire Protection and Life Safety Systems", "chapter-9-fire-protection-and-life-safety-systems"),
            ("Chapter 10 - Means of Egress", "chapter-10-means-of-egress"),
            ("Chapter 11 - Accessibility", "chapter-11-accessibility"),
            ("Chapter 12 - Interior Environment", "chapter-12-interior-environment"),
            ("Chapter 13 - Energy Efficiency", "chapter-13-energy-efficiency"),
            ("Chapter 14 - Exterior Walls", "chapter-14-exterior-walls"),
            ("Chapter 15 - Roof Assemblies and Rooftop Structures", "chapter-15-roof-assemblies-and-rooftop-structures"),
            ("Chapter 16 - Structural Design", "chapter-16-structural-design"),
            ("Chapter 17 - Special Inspections and Tests", "chapter-17-special-inspections-and-tests"),
            ("Chapter 18 - Soils and Foundations", "chapter-18-soils-and-foundations"),
            ("Chapter 19 - Concrete", "chapter-19-concrete"),
            ("Chapter 20 - Aluminum", "chapter-20-aluminum"),
            ("Chapter 21 - Masonry", "chapter-21-masonry"),
            ("Chapter 22 - Steel", "chapter-22-steel"),
            ("Chapter 23 - Wood", "chapter-23-wood"),
            ("Chapter 24 - Glass and Glazing", "chapter-24-glass-and-glazing"),
            ("Chapter 25 - Gypsum Board, Gypsum Panel Products and Plaster", "chapter-25-gypsum-board-gypsum-panel-products-and-plaster"),
            ("Chapter 26 - Plastic", "chapter-26-plastic"),
            ("Chapter 27 - Electrical", "chapter-27-electrical"),
            ("Chapter 28 - Mechanical Systems", "chapter-28-mechanical-systems"),
            ("Chapter 29 - Plumbing Systems", "chapter-29-plumbing-systems"),
            ("Chapter 30 - Elevators and Conveying Systems", "chapter-30-elevators-and-conveying-systems"),
            ("Chapter 31 - Special Construction", "chapter-31-special-construction"),
            ("Chapter 32 - Encroachments Into The Public Right-Of-Way", "chapter-32-encroachments-into-the-public-right-of-way"),
            ("Chapter 33 - Safeguards During Construction", "chapter-33-safeguards-during-construction"),
            ("Chapter 34 - Existing Buildings and Structures", "chapter-34-existing-buildings-and-structures"),
            ("Chapter 35 - Referenced Standards", "chapter-35-referenced-standards"),
        ]
        return [(title, f"{base}/{slug}") for title, slug in chapters]

    async def _rate_limit(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY_SECONDS:
            await asyncio.sleep(REQUEST_DELAY_SECONDS - elapsed)
        self.last_request_time = time.time()
