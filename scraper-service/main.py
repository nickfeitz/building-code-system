"""Playwright-based scraper service for JavaScript-rendered code sites.

Renders SPA pages via headless Chromium and returns extracted HTML content.
The backend calls this service to get rendered chapter content, then parses
it using existing BeautifulSoup parsers.
"""

import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from typing import Optional, List, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser, BrowserContext
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiting
REQUEST_DELAY = 2.0
last_request_time = 0.0

# Browser instance (shared across requests)
browser: Optional[Browser] = None
playwright_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage browser lifecycle."""
    global browser, playwright_instance
    playwright_instance = await async_playwright().start()
    browser = await playwright_instance.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-background-timer-throttling",
        ],
    )
    logger.info("Playwright browser launched")
    yield
    if browser:
        await browser.close()
    if playwright_instance:
        await playwright_instance.stop()
    logger.info("Playwright browser closed")


app = FastAPI(title="Scraper Service", lifespan=lifespan)


# ── Request/Response Models ─────────────────────────────────────────

class RenderRequest(BaseModel):
    """Request to render a page and extract content."""
    url: str
    wait_selector: Optional[str] = None  # CSS selector to wait for
    wait_ms: int = 5000  # Max ms to wait for content
    extract_selector: Optional[str] = None  # CSS selector to extract


class RenderResponse(BaseModel):
    """Rendered page content."""
    url: str
    html: str
    title: str
    content_length: int
    sections_detected: int


class ChapterListRequest(BaseModel):
    """Request to get chapter list from a code book page."""
    code_url: str  # e.g., https://codes.iccsafe.org/content/IBC2024P1
    wait_ms: int = 8000


class ChapterInfo(BaseModel):
    title: str
    url: str


class ChapterListResponse(BaseModel):
    code_url: str
    chapters: List[ChapterInfo]


class ScrapeChapterRequest(BaseModel):
    """Request to scrape a single chapter."""
    chapter_url: str
    wait_ms: int = 10000


class ScrapeChapterResponse(BaseModel):
    chapter_url: str
    html: str
    sections_found: int
    content_length: int


# ── Endpoints ───────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "browser": browser is not None and browser.is_connected(),
    }


@app.post("/render", response_model=RenderResponse)
async def render_page(request: RenderRequest):
    """Render a JavaScript page and return the HTML."""
    await _rate_limit()

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )

    try:
        page = await context.new_page()
        await page.goto(request.url, wait_until="networkidle", timeout=30000)

        # Wait for specific selector if provided
        if request.wait_selector:
            try:
                await page.wait_for_selector(request.wait_selector, timeout=request.wait_ms)
            except Exception:
                logger.warning(f"Selector {request.wait_selector} not found within {request.wait_ms}ms")

        # Additional wait for dynamic content
        await page.wait_for_timeout(2000)

        # Get rendered HTML
        if request.extract_selector:
            elements = await page.query_selector_all(request.extract_selector)
            html_parts = []
            for elem in elements:
                html_parts.append(await elem.inner_html())
            html = "\n".join(html_parts)
        else:
            html = await page.content()

        title = await page.title()

        # Count section numbers in rendered content
        section_count = len(re.findall(r'\b\d{3,4}\.\d+', html))

        return RenderResponse(
            url=request.url,
            html=html,
            title=title,
            content_length=len(html),
            sections_detected=section_count,
        )

    except Exception as e:
        logger.error(f"Error rendering {request.url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await context.close()


@app.post("/chapters", response_model=ChapterListResponse)
async def get_chapters(request: ChapterListRequest):
    """Get chapter list from a code book's table of contents."""
    await _rate_limit()

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )

    try:
        page = await context.new_page()
        await page.goto(request.code_url, wait_until="networkidle", timeout=30000)

        # Wait for TOC to render
        await page.wait_for_timeout(request.wait_ms)

        # Try multiple strategies to find chapter links
        chapters = []

        # Strategy 1: Look for links with "chapter" in href
        links = await page.query_selector_all('a[href*="chapter"]')
        for link in links:
            href = await link.get_attribute("href")
            text = (await link.inner_text()).strip()
            if href and text and re.search(r'chapter\s*\d+', text, re.IGNORECASE):
                full_url = href if href.startswith("http") else f"https://codes.iccsafe.org{href}"
                chapters.append(ChapterInfo(title=text, url=full_url))

        # Strategy 2: Look for TOC items in ICC's Vue components
        if not chapters:
            toc_items = await page.query_selector_all('.toc-item a, .chapter-link, .nav-link')
            for item in toc_items:
                href = await item.get_attribute("href")
                text = (await item.inner_text()).strip()
                if href and text:
                    full_url = href if href.startswith("http") else f"https://codes.iccsafe.org{href}"
                    chapters.append(ChapterInfo(title=text, url=full_url))

        # Strategy 3: Look for sidebar navigation links
        if not chapters:
            sidebar_links = await page.query_selector_all(
                '#sidebar a, .sidebar a, nav a, .navigation a, .v-list-item a'
            )
            for link in sidebar_links:
                href = await link.get_attribute("href")
                text = (await link.inner_text()).strip()
                if href and text and ('chapter' in text.lower() or re.search(r'^\d+\.', text)):
                    full_url = href if href.startswith("http") else f"https://codes.iccsafe.org{href}"
                    chapters.append(ChapterInfo(title=text, url=full_url))

        # Deduplicate
        seen = set()
        unique_chapters = []
        for ch in chapters:
            if ch.url not in seen:
                seen.add(ch.url)
                unique_chapters.append(ch)

        logger.info(f"Found {len(unique_chapters)} chapters at {request.code_url}")
        return ChapterListResponse(
            code_url=request.code_url,
            chapters=unique_chapters,
        )

    except Exception as e:
        logger.error(f"Error getting chapters from {request.code_url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await context.close()


@app.post("/scrape-chapter", response_model=ScrapeChapterResponse)
async def scrape_chapter(request: ScrapeChapterRequest):
    """Scrape a single chapter page, waiting for content to render."""
    await _rate_limit()

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )

    try:
        page = await context.new_page()

        # Navigate to chapter
        await page.goto(request.chapter_url, wait_until="networkidle", timeout=30000)

        # Wait for content to render — look for section numbers in the page
        try:
            # ICC renders code content in a main content area
            await page.wait_for_selector(
                '.content-section, .code-content, .chapter-content, #content-body, article',
                timeout=request.wait_ms,
            )
        except Exception:
            logger.info("Content selector not found, waiting additional time...")
            await page.wait_for_timeout(5000)

        # Scroll to load lazy content
        await _scroll_page(page)

        # Get full page HTML
        html = await page.content()

        # Also try to get just the main content area
        content_selectors = [
            '.content-section',
            '.code-content',
            '.chapter-content',
            '#content-body',
            'article',
            'main',
            '.v-main__wrap',
        ]

        content_html = ""
        for selector in content_selectors:
            try:
                elem = await page.query_selector(selector)
                if elem:
                    inner = await elem.inner_html()
                    if len(inner) > len(content_html):
                        content_html = inner
            except Exception:
                continue

        # Use the more specific content if available, otherwise full page
        final_html = content_html if len(content_html) > 500 else html

        # Count sections
        sections_found = len(re.findall(r'\b\d{3,4}\.\d+(?:\.\d+)*\b', final_html))

        logger.info(
            f"Scraped {request.chapter_url}: {len(final_html)} chars, "
            f"{sections_found} sections detected"
        )

        return ScrapeChapterResponse(
            chapter_url=request.chapter_url,
            html=final_html,
            sections_found=sections_found,
            content_length=len(final_html),
        )

    except Exception as e:
        logger.error(f"Error scraping chapter {request.chapter_url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        await context.close()


# ── Utility ─────────────────────────────────────────────────────────

async def _scroll_page(page):
    """Scroll page to trigger lazy loading."""
    try:
        for i in range(5):
            await page.evaluate(f"window.scrollTo(0, {(i + 1) * 2000})")
            await page.wait_for_timeout(500)
        # Scroll back to top
        await page.evaluate("window.scrollTo(0, 0)")
        await page.wait_for_timeout(1000)
    except Exception:
        pass


async def _rate_limit():
    """Enforce rate limiting."""
    global last_request_time
    elapsed = time.time() - last_request_time
    if elapsed < REQUEST_DELAY:
        await asyncio.sleep(REQUEST_DELAY - elapsed)
    last_request_time = time.time()
