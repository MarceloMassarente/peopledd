from __future__ import annotations

"""
peopledd.vendor.scraper
=======================
Standalone multi-strategy web scraper — ported from deepsearch ScrapeOrchestratorV5.

Chain (in order, first success wins):
  1. httpx + trafilatura  (fast, cacheable)
  2. Jina Reader API      (optional — set JINA_API_KEY)
  3. Browserless          (optional — JS-heavy pages)
  4. Wayback Machine      (optional — archived fallback)

Result object has:
  .success  bool
  .content  str   (clean markdown/text)
  .url      str   (final URL after redirects)
  .strategy str   (which adapter succeeded)

Usage:
    cfg = ScraperConfig(enable_browserless=True, browserless_endpoint="http://...")
    scraper = MultiStrategyScraper(cfg)
    result = await scraper.scrape_url("https://ri.empresa.com/governanca")
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScraperConfig:
    """Mirror of deepsearch ScraperConfig interface used in peopledd."""
    enable_httpx: bool = True
    enable_jina: bool = True
    enable_browserless: bool = False
    enable_document: bool = False      # not used in peopledd
    enable_wayback: bool = False

    browserless_endpoint: str | None = None
    browserless_token: str | None = None
    jina_api_key: str | None = None    # if None, uses env JINA_API_KEY

    request_timeout: int = 20
    browserless_timeout: int = 60
    jina_timeout: int = 30
    cache_ttl_sec: int = 3600

    timeout_ms: dict = field(default_factory=dict)  # unused, for compat
    max_retries: int = 2

    min_content_words: int = 50        # reject content shorter than this


# ─────────────────────────────────────────────────────────────────────────────
# Result
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScrapeResult:
    success: bool
    content: str
    url: str
    strategy: str
    status_code: int = 0
    error: str = ""

    @property
    def text(self) -> str:
        """Alias for .content (compat with deepsearch interface)."""
        return self.content


_FAIL = ScrapeResult(success=False, content="", url="", strategy="none")

# ─────────────────────────────────────────────────────────────────────────────
# Simple in-process URL cache (TTL-based)
# ─────────────────────────────────────────────────────────────────────────────

class _UrlCache:
    def __init__(self, ttl_sec: int = 3600):
        self._store: dict[str, tuple[float, str]] = {}
        self._ttl = ttl_sec

    def get(self, url: str) -> str | None:
        entry = self._store.get(url)
        if entry is None:
            return None
        ts, content = entry
        if time.time() - ts > self._ttl:
            del self._store[url]
            return None
        return content

    def set(self, url: str, content: str) -> None:
        self._store[url] = (time.time(), content)


# ─────────────────────────────────────────────────────────────────────────────
# Adapter: httpx + trafilatura
# ─────────────────────────────────────────────────────────────────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


async def _scrape_httpx(url: str, timeout: int) -> ScrapeResult:
    try:
        import httpx
    except ImportError:
        return ScrapeResult(success=False, content="", url=url, strategy="httpx", error="httpx not installed")

    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": _BROWSER_UA, "Accept": "text/html,application/xhtml+xml"},
        ) as client:
            resp = await client.get(url)
            final_url = str(resp.url)

            if resp.status_code in (403, 406, 429, 451):
                return ScrapeResult(
                    success=False, content="", url=final_url,
                    strategy="httpx", status_code=resp.status_code,
                    error=f"HTTP {resp.status_code}"
                )

            raw_html = resp.text
            content = _extract_text(raw_html, final_url)

            return ScrapeResult(
                success=bool(content),
                content=content,
                url=final_url,
                strategy="httpx",
                status_code=resp.status_code,
            )
    except Exception as e:
        return ScrapeResult(success=False, content="", url=url, strategy="httpx", error=str(e))


def _extract_text(html: str, url: str = "") -> str:
    """Extract clean text. Tries trafilatura first, falls back to basic HTML strip."""
    try:
        import trafilatura
        text = trafilatura.extract(
            html,
            include_tables=True,
            include_links=False,
            no_fallback=False,
            url=url or None,
        )
        if text:
            return text
    except Exception:
        pass

    # Minimal fallback: strip HTML tags
    import re
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:20_000]  # cap at 20k chars


# ─────────────────────────────────────────────────────────────────────────────
# Adapter: Jina Reader API
# ─────────────────────────────────────────────────────────────────────────────

async def _scrape_jina(url: str, api_key: str | None, timeout: int) -> ScrapeResult:
    key = api_key or os.environ.get("JINA_API_KEY", "")
    jina_url = f"https://r.jina.ai/{url}"
    headers: dict[str, str] = {"Accept": "text/markdown"}
    if key:
        headers["Authorization"] = f"Bearer {key}"

    try:
        import httpx
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={**headers, "User-Agent": _BROWSER_UA},
        ) as client:
            resp = await client.get(jina_url)
            resp.raise_for_status()
            content = resp.text.strip()
            return ScrapeResult(
                success=bool(content),
                content=content,
                url=url,
                strategy="jina",
                status_code=resp.status_code,
            )
    except Exception as e:
        return ScrapeResult(success=False, content="", url=url, strategy="jina", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Adapter: Browserless (CDP / REST)
# ─────────────────────────────────────────────────────────────────────────────

async def _scrape_browserless(
    url: str,
    endpoint: str,
    token: str | None,
    timeout: int,
) -> ScrapeResult:
    """
    Uses Browserless /content endpoint to fetch rendered HTML.
    Endpoint:  POST {browserless_endpoint}/content?token={token}
    Body:     {"url": url, "waitFor": 2000}
    """
    content_url = endpoint.rstrip("/") + "/content"
    params: dict[str, str] = {}
    if token:
        params["token"] = token

    payload = {"url": url, "waitFor": 2000, "rejectResourceTypes": ["image", "font"]}

    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(content_url, params=params, json=payload)
            resp.raise_for_status()
            rendered_html = resp.text
            content = _extract_text(rendered_html, url)
            return ScrapeResult(
                success=bool(content),
                content=content,
                url=url,
                strategy="browserless",
                status_code=resp.status_code,
            )
    except Exception as e:
        return ScrapeResult(success=False, content="", url=url, strategy="browserless", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Adapter: Wayback Machine
# ─────────────────────────────────────────────────────────────────────────────

async def _scrape_wayback(url: str, timeout: int) -> ScrapeResult:
    """Fetch latest snapshot from Wayback CDX API."""
    cdx_url = (
        f"http://archive.org/wayback/available?url={url}"
    )
    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout) as client:
            cdx_resp = await client.get(cdx_url)
            cdx_resp.raise_for_status()
            data = cdx_resp.json()
            snapshot = (data.get("archived_snapshots") or {}).get("closest", {})
            if not snapshot.get("available"):
                return ScrapeResult(success=False, content="", url=url, strategy="wayback", error="no snapshot")
            wayback_url = snapshot["url"]
            return await _scrape_httpx(wayback_url, timeout)
    except Exception as e:
        return ScrapeResult(success=False, content="", url=url, strategy="wayback", error=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator (matches ScrapeOrchestratorV5 interface)
# ─────────────────────────────────────────────────────────────────────────────

class MultiStrategyScraper:
    """
    Standalone replacement for deepsearch ScrapeOrchestratorV5.

    Drop-in interface:
        scraper = MultiStrategyScraper(cfg)
        result = await scraper.scrape_url(url)
        result.success, result.content, result.text
    """

    def __init__(self, cfg: ScraperConfig | None = None):
        self.cfg = cfg or ScraperConfig()
        self._cache = _UrlCache(ttl_sec=self.cfg.cache_ttl_sec)

    async def scrape_url(self, url: str) -> ScrapeResult:
        """Try adapters in chain order. Return first adequate result."""
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ScrapeResult(success=False, content="", url=url, strategy="none", error="invalid url")

        # Cache hit
        cached = self._cache.get(url)
        if cached:
            logger.debug(f"[Scraper] Cache hit: {url}")
            return ScrapeResult(success=True, content=cached, url=url, strategy="cache")

        def _adequate(result: ScrapeResult) -> bool:
            return result.success and len(result.content.split()) >= self.cfg.min_content_words

        result = ScrapeResult(success=False, content="", url=url, strategy="none")

        # 1. httpx + trafilatura
        if self.cfg.enable_httpx:
            result = await _scrape_httpx(url, self.cfg.request_timeout)
            if _adequate(result):
                self._cache.set(url, result.content)
                logger.info(f"[Scraper] httpx OK: {url} ({len(result.content.split())} words)")
                return result
            logger.debug(f"[Scraper] httpx insufficient ({result.error or result.status_code}): {url}")

        # 2. Jina
        if self.cfg.enable_jina:
            result = await _scrape_jina(url, self.cfg.jina_api_key, self.cfg.jina_timeout)
            if _adequate(result):
                self._cache.set(url, result.content)
                logger.info(f"[Scraper] Jina OK: {url}")
                return result
            logger.debug(f"[Scraper] Jina insufficient ({result.error}): {url}")

        # 3. Browserless
        if self.cfg.enable_browserless and self.cfg.browserless_endpoint:
            result = await _scrape_browserless(
                url,
                self.cfg.browserless_endpoint,
                self.cfg.browserless_token,
                self.cfg.browserless_timeout,
            )
            if _adequate(result):
                self._cache.set(url, result.content)
                logger.info(f"[Scraper] Browserless OK: {url}")
                return result
            logger.debug(f"[Scraper] Browserless insufficient ({result.error}): {url}")

        # 4. Wayback
        if self.cfg.enable_wayback:
            result = await _scrape_wayback(url, self.cfg.request_timeout)
            if _adequate(result):
                self._cache.set(url, result.content)
                logger.info(f"[Scraper] Wayback OK: {url}")
                return result

        logger.warning(f"[Scraper] All strategies failed for {url}")
        return result


# ─────────────────────────────────────────────────────────────────────────────
# Compat aliases (matches deepsearch import names used in peopledd)
# ─────────────────────────────────────────────────────────────────────────────

# These aliases allow ri_scraper.py / strategy_retriever.py to use
# the same class names as the deepsearch API:
ScrapeOrchestratorV5 = MultiStrategyScraper
