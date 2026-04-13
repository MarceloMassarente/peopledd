from __future__ import annotations

import re
from urllib.parse import urljoin

_GOVERNANCE_ANCHOR_RE = re.compile(
    r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

_LINK_TEXT_HINT_RE = re.compile(
    r"\b(governança|diretoria|conselho|liderança|administração|quem somos|corporate governance|board)\b",
    re.IGNORECASE,
)

_STATIC_SUFFIXES = [
    "/governanca-corporativa/estrutura-de-governanca",
    "/governanca/conselho-de-administracao",
    "/pt/governanca",
    "/governance",
    "/pt/quem-somos/governanca",
    "/ri/governanca",
    "/relacoes-com-investidores/governanca",
]


def discover_ri_surfaces(
    base_url: str,
    raw_html: str | None,
    *,
    max_links: int = 16,
    preferred_urls: list[str] | None = None,
) -> list[str]:
    """
    Build an ordered list of RI URLs to try: memory hints first, then base URL, then
    in-page governance links, then common static path suffixes on the same host.
    """
    seen: set[str] = set()
    out: list[str] = []

    def add(u: str) -> None:
        u = u.strip()
        if not u or u in seen:
            return
        if not u.startswith("http"):
            return
        seen.add(u)
        out.append(u)

    if preferred_urls:
        for u in preferred_urls:
            add(u)

    add(base_url)

    if raw_html:
        for match in _GOVERNANCE_ANCHOR_RE.finditer(raw_html):
            if len(out) >= max_links:
                break
            href = match.group(1).strip()
            raw_text = match.group(2)
            text = re.sub(r"<[^>]+>", " ", raw_text)
            if not _LINK_TEXT_HINT_RE.search(text):
                continue
            candidate = urljoin(base_url, href)
            if candidate == base_url:
                continue
            add(candidate)

    base_path = base_url.rstrip("/")
    for suffix in _STATIC_SUFFIXES:
        if len(out) >= max_links:
            break
        if base_url.rstrip("/").endswith(suffix):
            continue
        add(base_path + suffix)

    return out[:max_links]
