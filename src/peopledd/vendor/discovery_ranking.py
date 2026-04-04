from __future__ import annotations

"""
Discovery ranking helpers ported from deepsearch concepts (no deepsearch dependency).

Used by vendor.search: query sanitize, SearXNG/Exa interleaving, structural junk filter,
authority/recency signals blended with existing _quality_score.
"""

import logging
import re
from datetime import date, datetime, timezone
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Host substrings / netloc fragments — structurally non-useful for corporate research
_JUNK_HOST_MARKERS = frozenset(
    {
        "play.google.com",
        "apps.apple.com",
        "itunes.apple.com",
        "booking.com",
        "expedia.com",
        "airbnb.com",
        "hotels.com",
        "ticketmaster.com",
        "stubhub.com",
    }
)

_MEDIA_OR_BINARY = re.compile(
    r"\.(jpg|jpeg|png|gif|svg|webp|mp4|mp3|wav|zip|rar|7z|tar|gz|exe|dmg)(\?|$|#)",
    re.IGNORECASE,
)

_TECH_PATH = re.compile(
    r"/(api|cdn|static|assets|cache)(/|$)",
    re.IGNORECASE,
)


def sanitize_search_query(q: str) -> str:
    """
    Strip planner noise before HTTP search: list numbering, bullets, parenthetical notes.
    Preserves double-quoted phrases of multiple words for exact-match operators.
    """
    if not q:
        return ""
    query = q.strip()
    query = re.sub(r"^\d+\.\s*", "", query)
    query = re.sub(r"^\([^)]*\)\s*", "", query)
    query = re.sub(r"\s*\([^)]*\)$", "", query)
    query = re.sub(r"^[\*\-\u2022]\s*", "", query)

    def _quotes(m: re.Match[str]) -> str:
        content = m.group(1).strip()
        if " " in content:
            return f'"{content}"'
        return content

    query = re.sub(r'"([^"]*)"', _quotes, query)
    query = re.sub(r"\s+", " ", query).strip()
    return query


def is_structurally_junk(url: str) -> bool:
    """True for app stores, travel/ticket hosts, media URLs, and bare API/static paths."""
    if not url or not url.startswith("http"):
        return True
    try:
        parsed = urlparse(url)
    except Exception:
        return True
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    for marker in _JUNK_HOST_MARKERS:
        if marker in host:
            return True
    if _MEDIA_OR_BINARY.search(path) or _MEDIA_OR_BINARY.search(url):
        return True
    if _TECH_PATH.search(path):
        return True
    return False


def infer_date_guess(
    url: str,
    title: str = "",
    snippet: str = "",
    published_date: str = "",
) -> str | None:
    """
    Best-effort publication date YYYY-MM-DD from URL path, title, snippet, or ISO field.
    """
    if published_date:
        s = published_date.strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            try:
                y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
                dt = date(y, m, d)
                if 1990 <= dt.year <= datetime.now().year + 1:
                    return dt.isoformat()
            except ValueError:
                pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            d_only = dt.date()
            if 1990 <= d_only.year <= datetime.now().year + 1:
                return d_only.isoformat()
        except (ValueError, TypeError):
            pass

    url_patterns = [
        r"/(\d{4})/(\d{2})/(\d{2})/",
        r"/(\d{4})-(\d{2})-(\d{2})",
        r"_(\d{4})(\d{2})(\d{2})",
        r"/(\d{4})/(\d{2})/",
    ]
    for pattern in url_patterns:
        match = re.search(pattern, url)
        if not match:
            continue
        groups = match.groups()
        try:
            if len(groups) == 3:
                y, mo, d = int(groups[0]), int(groups[1]), int(groups[2])
            else:
                y, mo = int(groups[0]), int(groups[1])
                d = 1
            dt = date(y, mo, d)
            if 1990 <= dt.year <= datetime.now().year + 1:
                return dt.isoformat()
        except (ValueError, OverflowError):
            continue

    month_pt = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    month_en = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    def _parse_title_block(text: str) -> str | None:
        pt_pat = re.compile(
            r"(\d{1,2})\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*\.?\s+(\d{4})",
            re.I,
        )
        m = pt_pat.search(text)
        if m:
            d_s, m_s, y_s = m.groups()
            try:
                mo = month_pt[m_s.lower()[:3]]
                dt = date(int(y_s), mo, int(d_s))
                if 1990 <= dt.year <= datetime.now().year + 1:
                    return dt.isoformat()
            except (ValueError, KeyError):
                pass
        slash = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
        if slash:
            d_s, mo_s, y_s = slash.groups()
            try:
                dt = date(int(y_s), int(mo_s), int(d_s))
                if 1990 <= dt.year <= datetime.now().year + 1:
                    return dt.isoformat()
            except ValueError:
                pass
        en_pat = re.compile(
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})",
            re.I,
        )
        m = en_pat.search(text)
        if m:
            m_s, d_s, y_s = m.groups()
            try:
                mo = month_en[m_s.lower()[:3]]
                dt = date(int(y_s), mo, int(d_s))
                if 1990 <= dt.year <= datetime.now().year + 1:
                    return dt.isoformat()
            except (ValueError, KeyError):
                pass
        return None

    got = _parse_title_block(title)
    if got:
        return got
    return _parse_title_block(snippet[:200])


def authority_score(url: str, title: str = "", snippet: str = "") -> float:
    """
    Heuristic authority 0.0-1.0: gov/edu, LinkedIn, tier-1 news (BR + intl).
    """
    if not url:
        return 0.5
    domain = urlparse(url).netloc.lower()
    if any(t in domain for t in (".gov", ".gov.br", ".edu")):
        return 0.95
    if "linkedin.com" in domain:
        return 0.85
    top_news = ("reuters", "bloomberg", "ft.com", "wsj", "nytimes", "forbes")
    if any(n in domain for n in top_news):
        return 0.85
    br_news = ("g1.globo", "estadao", "folha", "valor", "exame")
    if any(n in domain for n in br_news):
        return 0.80
    if "cvm.gov.br" in domain:
        return 0.95
    return 0.50


def recency_score_from_date_guess(iso_date: str | None) -> float:
    """0.0-1.0 from infer_date_guess output; 0.5 if unknown."""
    if not iso_date:
        return 0.5
    try:
        y, m, d = int(iso_date[:4]), int(iso_date[5:7]), int(iso_date[8:10])
        pub = date(y, m, d)
    except (ValueError, IndexError):
        return 0.5
    today = date.today()
    days_old = (today - pub).days
    if days_old < 0:
        return 0.55
    if days_old <= 30:
        return 1.0
    if days_old <= 180:
        return 0.9 - (days_old - 30) * 0.2 / 150
    if days_old <= 365:
        return 0.7 - (days_old - 180) * 0.2 / 185
    return max(0.3, 0.5 - (days_old - 365) * 0.2 / 365)


def blend_pre_rank_score(
    *,
    base_quality: float,
    authority: float,
    recency: float,
    authority_weight: float = 0.12,
    recency_weight: float = 0.08,
) -> float:
    """
    Combine existing vendor _quality_score (engine boosts, prefer_domains, spam) with
    modest authority/recency so Exa/SearXNG ordering is not drowned out.
    Rejected candidates (base_quality < 0) stay negative.
    """
    if base_quality < 0:
        return base_quality
    return base_quality + authority_weight * authority + recency_weight * recency


def interleave_by_source(
    results: list["SearchResult"],
    *,
    order: tuple[str, ...] = ("searxng", "exa"),
) -> list["SearchResult"]:
    """
    Zipper merge by source label, each group sorted by score desc.
    First occurrence of a URL (normalized netloc+path lower) wins.
    """
    if not results:
        return []

    def _key(r: "SearchResult") -> str:
        try:
            p = urlparse(r.url)
            return f"{p.netloc.lower()}{p.path.lower()}"
        except Exception:
            return (r.url or "").lower()

    buckets: dict[str, list["SearchResult"]] = {src: [] for src in order}
    other: list["SearchResult"] = []
    for r in results:
        src = (r.source or "").lower() or "other"
        if src in buckets:
            buckets[src].append(r)
        else:
            other.append(r)

    for src in order:
        buckets[src].sort(key=lambda x: x.score, reverse=True)

    other.sort(key=lambda x: x.score, reverse=True)

    seen: set[str] = set()
    out: list["SearchResult"] = []

    iterators = {src: iter(buckets[src]) for src in order}
    other_it = iter(other)
    max_rounds = len(results) + 5
    for _ in range(max_rounds):
        progressed = False
        for src in order:
            it = iterators[src]
            while True:
                nxt = next(it, None)
                if nxt is None:
                    break
                k = _key(nxt)
                if k in seen:
                    continue
                seen.add(k)
                out.append(nxt)
                progressed = True
                break
        while True:
            nxt = next(other_it, None)
            if nxt is None:
                break
            k = _key(nxt)
            if k in seen:
                continue
            seen.add(k)
            out.append(nxt)
            progressed = True
            break
        if not progressed:
            break

    return out


def filter_structural_junk_results(results: list["SearchResult"]) -> list["SearchResult"]:
    """Drop structurally junk URLs; logs at debug."""
    kept: list["SearchResult"] = []
    for r in results:
        if is_structurally_junk(r.url):
            logger.debug("[discovery_ranking] drop junk url: %s", r.url[:80])
            continue
        kept.append(r)
    return kept
