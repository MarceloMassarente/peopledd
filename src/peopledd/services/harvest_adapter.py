from __future__ import annotations

"""
HarvestAdapter — desacoplado de OpenWebUI/valves/event_emitter.

Porta as funções essenciais de harvest_linkedin_profiles_tool.py:
  - _harvest_canonical_linkedin_url()
  - _is_likely_anonymized_linkedin_url()
  - _harvest_compact_profile()
  - _harvest_build_work_history_summary()
  - search_by_name()     → GET /linkedin/profile-search
  - get_profile()        → GET /linkedin/profile

Dedup de homônimos:
  Usa _shingles() + _jaccard() (mesma lógica de n1b) para eliminar perfis
  claramente divergentes pelo nome retornado pelo Harvest.

Retry 429:
  Backoff exponencial com base 2.0s, até MAX_RETRIES_429 tentativas.

Cache in-process:
  vendor.document_store.InMemoryDocumentStore (TTL 1h) para perfis já buscados.
"""

import asyncio
import logging
import os
import re
import unicodedata
from typing import Any
from urllib.parse import unquote

import httpx

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Harvest API constants (portados do harvest_linkedin_profiles_tool.py)
# ─────────────────────────────────────────────────────────────────────────────

HARVEST_BASE_URL = "https://api.harvest-api.com"
PROFILE_URL = f"{HARVEST_BASE_URL}/linkedin/profile"
PROFILE_SEARCH_URL = f"{HARVEST_BASE_URL}/linkedin/profile-search"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
DEFAULT_TIMEOUT = 15.0
MAX_RETRIES_429 = 3
RETRY_BACKOFF_BASE = 2.0

# Fuzzy match threshold for homonym dedup (same as n1b)
FUZZY_MATCH_THRESHOLD = 0.55

# ─────────────────────────────────────────────────────────────────────────────
# URL utilities (ported from harvest_linkedin_profiles_tool.py)
# ─────────────────────────────────────────────────────────────────────────────

def _harvest_canonical_linkedin_url(url: str) -> str:
    """Canonical LinkedIn URL: unquote + ASCII slug (no accents)."""
    if not url or "linkedin.com/in/" not in url:
        return (url or "").strip()
    raw = (url.strip().split("?")[0].rstrip("/") or "").strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    try:
        raw = unquote(raw)
    except Exception:
        pass
    match = re.search(r"linkedin\.com/in/([^/?#]+)", raw, re.IGNORECASE)
    if not match:
        return raw
    slug = (match.group(1) or "").strip()
    if not slug:
        return raw
    slug_nfd = unicodedata.normalize("NFKD", slug.lower())
    slug_ascii = slug_nfd.encode("ascii", "ignore").decode("ascii")
    slug_clean = re.sub(r"[^a-z0-9\-_]", "", slug_ascii)
    slug_clean = re.sub(r"-+", "-", slug_clean).strip("-")
    return ("https://www.linkedin.com/in/" + slug_clean) if slug_clean else raw


def _is_likely_anonymized_linkedin_url(url: str) -> bool:
    """True if slug looks like a LinkedIn-anonymized ID (starts with acwaa or long hex)."""
    if not url or "linkedin.com/in/" not in url:
        return False
    match = re.search(r"linkedin\.com/in/([^/?#]+)", url, re.IGNORECASE)
    if not match:
        return False
    slug = re.sub(r"[^a-z0-9\-]", "", (match.group(1) or "").strip().lower())
    if slug.startswith("acwaa"):
        return True
    if "-" not in slug and len(slug) >= 18 and re.match(r"^[a-z0-9]{18,}$", slug):
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Profile compaction (ported from harvest_linkedin_profiles_tool.py)
# ─────────────────────────────────────────────────────────────────────────────

def _harvest_compact_profile(element: dict[str, Any]) -> dict[str, Any]:
    """Reduces full Harvest profile to a compact representation."""
    first = element.get("firstName") or ""
    last = element.get("lastName") or ""
    pub_id = (element.get("publicIdentifier") or "").strip()
    name = ("%s %s" % (first, last)).strip() or pub_id or ""
    headline = element.get("headline") or ""
    linkedin_url_raw = element.get("linkedinUrl") or ""
    linkedin_url = (
        _harvest_canonical_linkedin_url(linkedin_url_raw)
        if linkedin_url_raw and "linkedin.com/in/" in linkedin_url_raw
        else linkedin_url_raw
    )
    loc = element.get("location") or {}
    location_text = loc.get("linkedinText") or (loc.get("parsed") or {}).get("text") or ""

    current = element.get("currentPositions") or element.get("currentPosition") or []
    if isinstance(current, dict):
        current = [current]
    elif not isinstance(current, list):
        current = []
    current_pos = (current[0].get("companyName") if current and isinstance(current[0], dict) else None) or ""

    exp = element.get("experience") or []
    exp_summary = []
    for e in exp[:6]:
        pos = e.get("position") or ""
        company = e.get("companyName") or ""
        if pos or company:
            exp_summary.append({
                "position": pos,
                "company": company,
                "duration": e.get("duration"),
                "is_current": e.get("isCurrent", False),
                "description": (e.get("description") or "")[:300],
            })

    edu = element.get("education") or []
    edu_summary = [
        {"title": e.get("title"), "degree": e.get("degree"), "school": e.get("schoolName")}
        for e in edu[:3]
    ]

    return {
        "name": name,
        "headline": headline,
        "linkedinUrl": linkedin_url,
        "publicIdentifier": pub_id,
        "location": location_text,
        "currentPosition": current_pos,
        "experience": exp_summary,
        "education": edu_summary,
        "about": (element.get("about") or "")[:600],
        "topSkills": element.get("topSkills") or "",
        "connections": element.get("connectionsCount"),
    }


def _harvest_build_work_history_summary(
    experience: list[dict] | None, max_entries: int = 6
) -> list[dict[str, Any]]:
    """Converts Harvest experience[] to structured work history."""
    if not experience or not isinstance(experience, list):
        return []
    out = []
    for i, e in enumerate(experience[:max(1, max_entries)]):
        if not isinstance(e, dict):
            continue
        pos = (e.get("position") or e.get("title") or "").strip()
        company = (e.get("companyName") or e.get("company") or "").strip()
        dur = (e.get("duration") or "").strip()
        end_date = e.get("endDate")
        is_current = (
            e.get("isCurrent") is True
            or (isinstance(end_date, dict) and (end_date.get("text") or "").strip() == "Present")
            or end_date is None
        )
        desc_raw = (e.get("description") or "").strip()
        desc_limit = 400 if i < 2 else 150
        entry: dict[str, Any] = {
            "title": pos or None,
            "company": company or None,
            "tenure": dur or None,
            "is_current": is_current,
        }
        if desc_raw:
            entry["description"] = desc_raw[:desc_limit]
        out.append(entry)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy name matching (same as n1b — no external dep)
# ─────────────────────────────────────────────────────────────────────────────

_NAME_STOPWORDS = {"de", "da", "do", "dos", "das", "e", "a", "o", "the", "of"}


def _shingles(s: str, n: int = 2) -> set[tuple[str, ...]]:
    tokens = [t for t in s.lower().split() if t not in _NAME_STOPWORDS and len(t) > 1]
    return set(tuple(tokens[i: i + n]) for i in range(max(0, len(tokens) - n + 1)))


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union > 0 else 0.0


def _name_similarity(observed_name: str, harvest_name: str) -> float:
    """Fuzzy similarity for homonym disambiguation (bigrams, lower threshold)."""
    sh_a = _shingles(observed_name, n=2)
    sh_b = _shingles(harvest_name, n=2)
    score = _jaccard(sh_a, sh_b)
    # Unigram fallback for very short names
    if score == 0.0:
        t_a = set(observed_name.lower().split()) - _NAME_STOPWORDS
        t_b = set(harvest_name.lower().split()) - _NAME_STOPWORDS
        score = _jaccard(t_a, t_b)
    return score


# ─────────────────────────────────────────────────────────────────────────────
# Cache (standalone vendor.document_store)
# ─────────────────────────────────────────────────────────────────────────────

from peopledd.vendor.document_store import InMemoryDocumentStore as _InMemoryDocumentStore

_cache_store = None


class _MinimalValves:
    DOC_STORE_MAX_SIZE = 500
    DOC_STORE_EVICTION_POLICY = "lru"
    DOC_STORE_DEFAULT_TTL_SECONDS = 3600  # 1h cache for Harvest profiles


def _get_cache():
    global _cache_store
    if _cache_store is not None:
        return _cache_store
    _cache_store = _InMemoryDocumentStore(valves=_MinimalValves())
    logger.info("[HarvestAdapter] InMemoryDocumentStore (vendor) cache initialized")
    return _cache_store


# ─────────────────────────────────────────────────────────────────────────────
# HTTP layer with retry-on-429
# ─────────────────────────────────────────────────────────────────────────────

async def _harvest_get(
    url: str,
    params: dict[str, str],
    api_key: str,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any] | None:
    """Async GET to Harvest API with exponential backoff on 429."""
    headers = {**DEFAULT_HEADERS, "X-API-Key": api_key}
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(MAX_RETRIES_429):
            try:
                resp = await client.get(url, params=params, headers=headers)
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    logger.warning(f"[HarvestAdapter] 429 rate limit — waiting {wait}s (attempt {attempt+1})")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.json()
            except httpx.TimeoutException:
                logger.warning(f"[HarvestAdapter] Timeout on attempt {attempt+1} for {url}")
                if attempt == MAX_RETRIES_429 - 1:
                    return None
                await asyncio.sleep(RETRY_BACKOFF_BASE ** attempt)
            except Exception as e:
                logger.error(f"[HarvestAdapter] Error: {e}")
                return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Profile search result model
# ─────────────────────────────────────────────────────────────────────────────

class ProfileSearchResult:
    """Lightweight result from Harvest profile-search."""

    def __init__(self, data: dict[str, Any], observed_name: str, company: str | None):
        self.linkedin_url: str = _harvest_canonical_linkedin_url(data.get("linkedinUrl") or "")
        self.name: str = (
            f"{data.get('firstName', '')} {data.get('lastName', '')}".strip()
            or data.get("publicIdentifier", "")
        )
        self.headline: str = data.get("headline") or ""
        self.location: str = (data.get("location") or {}).get("linkedinText") or ""
        self.current_company: str = (
            (data.get("currentPositions") or [{}])[0].get("companyName", "")
            if data.get("currentPositions")
            else ""
        )
        self.is_anonymized: bool = _is_likely_anonymized_linkedin_url(self.linkedin_url)
        self.name_similarity: float = (
            _name_similarity(observed_name, self.name) if self.name and observed_name else 0.0
        )
        # Company match bonus
        self.company_match: bool = bool(
            company
            and self.current_company
            and company.lower()[:12] in self.current_company.lower()
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main adapter
# ─────────────────────────────────────────────────────────────────────────────

class HarvestAdapter:
    """
    Decoupled Harvest API adapter for peopledd.

    Replaces HarvestConnector stub in connectors.py.
    No dependency on valves, event_emitter, or OpenWebUI internals.
    """

    def __init__(
        self,
        api_key: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        default_location: str = "Brazil",
    ):
        self.api_key = api_key or os.environ.get("HARVEST_API_KEY", "")
        self.timeout = timeout
        self.default_location = default_location
        self._cache = _get_cache()

    def _cache_get(self, key: str) -> dict | None:
        if self._cache is None:
            return None
        try:
            results = self._cache.get_documents(
                user_id="harvest_adapter",
                session_id=key,
            )
            if results:
                content = results[0].get("content") if isinstance(results[0], dict) else None
                if content:
                    import json
                    return json.loads(content)
        except Exception:
            pass
        return None

    def _cache_set(self, key: str, value: dict) -> None:
        if self._cache is None:
            return
        try:
            import json
            self._cache.add_documents(
                user_id="harvest_adapter",
                session_id=key,
                documents=[{"content": json.dumps(value), "meta": {"cache_key": key}}],
                stage="profile",
            )
        except Exception:
            pass

    async def search_by_name_async(
        self,
        name: str,
        company: str | None = None,
        location: str | None = None,
        page: int = 1,
    ) -> list[ProfileSearchResult]:
        """
        Search LinkedIn profiles by name via Harvest profile-search.
        Filters out anonymized profiles and low similarity homonyms.
        """
        if not self.api_key:
            logger.warning("[HarvestAdapter] No API key — skipping profile search")
            return []

        cache_key = f"search:{name}:{company}:{location}:{page}"
        cached = self._cache_get(cache_key)
        if cached:
            logger.debug(f"[HarvestAdapter] Cache hit for search: {name}")
            return [
                ProfileSearchResult(r, name, company) for r in cached.get("elements", [])
            ]

        params: dict[str, str] = {
            "search": name,
            "location": location or self.default_location,
            "page": str(page),
        }
        if company:
            params["current_company"] = company

        data = await _harvest_get(PROFILE_SEARCH_URL, params, self.api_key, self.timeout)
        if not data:
            return []

        self._cache_set(cache_key, data)

        elements = data.get("elements") or data.get("results") or []
        results = [ProfileSearchResult(e, name, company) for e in elements]

        # Filter: remove anonymized and very low similarity
        filtered = [
            r for r in results
            if not r.is_anonymized and r.name_similarity >= FUZZY_MATCH_THRESHOLD
        ]

        # Sort: company match first, then by name similarity
        filtered.sort(key=lambda r: (r.company_match, r.name_similarity), reverse=True)

        logger.info(
            f"[HarvestAdapter] search '{name}': {len(elements)} raw → "
            f"{len(filtered)} after dedup filter"
        )
        return filtered

    def search_by_name(
        self, name: str, company: str | None = None, location: str | None = None
    ) -> list[ProfileSearchResult]:
        """Sync wrapper."""
        return asyncio.run(self.search_by_name_async(name, company, location))

    async def get_profile_async(self, linkedin_url: str) -> dict[str, Any] | None:
        """
        Fetch full profile from Harvest /linkedin/profile.
        Returns compact profile dict or None if not found.
        """
        if not self.api_key:
            logger.warning("[HarvestAdapter] No API key — skipping get_profile")
            return None

        canonical = _harvest_canonical_linkedin_url(linkedin_url)
        if _is_likely_anonymized_linkedin_url(canonical):
            logger.info(f"[HarvestAdapter] Skipping anonymized profile: {canonical}")
            return None

        cache_key = f"profile:{canonical}"
        cached = self._cache_get(cache_key)
        if cached:
            logger.debug(f"[HarvestAdapter] Cache hit for profile: {canonical}")
            return cached

        params = {"url": canonical, "main": "true", "includeAboutProfile": "true"}
        data = await _harvest_get(PROFILE_URL, params, self.api_key, self.timeout)
        if not data:
            return None

        compact = _harvest_compact_profile(data)
        self._cache_set(cache_key, compact)
        return compact

    def get_profile(self, linkedin_url: str) -> dict[str, Any] | None:
        """Sync wrapper."""
        return asyncio.run(self.get_profile_async(linkedin_url))

    def compute_profile_quality(
        self, compact_profile: dict[str, Any] | None
    ) -> dict[str, float]:
        """
        Compute ProfileQuality metrics from a compact Harvest profile.
        Returns: dict with useful_coverage_score, evidence_density, recency_score, profile_confidence.
        """
        if not compact_profile:
            return {
                "useful_coverage_score": 0.0,
                "evidence_density": 0.0,
                "recency_score": 0.0,
                "profile_confidence": 0.1,
            }

        exp = compact_profile.get("experience") or []
        edu = compact_profile.get("education") or []
        about = compact_profile.get("about") or ""
        headline = compact_profile.get("headline") or ""

        # Useful coverage: has experience + education + about
        coverage = 0.0
        if exp:
            coverage += 0.5 + min(0.2, len(exp) * 0.04)  # up to 0.7 for 5+ roles
        if edu:
            coverage += 0.1
        if about and len(about) > 100:
            coverage += 0.1
        coverage = min(1.0, coverage)

        # Evidence density: proportion of experience entries with meaningful descriptions
        described = sum(1 for e in exp if e.get("description") and len(e["description"]) > 50)
        evidence_density = (described / len(exp)) if exp else 0.0

        # Recency: latest role is current
        recency = 0.0
        if exp:
            is_current = exp[0].get("is_current", False)
            recency = 0.9 if is_current else 0.4

        # Overall confidence
        confidence = round(
            0.4 * coverage + 0.3 * evidence_density + 0.3 * recency,
            2,
        )

        return {
            "useful_coverage_score": round(coverage, 2),
            "evidence_density": round(evidence_density, 2),
            "recency_score": round(recency, 2),
            "profile_confidence": confidence,
        }

    def build_career_summary(self, compact_profile: dict[str, Any] | None) -> dict[str, list[str]]:
        """
        Build career_summary dict from compact Harvest profile.
        Keys: current_roles, prior_roles, functional_experience, industry_experience, governance_signals.
        """
        if not compact_profile:
            return {
                "current_roles": [],
                "prior_roles": [],
                "functional_experience": [],
                "industry_experience": [],
                "governance_signals": [],
            }

        exp = compact_profile.get("experience") or []
        current_roles = []
        prior_roles = []

        for e in exp:
            title = e.get("position") or ""
            company = e.get("company") or ""
            entry = f"{title} @ {company}" if title and company else title or company
            if e.get("is_current"):
                current_roles.append(entry)
            else:
                prior_roles.append(entry)

        # Infer functional experience from titles
        func_map = {
            "financial": ["cfo", "financ", "tesour", "controller", "invest"],
            "technology": ["cto", "cio", "tech", "digital", "data", "ti", "it"],
            "operations": ["coo", "operat", "supply", "manufactur", "industr"],
            "people_hr": ["chro", "people", "human resource", "rh", "talent"],
            "legal_compliance": ["legal", "jurídic", "compli", "regulat", "counsel"],
            "commercial_marketing": ["cmo", "comerci", "marketing", "sales", "revenue"],
            "strategy": ["strateg", "planeja", "m&a", "fusão", "strategy"],
            "general_management": ["ceo", "diretor presidente", "president", "coo", "gm"],
        }
        all_titles = " ".join(
            e.get("position", "").lower() for e in exp
        )
        functional = [func for func, terms in func_map.items() if any(t in all_titles for t in terms)]

        # Governance signals
        gov_signals = []
        if any("conselho" in (e.get("position") or "").lower() or "board" in (e.get("position") or "").lower() for e in exp):
            gov_signals.append("board_experience")
        if any("audit" in (e.get("position") or "").lower() for e in exp):
            gov_signals.append("audit_committee")
        if len(exp) >= 3 and any(e.get("is_current") for e in exp):
            gov_signals.append("executive_track")

        return {
            "current_roles": current_roles[:3],
            "prior_roles": prior_roles[:5],
            "functional_experience": functional,
            "industry_experience": [],  # would need sector inference from company names
            "governance_signals": gov_signals,
        }
