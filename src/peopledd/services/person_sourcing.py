from __future__ import annotations

"""
Secondary LinkedIn URL discovery when Harvest profile-search returns no candidates.

1. Exa People Search (category=people) — LinkedIn /in/ candidates with snippets.
2. Exa Company Search (category=company), same shape as exa_py: outputSchema text + highlights,
   governance-oriented query (RI, conselho, diretoria).
3. Optional LLM pick (OPENAI_API_KEY + budget): chooses one URL when multiple candidates exist,
   using company hits as disambiguation context. Set PEOPLEDd_DISABLE_EXA_PERSON_LLM=1 to skip.
"""

import asyncio
import json
import logging
import os
from typing import TYPE_CHECKING, Any

from peopledd.runtime.adaptive_models import PersonLinkedInQueryStyle, PersonSearchParams, SearchAttemptRecord
from peopledd.runtime.pipeline_context import get_attached_run_context

if TYPE_CHECKING:
    from peopledd.vendor.search import SearchOrchestrator, SearchResult

logger = logging.getLogger(__name__)

_NUM_RESULTS_PER_VARIANT = 8
_COMPANY_GOV_NUM_RESULTS = 10

_LINKEDIN_PICK_SYSTEM = """You reconcile a governance person name to one LinkedIn profile.
You receive:
- people_hits: Exa People Search results. Each item has a linkedin.com/in/ URL and career-related text.
- company_hits: Exa Company Search results (RI pages, company library entries, news). Use them only to \
disambiguate which company and roles are meant; do not invent a URL that is not listed under people_hits.

Rules:
- Pick exactly one linkedin.com/in/ URL from people_hits that best matches the person_name for the \
stated target company context.
- If none of the people_hits clearly match the named person, return linkedin_url null.
- Respond only with JSON matching the schema."""

_LINKEDIN_PICK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "linkedin_url": {"type": ["string", "null"]},
        "confidence": {"type": "number"},
        "reason": {"type": "string"},
    },
    "required": ["linkedin_url", "confidence", "reason"],
    "additionalProperties": False,
}


def _governance_company_query(company_name: str | None) -> str:
    c = (company_name or "").strip()
    if not c:
        return ""
    return (
        f"conselheiros de administração e diretoria executiva atualmente na empresa {c} "
        f"Brasil governança corporativa relações com investidores CVM"
    )


def _people_search_queries(
    person_name: str,
    company_name: str | None,
    style: PersonLinkedInQueryStyle,
    escalation_level: int,
) -> list[str]:
    c = (company_name or "").strip()
    name = (person_name or "").strip()
    if not name:
        return []

    queries: list[str] = []
    if style == "default":
        queries.append(f"{name} {c}".strip() if c else name)
        if c:
            queries.append(f"{c} {name}")
    elif style == "company_first":
        queries.append(f"{c} {name}".strip() if c else name)
        if c:
            queries.append(f"{name} {c}")
    else:
        queries.append(
            f"{name} diretor conselho administrativo {c}".strip()
            if c
            else f"{name} diretor conselho administrativo"
        )
        if c:
            queries.append(f"{name} {c}")

    if escalation_level >= 1:
        if c:
            queries.append(f"{name} board member executive {c}")
        else:
            queries.append(f"{name} executive board member")

    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        qn = q.strip()
        if qn and qn not in seen:
            seen.add(qn)
            out.append(qn)
    return out[:5]


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 3] + "..."


async def _llm_pick_linkedin_url(
    person_name: str,
    company_name: str | None,
    people_results: list[Any],
    company_hits: list[Any],
    allowed_canonical: dict[str, str],
) -> str | None | bool:
    """
    Returns:
      - str: chosen canonical URL
      - None: LLM says no match
      - True: skip / unavailable (caller uses ranked list)
    """
    from peopledd.vendor.search import _llm_json

    disable = os.environ.get("PEOPLEDd_DISABLE_EXA_PERSON_LLM", "").lower() in ("1", "true", "yes")
    if disable:
        return True

    force = os.environ.get("PEOPLEDd_EXA_PERSON_LLM", "").lower() in ("1", "true", "always")
    if len(allowed_canonical) < 2 and not force:
        return True

    ph = []
    for i, r in enumerate(people_results[:12]):
        ph.append(
            {
                "rank": i + 1,
                "url": getattr(r, "url", "") or "",
                "title": _truncate(getattr(r, "title", "") or "", 200),
                "text": _truncate(getattr(r, "snippet", "") or "", 3500),
            }
        )
    ch = []
    for i, r in enumerate(company_hits[:12]):
        ch.append(
            {
                "rank": i + 1,
                "url": getattr(r, "url", "") or "",
                "title": _truncate(getattr(r, "title", "") or "", 200),
                "text": _truncate(getattr(r, "snippet", "") or "", 3500),
            }
        )

    user = json.dumps(
        {
            "person_name": person_name,
            "target_company": (company_name or "").strip(),
            "people_hits": ph,
            "company_hits": ch,
        },
        ensure_ascii=False,
    )

    model = (
        os.environ.get("OPENAI_MODEL_MINI")
        or os.environ.get("OPENAI_MODEL")
        or "gpt-5.4-mini"
    )
    raw = await _llm_json(
        system=_LINKEDIN_PICK_SYSTEM,
        user=user,
        model=model,
        schema=_LINKEDIN_PICK_SCHEMA,
        timeout=45.0,
        budget_step="exa_person_profile_pick",
    )
    if raw is None:
        return True

    lu = raw.get("linkedin_url")
    if lu is None:
        return None
    if not isinstance(lu, str) or not lu.strip():
        return None

    from peopledd.services.harvest_adapter import _harvest_canonical_linkedin_url

    canon = _harvest_canonical_linkedin_url(lu.strip())
    if canon and canon in allowed_canonical:
        return allowed_canonical[canon]
    return True


async def linkedin_profile_urls_async(
    orchestrator: SearchOrchestrator,
    person_name: str,
    company_name: str | None,
    person_params: PersonSearchParams | None = None,
    attempt_index: int = 0,
) -> list[str]:
    pp = person_params or PersonSearchParams.default()
    qlist = _people_search_queries(
        person_name,
        company_name,
        pp.query_style,
        pp.escalation_level,
    )
    gov_q = _governance_company_query(company_name)

    people_results: list[Any] = []
    company_hits: list[Any] = []

    if orchestrator.exa.api_key and qlist:
        try:
            pt = orchestrator.exa.search_people_linkedin_async(
                qlist,
                num_results_per_query=_NUM_RESULTS_PER_VARIANT,
            )
            if gov_q:
                ct = orchestrator.exa.search_company_rich_async(
                    gov_q,
                    num_results=_COMPANY_GOV_NUM_RESULTS,
                )
                people_results, company_hits = await asyncio.gather(pt, ct)
            else:
                people_results = await pt
        except Exception as e:
            logger.warning("[person_sourcing] Exa people/company search failed: %s", e, exc_info=True)

    urls: list[str] = []
    for r in people_results:
        u = (getattr(r, "url", None) or "").strip()
        if "linkedin.com/in/" in u.lower():
            urls.append(u)

    from peopledd.services.harvest_adapter import (
        _harvest_canonical_linkedin_url,
        _is_likely_anonymized_linkedin_url,
    )

    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        c = _harvest_canonical_linkedin_url(u)
        if not c or _is_likely_anonymized_linkedin_url(c):
            continue
        if c not in seen:
            seen.add(c)
            out.append(c)

    allowed_canonical: dict[str, str] = {}
    for u in out:
        cn = _harvest_canonical_linkedin_url(u)
        if cn:
            allowed_canonical[cn] = u

    if len(out) >= 2 or (
        os.environ.get("PEOPLEDd_EXA_PERSON_LLM", "").lower() in ("1", "true", "always") and out
    ):
        pick = await _llm_pick_linkedin_url(
            person_name,
            company_name,
            people_results,
            company_hits,
            allowed_canonical,
        )
        if pick is True:
            pass
        elif pick is None:
            out = []
        elif isinstance(pick, str):
            out = [pick]

    ctx = get_attached_run_context()
    if ctx is not None:
        requested_people = _NUM_RESULTS_PER_VARIANT * max(1, len(qlist))
        requested_co = _COMPANY_GOV_NUM_RESULTS if (gov_q and orchestrator.exa.api_key) else 0
        ctx.record_search_attempt(
            SearchAttemptRecord(
                purpose="person_exa_people",
                attempt_index=attempt_index,
                escalation_level=pp.escalation_level,
                searxng_queries_used=0,
                exa_num_results_requested=requested_people + requested_co,
                exa_company_context_results_requested=requested_co,
                url_count=len(out),
                empty_pool=len(out) == 0,
                topic_excerpt=(qlist[0][:100] if qlist else (person_name or "")[:100]),
            )
        )

    return out[:5]


def linkedin_profile_urls(
    orchestrator: SearchOrchestrator | None,
    person_name: str,
    company_name: str | None,
    person_params: PersonSearchParams | None = None,
    attempt_index: int = 0,
) -> list[str]:
    if orchestrator is None:
        return []
    return asyncio.run(
        linkedin_profile_urls_async(
            orchestrator,
            person_name,
            company_name,
            person_params=person_params,
            attempt_index=attempt_index,
        )
    )


def harvest_style_results_from_urls(
    urls: list[str],
    observed_name: str,
    company: str | None,
) -> list[Any]:
    """
    Build ProfileSearchResult list so n2 can reuse the same ranking path as Harvest hits.
    """
    from peopledd.services.harvest_adapter import ProfileSearchResult

    parts = observed_name.split(None, 1)
    first = parts[0] if parts else observed_name
    last = parts[1] if len(parts) > 1 else ""
    out: list[Any] = []
    for url in urls:
        data: dict[str, Any] = {
            "linkedinUrl": url,
            "firstName": first,
            "lastName": last,
            "headline": "",
            "location": {},
            "currentPositions": [{"companyName": company}] if company else [],
        }
        out.append(ProfileSearchResult(data, observed_name, company))
    return out
