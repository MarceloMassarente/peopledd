from __future__ import annotations

"""
peopledd.vendor.search
======================
Standalone search + intelligent URL selection — ported/simplified from toolsearchscrape.py.

Architecture (same as toolsearchscrape):
    LLM Planner → [SearXNG + Exa] → LLM Selector → ranked URL list

Components:
  - SearchPlanner: gpt-5.4-mini generates focused queries for a company/topic
  - SearXNGProvider: queries a local SearXNG instance (optional)
  - ExaProvider: queries Exa.ai semantic search (optional)
  - URLSelector: gpt-5.4-mini curates and ranks the raw URL pool

All components are optional — if no providers are configured, returns empty.
Used by: strategy_retriever.py, ri_scraper.py

Environment variables:
  SEARXNG_URL          SearXNG base URL (e.g. http://localhost:8080)
  EXA_API_KEY          Exa.ai API key
  OPENAI_API_KEY       For planner + selector LLM calls
"""

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SearchResult:
    url: str
    title: str = ""
    snippet: str = ""
    score: float = 0.0
    source: str = ""          # "searxng" | "exa" | "fallback"
    published_date: str = ""


@dataclass
class PlannerOutput:
    web_queries: list[str]           # 1-3 queries for SearXNG/web search
    exa_query: str                    # semantic query for Exa
    preferred_domains: list[str] = field(default_factory=list)  # e.g. ["ri.empresa.com"]
    avoid_terms: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# LLM helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _llm_json(
    system: str,
    user: str,
    model: str = "gpt-5.4-mini",
    schema: dict | None = None,
    timeout: float = 30.0,
) -> dict | None:
    """Call OpenAI with optional json_schema enforcement. Returns parsed dict or None."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return None
    try:
        import httpx
        payload: dict[str, Any] = {
            "model": model,
            "temperature": 0.1,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        }
        if schema:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "output", "strict": True, "schema": schema},
            }
        else:
            payload["response_format"] = {"type": "json_object"}

        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"] or "{}"
            return json.loads(content)
    except Exception as e:
        logger.warning(f"[Search] LLM call failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# SearchPlanner — generate queries from company/topic context
# ─────────────────────────────────────────────────────────────────────────────

_PLANNER_SYSTEM = """
Você é um especialista em pesquisa de informações corporativas brasileiras.
Dado contexto sobre uma empresa, gere queries otimizadas para descobrir páginas relevantes.

OBJETIVO: encontrar páginas de RI, relatórios de estratégia, comunicados, fatos relevantes.

RETORNE JSON:
{
  "web_queries": ["query 1", "query 2"],
  "exa_query": "semantic query em inglês ou português",
  "preferred_domains": ["ri.empresa.com.br"],
  "avoid_terms": ["emprego", "vagas"]
}

REGRAS:
- web_queries: 1-3 queries específicas. Inclua nome da empresa + termos chave.
- exa_query: uma query longa e semântica para Exa.ai.
- preferred_domains: domínios RI conhecidos com base no nome da empresa.
- Foque em fontes primárias: RI, CVM, releases, relatórios anuais.
""".strip()

_PLANNER_SCHEMA = {
    "type": "object",
    "properties": {
        "web_queries": {"type": "array", "items": {"type": "string"}},
        "exa_query": {"type": "string"},
        "preferred_domains": {"type": "array", "items": {"type": "string"}},
        "avoid_terms": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["web_queries", "exa_query", "preferred_domains", "avoid_terms"],
    "additionalProperties": False,
}


class SearchPlanner:
    """
    Generates smart queries for a company/topic.
    Falls back to rule-based query generation if LLM unavailable.
    """

    def __init__(self, model: str = "gpt-5.4-mini"):
        self.model = model

    async def plan_async(
        self,
        company_name: str,
        topic: str = "estratégia governança",
        ri_url: str | None = None,
        sector: str | None = None,
    ) -> PlannerOutput:
        ctx = (
            f"Empresa: {company_name}\n"
            f"Tópico: {topic}\n"
            f"Setor: {sector or 'desconhecido'}\n"
            f"URL RI conhecida: {ri_url or 'não disponível'}"
        )
        result = await _llm_json(
            system=_PLANNER_SYSTEM,
            user=ctx,
            model=self.model,
            schema=_PLANNER_SCHEMA,
        )
        if result:
            return PlannerOutput(
                web_queries=result.get("web_queries", []) or [f"{company_name} {topic}"],
                exa_query=result.get("exa_query", "") or f"{company_name} {topic}",
                preferred_domains=result.get("preferred_domains", []),
                avoid_terms=result.get("avoid_terms", []),
            )
        # Rule-based fallback
        return self._rule_based(company_name, topic, ri_url)

    def _rule_based(
        self, company_name: str, topic: str, ri_url: str | None
    ) -> PlannerOutput:
        slug = company_name.lower().replace(" ", "-")
        queries = [
            f'"{company_name}" {topic} site:ri.* OR filetype:pdf',
            f"{company_name} {topic} relações investidores",
        ]
        exa_query = f"{company_name} corporate strategy annual report investor relations Brazil"
        preferred = []
        if ri_url:
            preferred = [urlparse(ri_url).netloc]
        return PlannerOutput(
            web_queries=queries,
            exa_query=exa_query,
            preferred_domains=preferred,
            avoid_terms=["emprego", "vagas", "linkedin"],
        )

    def plan(self, company_name: str, topic: str = "estratégia", ri_url: str | None = None, sector: str | None = None) -> PlannerOutput:
        return asyncio.run(self.plan_async(company_name, topic, ri_url, sector))


# ─────────────────────────────────────────────────────────────────────────────
# SearXNG search provider
# ─────────────────────────────────────────────────────────────────────────────

class SearXNGProvider:
    """
    Queries a SearXNG instance.
    Set SEARXNG_URL env var (e.g. http://localhost:8080).
    """

    def __init__(self, base_url: str | None = None, timeout: int = 15):
        self.base_url = (base_url or os.environ.get("SEARXNG_URL", "")).rstrip("/")
        self.timeout = timeout

    async def search_async(
        self,
        query: str,
        num_results: int = 10,
        category: str = "general",
        language: str = "pt-BR",
    ) -> list[SearchResult]:
        if not self.base_url:
            return []
        try:
            import httpx
            params = {
                "q": query,
                "format": "json",
                "categories": category,
                "language": language,
                "safesearch": "0",
                "pageno": "1",
            }
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(
                    f"{self.base_url}/search",
                    params=params,
                    headers={"User-Agent": "peopledd/1.0"},
                )
                resp.raise_for_status()
                data = resp.json()
                results = []
                for item in (data.get("results") or [])[:num_results]:
                    results.append(SearchResult(
                        url=item.get("url", ""),
                        title=item.get("title", ""),
                        snippet=item.get("content", ""),
                        score=float(item.get("score", 0.5)),
                        source="searxng",
                        published_date=item.get("publishedDate", ""),
                    ))
                logger.info(f"[SearXNG] '{query[:50]}': {len(results)} results")
                return results
        except Exception as e:
            logger.warning(f"[SearXNG] Search failed for '{query[:50]}': {e}")
            return []

    def search(self, query: str, num_results: int = 10) -> list[SearchResult]:
        return asyncio.run(self.search_async(query, num_results))


# ─────────────────────────────────────────────────────────────────────────────
# Exa search provider
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CompanyProfile:
    """
    Structured company attributes extracted via Exa Company Search.
    Used to enrich strategy_retriever context before LLM extraction.
    """
    name: str
    website: str = ""
    ri_url: str = ""          # investor relations page if distinct
    description: str = ""
    sector: str = ""
    hq_country: str = ""
    hq_city: str = ""
    employee_range: str = ""  # e.g. "1000-5000"
    founded_year: int | None = None
    exa_score: float = 0.0


class ExaProvider:
    """
    Queries Exa.ai semantic search API.
    Set EXA_API_KEY env var.

    Two modes:
      - search_async(): general semantic search (category=auto/news/etc.)
      - company_lookup_async(): Exa Company Search — category="company", type="auto"
        Optimized for entity matching; returns structured company attributes.
        See: https://exa.ai/blog/company-search-benchmarks
    """

    SEARCH_URL = "https://api.exa.ai/search"
    ANSWER_URL = "https://api.exa.ai/answer"  # for fact extraction

    def __init__(self, api_key: str | None = None, timeout: int = 20):
        self.api_key = api_key or os.environ.get("EXA_API_KEY", "")
        self.timeout = timeout

    async def search_async(
        self,
        query: str,
        num_results: int = 10,
        include_domains: list[str] | None = None,
        category: str = "auto",
    ) -> list[SearchResult]:
        """General semantic search."""
        if not self.api_key:
            return []
        try:
            import httpx
            payload: dict[str, Any] = {
                "query": query,
                "numResults": num_results,
                "useAutoprompt": True,
                "type": "auto",
                "contents": {"text": {"maxCharacters": 500}},
            }
            if category != "auto":
                payload["category"] = category
            if include_domains:
                payload["includeDomains"] = include_domains

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    self.SEARCH_URL,
                    json=payload,
                    headers={
                        "accept": "application/json",
                        "content-type": "application/json",
                        "x-api-key": self.api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                results = []
                for item in (data.get("results") or [])[:num_results]:
                    snippet = (item.get("text") or "")[:500]
                    results.append(SearchResult(
                        url=item.get("url", ""),
                        title=item.get("title", ""),
                        snippet=snippet,
                        score=float(item.get("score", 0.5)),
                        source="exa",
                        published_date=item.get("publishedDate", ""),
                    ))
                logger.info(f"[Exa] '{query[:50]}': {len(results)} results")
                return results
        except Exception as e:
            logger.warning(f"[Exa] Search failed for '{query[:50]}': {e}")
            return []

    async def company_lookup_async(
        self,
        company_name: str,
        hq_country: str | None = None,
        sector: str | None = None,
    ) -> CompanyProfile | None:
        """
        Exa Company Search — uses category="company" + type="auto".

        Optimized for corporate entity matching vs. general retrieval
        (reference: https://exa.ai/blog/company-search-benchmarks).

        Returns structured CompanyProfile with website, description, sector.
        Returns None if API unavailable or no confident match found.
        """
        if not self.api_key:
            return None

        # Build query with structural constraints for better entity matching
        query_parts = [company_name]
        if hq_country:
            query_parts.append(f"based in {hq_country}")
        if sector:
            query_parts.append(sector)
        query = " ".join(query_parts)

        try:
            import httpx
            payload: dict[str, Any] = {
                "query": query,
                "numResults": 3,               # top-3 candidates for entity disambiguation
                "type": "auto",                # Exa's optimized routing for company queries
                "category": "company",         # Company Search mode
                "useAutoprompt": True,
                "contents": {
                    "text": {"maxCharacters": 1000},
                    "livecrawl": "never",      # cached only — fast
                },
            }

            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    self.SEARCH_URL,
                    json=payload,
                    headers={
                        "accept": "application/json",
                        "content-type": "application/json",
                        "x-api-key": self.api_key,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

            items = data.get("results") or []
            if not items:
                return None

            # Take highest scored result as primary entity match
            best = max(items, key=lambda r: float(r.get("score", 0)))
            score = float(best.get("score", 0))

            # Minimum confidence threshold to accept entity match
            if score < 0.4:
                logger.info(f"[Exa Company] Low confidence match for '{company_name}' (score={score:.2f})")
                return None

            profile = CompanyProfile(
                name=company_name,
                website=best.get("url", ""),
                description=(best.get("text") or "")[:600],
                exa_score=score,
            )

            # Heuristic: if RI / investor-relations URL appears in top results, capture it
            for item in items:
                url = item.get("url", "").lower()
                if any(t in url for t in ("ri.", "/ri", "investor", "relacoes", "relações")):
                    profile.ri_url = item["url"]
                    break

            logger.info(
                f"[Exa Company] Match for '{company_name}': {profile.website} (score={score:.2f})"
            )
            return profile

        except Exception as e:
            logger.warning(f"[Exa Company] Lookup failed for '{company_name}': {e}")
            return None

    def company_lookup(self, company_name: str, hq_country: str | None = None, sector: str | None = None) -> CompanyProfile | None:
        """Sync wrapper for company_lookup_async."""
        return asyncio.run(self.company_lookup_async(company_name, hq_country, sector))

    def search(self, query: str, num_results: int = 10) -> list[SearchResult]:
        return asyncio.run(self.search_async(query, num_results))


# ─────────────────────────────────────────────────────────────────────────────
# URL dedup + quality filter (ported from toolsearchscrape Constants)
# ─────────────────────────────────────────────────────────────────────────────

_SPAM_URL_PATTERNS = re.compile(
    r"(linkedin\.com/jobs|glassdoor|indeed\.com|emprego|vagas|"
    r"\.(jpg|png|gif|mp4|mp3|zip|exe)$|"
    r"twitter\.com|facebook\.com|instagram\.com|tiktok\.com)",
    re.IGNORECASE,
)

_BOOST_DOMAINS: dict[str, float] = {
    "cvm.gov.br": 1.5,
    "valor.com.br": 1.3,
    "infomoney.com.br": 1.2,
    "estadao.com.br": 1.2,
    "folha.uol.com.br": 1.2,
    "exame.com": 1.1,
}


def _quality_score(result: SearchResult, prefer_domains: list[str]) -> float:
    url = result.url
    if _SPAM_URL_PATTERNS.search(url):
        return -1.0  # reject
    if not url.startswith("http"):
        return -1.0

    score = result.score

    # Domain boost
    domain = urlparse(url).netloc.lower().replace("www.", "")
    for boost_domain, mult in _BOOST_DOMAINS.items():
        if boost_domain in domain:
            score *= mult
            break

    # Preferred domain bonus
    if any(pd and pd in domain for pd in prefer_domains):
        score += 0.3

    # Snippet length bonus
    if result.snippet and len(result.snippet) > 150:
        score += 0.1

    return score


def _dedup_urls(results: list[SearchResult]) -> list[SearchResult]:
    seen: set[str] = set()
    out = []
    for r in results:
        key = urlparse(r.url).netloc.lower() + urlparse(r.url).path.lower()
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# URLSelector — LLM-based semantic curation of URL pool
# ─────────────────────────────────────────────────────────────────────────────

_SELECTOR_SYSTEM = """
Você é um curador de fontes de informação corporativa.
Dado uma lista de URLs candidatas, selecione as MAIS RELEVANTES para o objetivo informado.

Critérios de seleção:
- Foco: RI (relações com investidores), relatórios anuais, estratégia, fatos relevantes, CVM
- Prioridade: fontes primárias > secundárias > terceiras
- Recência: fontes mais recentes são preferidas
- Diversidade: evite múltiplas URLs do mesmo domínio

Retorne JSON:
{"selected_indices": [0, 2, 5], "reasoning": "..."}
""".strip()

_SELECTOR_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_indices": {"type": "array", "items": {"type": "integer"}},
        "reasoning": {"type": "string"},
    },
    "required": ["selected_indices", "reasoning"],
    "additionalProperties": False,
}


class URLSelector:
    """
    Curates a list of SearchResult using gpt-5.4-mini.
    Falls back to score-based ranking if LLM unavailable.
    """

    def __init__(self, model: str = "gpt-5.4-mini", max_urls: int = 6):
        self.model = model
        self.max_urls = max_urls

    async def select_async(
        self,
        candidates: list[SearchResult],
        company_name: str,
        topic: str,
        prefer_domains: list[str] | None = None,
    ) -> list[SearchResult]:
        if not candidates:
            return []

        prefer_domains = prefer_domains or []

        # Pre-rank by quality score
        scored = [(r, _quality_score(r, prefer_domains)) for r in candidates]
        scored = [(r, s) for r, s in scored if s >= 0]
        scored.sort(key=lambda x: x[1], reverse=True)
        pool = [r for r, _ in scored[:20]]  # send top 20 to LLM

        if not pool:
            return []

        # Build candidate list for LLM
        candidate_list = "\n".join(
            f"[{i}] {r.url} | {r.title[:60]} | {r.snippet[:100]}"
            for i, r in enumerate(pool)
        )
        user_msg = (
            f"Empresa: {company_name}\n"
            f"Objetivo: {topic}\n\n"
            f"Candidatos:\n{candidate_list}\n\n"
            f"Selecione até {self.max_urls} URLs mais relevantes."
        )

        result = await _llm_json(
            system=_SELECTOR_SYSTEM,
            user=user_msg,
            model=self.model,
            schema=_SELECTOR_SCHEMA,
        )

        if result and result.get("selected_indices"):
            selected = []
            for idx in result["selected_indices"]:
                if 0 <= idx < len(pool):
                    selected.append(pool[idx])
            return selected[:self.max_urls]

        # Fallback: top-K by score
        return pool[:self.max_urls]

    def select(
        self, candidates: list[SearchResult], company_name: str, topic: str
    ) -> list[SearchResult]:
        return asyncio.run(self.select_async(candidates, company_name, topic))


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator: SearchOrchestrator
# ─────────────────────────────────────────────────────────────────────────────

class SearchOrchestrator:
    """
    Full search pipeline: Planner → [SearXNG + Exa] → Dedup → Selector → ranked URLs.

    Usage:
        orch = SearchOrchestrator()
        urls = await orch.find_urls_async(
            company_name="Itaú Unibanco",
            topic="estratégia governança RI",
            ri_url="https://www.itausa.com.br/ri",
            max_results=6,
        )
        # urls: list[str] ready for scraping
    """

    def __init__(
        self,
        searxng_url: str | None = None,
        exa_api_key: str | None = None,
        planner_model: str = "gpt-5.4-mini",
        selector_model: str = "gpt-5.4-mini",
        max_results: int = 6,
    ):
        self.planner = SearchPlanner(model=planner_model)
        self.searxng = SearXNGProvider(base_url=searxng_url)
        self.exa = ExaProvider(api_key=exa_api_key)
        self.selector = URLSelector(model=selector_model, max_urls=max_results)
        self.max_results = max_results

    async def resolve_company_async(
        self,
        company_name: str,
        hq_country: str = "Brazil",
        sector: str | None = None,
    ) -> CompanyProfile | None:
        """
        Uses Exa Company Search (category="company") to resolve:
          - Official website / RI URL when not known
          - Company description for strategy context

        Call this BEFORE find_urls_async when ri_url is unknown.
        Example:
            profile = await orch.resolve_company_async("Itaú Unibanco", sector="financial")
            if profile and profile.ri_url:
                urls = await orch.find_urls_async(..., ri_url=profile.ri_url)
        """
        return await self.exa.company_lookup_async(company_name, hq_country, sector)

    def resolve_company(
        self,
        company_name: str,
        hq_country: str = "Brazil",
        sector: str | None = None,
    ) -> CompanyProfile | None:
        """Sync wrapper."""
        return asyncio.run(self.resolve_company_async(company_name, hq_country, sector))

    async def find_urls_async(
        self,
        company_name: str,
        topic: str = "estratégia governança relações investidores",
        ri_url: str | None = None,
        sector: str | None = None,
    ) -> list[str]:
        """
        Full pipeline: returns list of relevant URLs for company/topic.

        If ri_url is None, first attempts Exa Company Search to discover
        the company's RI/investor-relations URL automatically.
        """
        # 0. Auto-discover RI URL via Exa Company Search if not provided
        if not ri_url:
            profile = await self.exa.company_lookup_async(company_name, sector=sector)
            if profile:
                ri_url = profile.ri_url or profile.website
                logger.info(f"[SearchOrchestrator] Exa Company resolved RI URL: {ri_url}")

        # 1. Plan queries
        plan = await self.planner.plan_async(company_name, topic, ri_url, sector)
        logger.info(
            f"[SearchOrchestrator] Plan for '{company_name}': "
            f"web_queries={plan.web_queries}, exa={plan.exa_query[:50]}"
        )

        # 2. Execute search providers in parallel
        tasks = []
        for query in plan.web_queries[:2]:  # max 2 SearXNG queries
            tasks.append(self.searxng.search_async(query, num_results=10))

        if plan.exa_query:
            tasks.append(self.exa.search_async(
                plan.exa_query,
                num_results=10,
                include_domains=plan.preferred_domains or None,
            ))

        all_results: list[SearchResult] = []
        for batch in await asyncio.gather(*tasks, return_exceptions=True):
            if isinstance(batch, list):
                all_results.extend(batch)

        # 3. Dedup
        deduped = _dedup_urls(all_results)
        logger.info(f"[SearchOrchestrator] Pool: {len(all_results)} raw → {len(deduped)} deduped")

        if not deduped:
            # If no search results, at least return the known RI URL
            return [ri_url] if ri_url else []

        # 4. LLM Selector
        selected = await self.selector.select_async(
            deduped, company_name, topic, plan.preferred_domains
        )
        urls = [r.url for r in selected if r.url]

        # Always include the RI root if known and not already in list
        if ri_url and ri_url not in urls:
            urls.insert(0, ri_url)

        logger.info(f"[SearchOrchestrator] Final: {len(urls)} URLs for '{company_name}'")
        return urls[:self.max_results]

    def find_urls(
        self,
        company_name: str,
        topic: str = "estratégia governança",
        ri_url: str | None = None,
        sector: str | None = None,
    ) -> list[str]:
        """Sync wrapper."""
        return asyncio.run(self.find_urls_async(company_name, topic, ri_url, sector))
