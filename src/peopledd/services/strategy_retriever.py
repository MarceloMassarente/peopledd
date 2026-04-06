from __future__ import annotations

"""
StrategyRetriever — standalone scraping + LLM extraction de estratégia corporativa.

Pipeline:
  1. Se ri_url desconhecido → SearchOrchestrator.resolve_company_async()
     (Exa Company Search: category="company" + type="auto")
  2. SearchOrchestrator.find_urls_async() → Planner + SearXNG + Exa → LLM Selector
  3. MultiStrategyScraper.scrape_url() para cada URL selecionada
  4. gpt-5.4 com json_schema estrito → StrategyChallenges dict
  Paralelo: Perplexity Sonar Pro (2 queries) quando PERPLEXITY_API_KEY está definida.

Totalmente standalone — nenhuma dependência do deepsearch.
"""

import asyncio
import json
import logging
import os
from typing import Any

import httpx

from peopledd.models.common import SourceRef
from peopledd.models.contracts import ExternalSonarBrief
from peopledd.runtime.adaptive_models import FindUrlsParams, SearchAttemptRecord
from peopledd.runtime.pipeline_context import get_attached_run_context, record_llm_route, try_consume_llm_call
from peopledd.services.perplexity_sonar import (
    SonarQueryOutcome,
    fetch_sonar_briefs_pair,
    perplexity_sonar_enabled,
)
from peopledd.vendor.scraper import MultiStrategyScraper, ScraperConfig
from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# OpenAI client
# ─────────────────────────────────────────────────────────────────────────────

_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        try:
            from openai import AsyncOpenAI
            _openai_client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        except ImportError:
            raise RuntimeError("openai package not installed. Run: pip install openai")
    return _openai_client


# ─────────────────────────────────────────────────────────────────────────────
# JSON Schema for strategy extraction
# ─────────────────────────────────────────────────────────────────────────────

_STRATEGY_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "strategic_priorities": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "priority": {"type": "string"},
                    "time_horizon": {"type": "string", "enum": ["short", "medium", "long"]},
                    "confidence": {"type": "number"},
                    "evidence_snippet": {"type": "string"},
                },
                "required": ["priority", "time_horizon"],
                "additionalProperties": False,
            },
        },
        "key_challenges": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "challenge": {"type": "string"},
                    "challenge_type": {
                        "type": "string",
                        "enum": ["financial", "operational", "market", "regulatory", "governance", "technology", "people"],
                    },
                    "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                    "confidence": {"type": "number"},
                    "evidence_snippet": {"type": "string"},
                },
                "required": ["challenge", "challenge_type", "severity"],
                "additionalProperties": False,
            },
        },
        "recent_triggers": {
            "type": "array",
            "items": {"type": "string"},
        },
        "company_phase_hypothesis": {
            "type": "object",
            "properties": {
                "phase": {
                    "type": "string",
                    "enum": ["growth", "expansion", "turnaround", "consolidation", "mature", "crisis", "mixed"],
                },
                "rationale": {"type": "string"},
                "confidence": {"type": "number"},
            },
            "required": ["phase", "confidence"],
            "additionalProperties": False,
        },
    },
    "required": ["strategic_priorities", "key_challenges", "recent_triggers", "company_phase_hypothesis"],
    "additionalProperties": False,
}

_STRATEGY_SYSTEM_PROMPT = """
Você é um analista sênior de estratégia corporativa especializado em empresas brasileiras.
Dado conteúdo de documentos RI (Relações com Investidores), relatórios anuais e comunicados,
extraia inferências estratégicas concretas e verificáveis.

EXTRAIA:
1. strategic_priorities: prioridades estratégicas explícitas (expansão, M&A, transformação digital, etc.)
2. key_challenges: desafios materiais (pressão de margens, disrupção regulatória, etc.)
3. recent_triggers: eventos recentes relevantes (aquisições, desinvestimentos, mudanças de liderança, guidances revisados)
4. company_phase_hypothesis: hipótese de fase (growth, turnaround, consolidation, etc.)

REGRAS:
- Ancoragem obrigatória: cada item deve ter suporte textual no conteúdo.
- confidence: 0.9+ se explícito no texto; 0.7 se inferido; 0.5 se especulativo.
- NÃO invente prioridades. Se não houver evidência, use arrays vazios.
- Foco em BR: priorize menções a SELIC, BRL/USD, BNDES, regulação CVM/ANATEL/ANAC/ANP.

CONTEXTO AUXILIAR (Perplexity Sonar Pro):
- Se houver bloco "Contexto auxiliar (Perplexity Sonar Pro — pesquisa web)", use-o como evidência web complementar.
- Priorize sempre trechos ancorados no conteúdo RI quando houver conflito com a web.
- Incorpore gatilhos recentes ou desafios suportados pelo bloco auxiliar com confidence moderada (0.55–0.75).
""".strip()

_SONAR_ROLE_LABELS: dict[str, str] = {
    "recent_company_facts": "Fatos recentes (web)",
    "sector_governance_context": "Contexto setorial e governança (web)",
}


async def _http_ri_maturity_signal(ri_url: str | None) -> float:
    """
    GET the RI base URL once (timeout-bound). Map status + HTML size to a 0-1 maturity signal
    used to scale extraction confidences (thin or error responses reduce confidence).
    """
    if not ri_url or not isinstance(ri_url, str):
        return 0.55
    u = ri_url.strip()
    if not u.lower().startswith("http"):
        return 0.55
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=6.0),
            follow_redirects=True,
            headers={"User-Agent": "peopledd-strategy-retriever/1.0"},
        ) as client:
            r = await client.get(u)
            if r.status_code >= 400:
                return 0.42
            n = len(r.text or "")
            if n < 400:
                return 0.45 + (n / 400.0) * 0.12
            if n < 4000:
                return 0.57 + (n / 4000.0) * 0.28
            return min(1.0, 0.85 + min(0.15, n / 200_000.0))
    except Exception as e:
        logger.debug("[StrategyRetriever] RI maturity GET failed for %s: %s", ri_url, e)
        return 0.48


def _scrape_corpus_maturity(content_parts: list[str]) -> float:
    """Map aggregated scraped text volume to 0-1 (strategy pages already filtered by min words)."""
    words = sum(len(p.split()) for p in content_parts)
    if words <= 0:
        return 0.4
    return min(1.0, (words / 2200.0) ** 0.55)


def _combined_strategy_confidence_scale(ri_http: float, scrape: float) -> float:
    return max(0.38, min(1.0, 0.45 * ri_http + 0.55 * scrape))


def _apply_strategy_maturity_to_result(result: dict[str, Any], scale: float) -> None:
    for key in ("strategic_priorities", "key_challenges"):
        for item in result.get(key) or []:
            if not isinstance(item, dict):
                continue
            c = float(item.get("confidence", 0.6))
            item["confidence"] = max(0.12, min(0.95, c * scale))
    phase = result.get("company_phase_hypothesis")
    if isinstance(phase, dict):
        pc = float(phase.get("confidence", 0.4))
        phase["confidence"] = max(0.1, min(0.95, pc * scale))


def _sonar_outcomes_to_briefs(outcomes: list[SonarQueryOutcome]) -> list[ExternalSonarBrief]:
    briefs: list[ExternalSonarBrief] = []
    for o in outcomes:
        label = _SONAR_ROLE_LABELS.get(o.role, o.role)
        refs = [
            SourceRef(source_type="perplexity_sonar_pro", label=label, url_or_ref=u)
            for u in o.citation_urls
            if u.startswith("http")
        ]
        briefs.append(ExternalSonarBrief(role=o.role, body=o.body, source_refs=refs))
    return briefs


class StrategyRetriever:
    """
    Retrieves and extracts corporate strategy context for a company.

    Pipeline:
      1. Exa Company Search → auto-resolve RI URL (when unknown)
      2. SearchOrchestrator → LLM Planner + SearXNG + Exa → ranked URLs
      3. MultiStrategyScraper → fetch page content (httpx → Jina → Browserless)
      4. gpt-5.4 → StrategyChallenges JSON extraction

    Fully standalone — no deepsearch dependency.
    Used by n4_strategy_inference.
    """

    def __init__(
        self,
        browserless_endpoint: str | None = None,
        browserless_token: str | None = None,
        jina_api_key: str | None = None,
        exa_api_key: str | None = None,
        searxng_url: str | None = None,
        llm_model: str = "gpt-5.4",
        max_pages: int = 4,
        search_orchestrator: SearchOrchestrator | None = None,
        use_perplexity_sonar: bool | None = None,
    ):
        self.llm_model = llm_model
        self.max_pages = max_pages
        if use_perplexity_sonar is None:
            self._use_perplexity = perplexity_sonar_enabled()
        else:
            self._use_perplexity = bool(use_perplexity_sonar)

        # Scraper (transport layer)
        cfg = ScraperConfig(
            enable_httpx=True,
            enable_jina=bool(jina_api_key or os.environ.get("JINA_API_KEY")),
            enable_browserless=bool(browserless_endpoint or os.environ.get("BROWSERLESS_ENDPOINT")),
            enable_wayback=False,
            browserless_endpoint=browserless_endpoint or os.environ.get("BROWSERLESS_ENDPOINT") or None,
            browserless_token=browserless_token or os.environ.get("BROWSERLESS_TOKEN") or None,
            jina_api_key=jina_api_key or os.environ.get("JINA_API_KEY") or None,
            request_timeout=25,
            browserless_timeout=60,
            jina_timeout=30,
            cache_ttl_sec=7200,  # 2h cache for strategy pages
            min_content_words=60,
        )
        self._scraper = MultiStrategyScraper(cfg)

        # Search orchestrator (discovery layer) — shared instance optional for adaptive runs
        if search_orchestrator is not None:
            self._search = search_orchestrator
        else:
            self._search = SearchOrchestrator(
                searxng_url=searxng_url or os.environ.get("SEARXNG_URL"),
                exa_api_key=exa_api_key or os.environ.get("EXA_API_KEY"),
                planner_model="gpt-5.4-mini",
                selector_model="gpt-5.4-mini",
                max_results=max_pages,
            )
        self._search.max_results = max(1, min(24, max_pages))
        self._search.selector.max_urls = max(1, min(24, max_pages))

    async def _scrape_url(self, url: str) -> str:
        """Fetch and return text content from a URL."""
        try:
            result = await self._scraper.scrape_url(url)
            if result.success and result.content:
                return result.content
        except Exception as e:
            logger.debug(f"[StrategyRetriever] Scrape failed for {url}: {e}")
        return ""

    async def _gather_content(
        self,
        company_name: str,
        ri_url: str | None,
        sector: str | None,
        find_params: FindUrlsParams | None = None,
        strategy_search_attempt_index: int = 0,
        find_urls_escalation_level: int = 0,
    ) -> tuple[list[str], list[str]]:
        """
        Discover + scrape strategy pages.
        Returns (content_parts, source_urls).

        Strategy:
          - SearchOrchestrator resolves RI URL if missing (Exa Company Search)
          - SearchOrchestrator generates queries + selects URLs via LLM
          - MultiStrategyScraper fetches each URL
        """
        fp = find_params or FindUrlsParams.default()
        # 1. Discover URLs (includes Exa Company Search for RI URL when unknown)
        outcome = await self._search.find_urls_async(
            company_name=company_name,
            topic="estratégia governança relações investidores relatório anual",
            ri_url=ri_url,
            sector=sector,
            find_params=fp,
        )
        urls = outcome.urls

        ctx = get_attached_run_context()
        if ctx is not None:
            ctx.record_search_attempt(
                SearchAttemptRecord(
                    purpose="strategy_find_urls",
                    attempt_index=strategy_search_attempt_index,
                    escalation_level=find_urls_escalation_level,
                    searxng_queries_used=outcome.searxng_queries_used,
                    exa_num_results_requested=outcome.exa_num_results_requested,
                    url_count=len(urls),
                    empty_pool=outcome.empty_pool,
                    topic_excerpt=outcome.topic_effective,
                )
            )

        if not urls and ri_url:
            # Fallback: use RI URL + static subpaths
            static_paths = [
                "", "/resultados", "/relatorio-anual",
                "/carta-ao-acionista", "/estrategia", "/apresentacoes",
            ]
            base = ri_url.rstrip("/")
            urls = [base + p for p in static_paths][:self.max_pages]

        # 2. Scrape selected URLs
        content_parts: list[str] = []
        source_urls: list[str] = []

        tasks = [self._scrape_url(url) for url in urls[:self.max_pages]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for url, content in zip(urls[:self.max_pages], results):
            if isinstance(content, str) and content and len(content.split()) >= 60:
                content_parts.append(f"## Fonte: {url}\n\n{content[:4500]}")
                source_urls.append(url)
                logger.info(f"[StrategyRetriever] ✓ {url} ({len(content)} chars)")
            else:
                err = content if isinstance(content, Exception) else "insuficiente"
                logger.debug(f"[StrategyRetriever] ✗ {url}: {err}")

        return content_parts, source_urls

    async def retrieve_async(
        self,
        company_name: str,
        ri_url: str | None = None,
        sector: str | None = None,
        country: str = "BR",
        find_urls_params: FindUrlsParams | None = None,
        strategy_search_attempt_index: int = 0,
        find_urls_escalation_level: int = 0,
        skip_perplexity_sonar: bool = False,
    ) -> dict[str, Any]:
        """
        Main async method. Returns raw dict matching _STRATEGY_EXTRACTION_SCHEMA.
        Falls back to empty structure on any failure.
        """
        run_sonar = self._use_perplexity and not skip_perplexity_sonar
        ri_probe = asyncio.create_task(_http_ri_maturity_signal(ri_url))
        if run_sonar:
            (content_parts, source_urls), sonar_outcomes = await asyncio.gather(
                self._gather_content(
                    company_name,
                    ri_url,
                    sector,
                    find_params=find_urls_params,
                    strategy_search_attempt_index=strategy_search_attempt_index,
                    find_urls_escalation_level=find_urls_escalation_level,
                ),
                fetch_sonar_briefs_pair(company_name, sector=sector, country=country),
            )
        else:
            content_parts, source_urls = await self._gather_content(
                company_name,
                ri_url,
                sector,
                find_params=find_urls_params,
                strategy_search_attempt_index=strategy_search_attempt_index,
                find_urls_escalation_level=find_urls_escalation_level,
            )
            sonar_outcomes = []  # skipped or disabled
        ri_http_signal = await ri_probe

        external_briefs = _sonar_outcomes_to_briefs(sonar_outcomes)
        external_payload = [b.model_dump(mode="json") for b in external_briefs]

        def _with_sonar(base: dict[str, Any]) -> dict[str, Any]:
            base["external_sonar_briefs"] = external_payload
            return base

        if not content_parts:
            logger.warning(f"[StrategyRetriever] No content retrieved for {company_name}")
            return _with_sonar(_empty_strategy_dict())

        full_context = "\n\n---\n\n".join(content_parts)
        max_chars = 18_000
        truncated = full_context[:max_chars]

        user_msg = (
            f"Empresa: {company_name}\n"
            f"Setor: {sector or 'desconhecido'}\n"
            f"País: {country}\n"
            f"Fontes consultadas: {', '.join(source_urls[:5])}\n\n"
            f"Conteúdo coletado:\n\n{truncated}"
        )
        if external_briefs:
            aux_lines = ["\n## Contexto auxiliar (Perplexity Sonar Pro — pesquisa web)\n"]
            for b in external_briefs:
                title = _SONAR_ROLE_LABELS.get(b.role, b.role)
                snippet = b.body[:4500] if len(b.body) > 4500 else b.body
                urls_note = ""
                if b.source_refs:
                    urls_note = "\nURLs (API): " + ", ".join(
                        r.url_or_ref for r in b.source_refs[:12]
                    )
                aux_lines.append(f"### {title}\n{snippet}{urls_note}\n")
            user_msg += "\n".join(aux_lines)

        if not try_consume_llm_call("strategy_extraction"):
            record_llm_route("strategy_extraction", False, "budget_exhausted")
            logger.warning("[StrategyRetriever] LLM extraction skipped (budget) for %s", company_name)
            return _with_sonar(_empty_strategy_dict())

        try:
            client = _get_openai_client()
            response = await client.chat.completions.create(
                model=self.llm_model,
                temperature=0.15,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "strategy_extraction",
                        "strict": True,
                        "schema": _STRATEGY_EXTRACTION_SCHEMA,
                    },
                },
                messages=[
                    {"role": "system", "content": _STRATEGY_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
            )
            raw_json = response.choices[0].message.content or "{}"
            result = json.loads(raw_json)
            scrape_sig = _scrape_corpus_maturity(content_parts)
            conf_scale = _combined_strategy_confidence_scale(ri_http_signal, scrape_sig)
            _apply_strategy_maturity_to_result(result, conf_scale)
            record_llm_route("strategy_extraction", True, "ok")
            logger.info(
                f"[StrategyRetriever] Extracted for {company_name}: "
                f"{len(result.get('strategic_priorities', []))} priorities, "
                f"{len(result.get('key_challenges', []))} challenges "
                f"(conf_scale={conf_scale:.2f} ri_http={ri_http_signal:.2f} scrape={scrape_sig:.2f})"
            )
            return _with_sonar(result)
        except Exception as e:
            logger.error(f"[StrategyRetriever] LLM extraction failed: {e}")
            record_llm_route("strategy_extraction", False, f"llm_error:{type(e).__name__}")
            return _with_sonar(_empty_strategy_dict())

    def retrieve(
        self,
        company_name: str,
        ri_url: str | None = None,
        sector: str | None = None,
        country: str = "BR",
        find_urls_params: FindUrlsParams | None = None,
        strategy_search_attempt_index: int = 0,
        find_urls_escalation_level: int = 0,
        skip_perplexity_sonar: bool = False,
    ) -> dict[str, Any]:
        """Sync wrapper."""
        return asyncio.run(
            self.retrieve_async(
                company_name,
                ri_url,
                sector,
                country=country,
                find_urls_params=find_urls_params,
                strategy_search_attempt_index=strategy_search_attempt_index,
                find_urls_escalation_level=find_urls_escalation_level,
                skip_perplexity_sonar=skip_perplexity_sonar,
            )
        )


def _empty_strategy_dict() -> dict[str, Any]:
    return {
        "strategic_priorities": [],
        "key_challenges": [],
        "recent_triggers": [],
        "company_phase_hypothesis": {"phase": "mixed", "confidence": 0.3},
        "external_sonar_briefs": [],
    }
