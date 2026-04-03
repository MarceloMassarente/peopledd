from __future__ import annotations

"""
StrategyRetriever — standalone scraping + LLM extraction de estratégia corporativa.

Pipeline:
  1. Se ri_url desconhecido → SearchOrchestrator.resolve_company_async()
     (Exa Company Search: category="company" + type="auto")
  2. SearchOrchestrator.find_urls_async() → Planner + SearXNG + Exa → LLM Selector
  3. MultiStrategyScraper.scrape_url() para cada URL selecionada
  4. gpt-5.4 com json_schema estrito → StrategyChallenges dict

Totalmente standalone — nenhuma dependência do deepsearch.
"""

import asyncio
import json
import logging
import os
from typing import Any

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
""".strip()


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
    ):
        self.llm_model = llm_model
        self.max_pages = max_pages

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

        # Search orchestrator (discovery layer)
        self._search = SearchOrchestrator(
            searxng_url=searxng_url or os.environ.get("SEARXNG_URL"),
            exa_api_key=exa_api_key or os.environ.get("EXA_API_KEY"),
            planner_model="gpt-5.4-mini",
            selector_model="gpt-5.4-mini",
            max_results=max_pages,
        )

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
    ) -> tuple[list[str], list[str]]:
        """
        Discover + scrape strategy pages.
        Returns (content_parts, source_urls).

        Strategy:
          - SearchOrchestrator resolves RI URL if missing (Exa Company Search)
          - SearchOrchestrator generates queries + selects URLs via LLM
          - MultiStrategyScraper fetches each URL
        """
        # 1. Discover URLs (includes Exa Company Search for RI URL when unknown)
        urls = await self._search.find_urls_async(
            company_name=company_name,
            topic="estratégia governança relações investidores relatório anual",
            ri_url=ri_url,
            sector=sector,
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
    ) -> dict[str, Any]:
        """
        Main async method. Returns raw dict matching _STRATEGY_EXTRACTION_SCHEMA.
        Falls back to empty structure on any failure.
        """
        content_parts, source_urls = await self._gather_content(company_name, ri_url, sector)

        if not content_parts:
            logger.warning(f"[StrategyRetriever] No content retrieved for {company_name}")
            return _empty_strategy_dict()

        full_context = "\n\n---\n\n".join(content_parts)
        max_chars = 18_000
        truncated = full_context[:max_chars]

        user_msg = (
            f"Empresa: {company_name}\n"
            f"Setor: {sector or 'desconhecido'}\n"
            f"Fontes consultadas: {', '.join(source_urls[:5])}\n\n"
            f"Conteúdo coletado:\n\n{truncated}"
        )

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
            logger.info(
                f"[StrategyRetriever] Extracted for {company_name}: "
                f"{len(result.get('strategic_priorities', []))} priorities, "
                f"{len(result.get('key_challenges', []))} challenges"
            )
            return result
        except Exception as e:
            logger.error(f"[StrategyRetriever] LLM extraction failed: {e}")
            return _empty_strategy_dict()

    def retrieve(
        self,
        company_name: str,
        ri_url: str | None = None,
        sector: str | None = None,
    ) -> dict[str, Any]:
        """Sync wrapper."""
        return asyncio.run(self.retrieve_async(company_name, ri_url, sector))


def _empty_strategy_dict() -> dict[str, Any]:
    return {
        "strategic_priorities": [],
        "key_challenges": [],
        "recent_triggers": [],
        "company_phase_hypothesis": {"phase": "mixed", "confidence": 0.3},
    }
