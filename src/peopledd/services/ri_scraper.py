from __future__ import annotations

"""
RIScraper — extrai governança da página RI de uma empresa.

Chain de scraping (standalone, sem deepsearch):
    httpx+trafilatura → Jina → Browserless

Após obter o markdown da página, usa gpt-5.4-mini com schema JSON enforçado
para extrair membros do conselho e diretoria.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any

from peopledd.models.contracts import (
    BoardMember,
    Committee,
    CommitteeMember,
    ExecutiveMember,
    GovernanceSnapshot,
)
from peopledd.models.common import SourceRef
from peopledd.runtime.pipeline_context import record_llm_route, try_consume_llm_call
from peopledd.runtime.source_attempt import (
    SourceAttemptResult,
    classify_http_status,
    classify_scrape_exception,
)
from peopledd.services.ri_surface_discoverer import discover_ri_surfaces
from peopledd.vendor.scraper import MultiStrategyScraper, ScraperConfig, ScrapeResult

logger = logging.getLogger(__name__)

# Lazy OpenAI client
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


# JSON extraction schema for the LLM
_GOVERNANCE_EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "board_members": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "person_name": {"type": "string"},
                    "role": {"type": "string", "enum": ["chair", "vice-chair", "board-member", "unknown"]},
                    "independence_status": {"type": "string", "enum": ["independent", "non_independent", "unknown"]},
                    "term_start": {"type": "string"},
                    "term_end": {"type": "string"},
                },
                "required": ["person_name", "role"],
            },
        },
        "executive_members": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "person_name": {"type": "string"},
                    "formal_title": {"type": "string"},
                    "normalized_role": {
                        "type": "string",
                        "enum": ["ceo", "cfo", "coo", "chro", "cto", "cio", "cmo", "legal", "other"],
                    },
                    "term_start": {"type": "string"},
                },
                "required": ["person_name", "formal_title"],
            },
        },
        "committees": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "committee_name": {"type": "string"},
                    "committee_type": {
                        "type": "string",
                        "enum": ["audit", "people", "finance", "strategy", "risk", "esg", "other"],
                    },
                    "members": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "person_name": {"type": "string"},
                                "position_in_committee": {
                                    "type": "string",
                                    "enum": ["chair", "member", "unknown"],
                                },
                            },
                            "required": ["person_name"],
                        },
                    },
                },
                "required": ["committee_name"],
            },
        },
        "as_of_date": {"type": "string"},
    },
    "required": ["board_members", "executive_members"],
    "additionalProperties": False,
}

_EXTRACTION_SYSTEM_PROMPT = """
Você é um extrator especializado em governança corporativa brasileira.
Dado o conteúdo markdown de uma página de Relações com Investidores (RI), extraia:

1. board_members: membros do Conselho de Administração (CA).
2. executive_members: membros da Diretoria Estatutária (Diretores).
3. committees: comitês assessores (Auditoria, Pessoas, Risco, Estratégia, etc.).
4. as_of_date: data de referência indicada na página (formato YYYY-MM-DD se encontrada).

REGRAS CRÍTICAS:
- Inclua APENAS nomes que aparecem explicitamente no conteúdo.
- Se um campo não estiver disponível, omita-o ou use null.
- Para independence_status: "independent" se o texto disser independente, "non_independent" caso contrário.
- Para normalized_role use a categoria mais próxima da lista permitida.
- NÃO invente nomes. Se a lista estiver vazia, retorne arrays vazios.
""".strip()


def _governance_nonempty(snap: GovernanceSnapshot) -> bool:
    return bool(snap.board_members or snap.executive_members or snap.committees)


class RIScraper:
    """
    Scraper de páginas RI para extração de governança.

    Estratégia:
    1. Scraping via vendor.scraper.MultiStrategyScraper (httpx → Jina → Browserless).
    2. LLM (gpt-5.4-mini) extrai structured JSON do markdown.
    """

    def __init__(
        self,
        browserless_endpoint: str | None = None,
        browserless_token: str | None = None,
        jina_api_key: str | None = None,
        llm_model: str = "gpt-5.4-mini",
    ):
        self.llm_model = llm_model
        _bl_endpoint = browserless_endpoint or os.environ.get("BROWSERLESS_ENDPOINT") or None
        cfg = ScraperConfig(
            enable_httpx=True,
            enable_jina=bool(jina_api_key or os.environ.get("JINA_API_KEY")),
            enable_browserless=bool(_bl_endpoint),
            enable_browserless_interactive=bool(_bl_endpoint),
            enable_wayback=True,
            browserless_endpoint=_bl_endpoint,
            browserless_token=browserless_token or os.environ.get("BROWSERLESS_TOKEN") or None,
            jina_api_key=jina_api_key or os.environ.get("JINA_API_KEY") or None,
            request_timeout=20,
            browserless_timeout=90,
            jina_timeout=30,
            cache_ttl_sec=3600,
            max_retries=2,
            min_content_words=50,
        )
        self._scraper = MultiStrategyScraper(cfg)

    async def _scrape_url_traced(
        self, url: str, attempts: list[SourceAttemptResult]
    ) -> ScrapeResult | None:
        t0 = time.perf_counter()
        try:
            result = await self._scraper.scrape_url(url)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            if result.success and result.content:
                wc = len(result.content.split())
                attempts.append(
                    SourceAttemptResult(
                        success=True,
                        failure_mode=None,
                        source_url=url,
                        content_words=wc,
                        strategy_used=result.strategy,
                        latency_ms=elapsed_ms,
                    )
                )
                logger.info(f"[RIScraper] Scraped {url} via {result.strategy} ({len(result.content)} chars)")
                return result
            fm: str | None = None
            if result.status_code:
                fm = classify_http_status(result.status_code)
            if fm is None:
                fm = "network_error"
            attempts.append(
                SourceAttemptResult(
                    success=False,
                    failure_mode=fm,  # type: ignore[arg-type]
                    source_url=url,
                    content_words=len(result.content.split()) if result.content else 0,
                    strategy_used=result.strategy or None,
                    latency_ms=elapsed_ms,
                    error_detail=(result.error or "")[:500] or None,
                )
            )
            return None
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.error(f"[RIScraper] Scrape failed for {url}: {e}")
            attempts.append(
                SourceAttemptResult(
                    success=False,
                    failure_mode=classify_scrape_exception(e),
                    source_url=url,
                    content_words=0,
                    strategy_used=None,
                    latency_ms=elapsed_ms,
                    error_detail=str(e)[:500],
                )
            )
            return None

    async def _extract_governance_with_attempt(
        self, content: str, company_name: str, source_url: str
    ) -> tuple[GovernanceSnapshot, SourceAttemptResult]:
        t0 = time.perf_counter()
        wc = len(content.split()) if content else 0

        def done(snap: GovernanceSnapshot, success: bool, fm: str | None, detail: str | None = None) -> tuple[GovernanceSnapshot, SourceAttemptResult]:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            return snap, SourceAttemptResult(
                success=success,
                failure_mode=fm,  # type: ignore[arg-type]
                source_url=source_url,
                content_words=wc,
                strategy_used="llm_extract",
                latency_ms=elapsed_ms,
                as_of_date_hint=snap.as_of_date,
                governance_found=_governance_nonempty(snap),
                error_detail=detail,
            )

        if not content or wc < 30:
            logger.warning(f"[RIScraper] Insufficient content from {source_url}")
            return done(GovernanceSnapshot(), False, "low_content", "below_30_words")

        max_chars = 12_000
        truncated = content[:max_chars]
        if len(content) > max_chars:
            logger.info(f"[RIScraper] Content truncated {len(content)} → {max_chars} chars")

        user_msg = f"Empresa: {company_name}\n\nConteúdo da página RI:\n\n{truncated}"

        if not try_consume_llm_call("ri_governance_extraction"):
            record_llm_route("ri_governance_extraction", False, "budget_exhausted")
            logger.warning("[RIScraper] LLM extraction skipped (budget) for %s", source_url)
            return done(GovernanceSnapshot(), False, "budget_exhausted")

        try:
            client = _get_openai_client()
            response = await client.chat.completions.create(
                model=self.llm_model,
                temperature=0.1,
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "governance_extraction",
                        "strict": True,
                        "schema": _GOVERNANCE_EXTRACTION_SCHEMA,
                    },
                },
                messages=[
                    {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
            )
            raw_json = response.choices[0].message.content or "{}"
            data: dict[str, Any] = json.loads(raw_json)
            record_llm_route("ri_governance_extraction", True, "ok")
        except json.JSONDecodeError as e:
            logger.error(f"[RIScraper] LLM JSON parse failed: {e}")
            record_llm_route("ri_governance_extraction", False, "parse_error")
            return done(GovernanceSnapshot(), False, "parse_error", str(e)[:500])
        except Exception as e:
            logger.error(f"[RIScraper] LLM extraction failed: {e}")
            record_llm_route("ri_governance_extraction", False, f"llm_error:{type(e).__name__}")
            return done(GovernanceSnapshot(), False, "llm_extract_failed", str(e)[:500])

        src = SourceRef(source_type="ri", label="RI governança", url_or_ref=source_url)

        board_members = [
            BoardMember(
                person_name=m.get("person_name", ""),
                role=m.get("role", "unknown"),  # type: ignore
                independence_status=m.get("independence_status", "unknown"),  # type: ignore
                term_start=m.get("term_start") or None,
                term_end=m.get("term_end") or None,
                source_refs=[src],
            )
            for m in data.get("board_members", [])
            if m.get("person_name")
        ]

        executive_members = [
            ExecutiveMember(
                person_name=m.get("person_name", ""),
                formal_title=m.get("formal_title", ""),
                normalized_role=m.get("normalized_role", "other"),  # type: ignore
                term_start=m.get("term_start") or None,
                source_refs=[src],
            )
            for m in data.get("executive_members", [])
            if m.get("person_name")
        ]

        committees = [
            Committee(
                committee_name=c.get("committee_name", ""),
                committee_type=c.get("committee_type", "other"),  # type: ignore
                members=[
                    CommitteeMember(
                        person_name=cm.get("person_name", ""),
                        position_in_committee=cm.get("position_in_committee", "unknown"),  # type: ignore
                    )
                    for cm in c.get("members", [])
                    if cm.get("person_name")
                ],
                source_refs=[src],
            )
            for c in data.get("committees", [])
            if c.get("committee_name")
        ]

        as_of_date = data.get("as_of_date") or None

        snapshot = GovernanceSnapshot(
            as_of_date=as_of_date,
            board_members=board_members,
            executive_members=executive_members,
            committees=committees,
        )

        logger.info(
            f"[RIScraper] Extracted: {len(board_members)} board, "
            f"{len(executive_members)} exec, {len(committees)} committees"
        )
        return done(snapshot, True, None)

    def scrape_board(
        self,
        ri_url: str,
        company_name: str,
        *,
        preferred_urls: list[str] | None = None,
    ) -> tuple[GovernanceSnapshot, list[SourceAttemptResult]]:
        """Sync wrapper: scrape RI page(s) → LLM extract → snapshot + traced attempts."""
        return asyncio.run(
            self.scrape_board_multi_surface(ri_url, company_name, preferred_urls=preferred_urls)
        )

    async def scrape_board_multi_surface(
        self,
        ri_url: str,
        company_name: str,
        *,
        preferred_urls: list[str] | None = None,
        max_surface_tries: int = 10,
    ) -> tuple[GovernanceSnapshot, list[SourceAttemptResult]]:
        """
        Try base RI URL, then ranked governance surfaces (in-page links + common suffixes),
        then LLM extraction on the first page with sufficient markdown.
        """
        attempts: list[SourceAttemptResult] = []

        result = await self._scrape_url_traced(ri_url, attempts)
        content = result.content if result else ""
        raw_html = result.raw_html if result else ""
        ri_effective = ri_url

        def _words(s: str) -> int:
            return len(s.split())

        if _words(content) < 100:
            surfaces = discover_ri_surfaces(
                ri_url,
                raw_html or None,
                preferred_urls=preferred_urls,
            )
            tried: set[str] = {ri_url}
            n_try = 0
            for surf in surfaces:
                if surf in tried:
                    continue
                tried.add(surf)
                n_try += 1
                if n_try > max_surface_tries:
                    break
                logger.info("[RIScraper] Trying governance surface: %s", surf)
                r2 = await self._scrape_url_traced(surf, attempts)
                if r2 and r2.content and _words(r2.content) >= 100:
                    content = r2.content
                    raw_html = r2.raw_html or raw_html
                    ri_effective = surf
                    break

        snap, extract_attempt = await self._extract_governance_with_attempt(
            content, company_name, ri_effective
        )
        attempts.append(extract_attempt)
        return snap, attempts

    async def scrape_board_async(
        self,
        ri_url: str,
        company_name: str,
        *,
        preferred_urls: list[str] | None = None,
    ) -> tuple[GovernanceSnapshot, list[SourceAttemptResult]]:
        """Alias for scrape_board_multi_surface (backward compatible name)."""
        return await self.scrape_board_multi_surface(ri_url, company_name, preferred_urls=preferred_urls)
