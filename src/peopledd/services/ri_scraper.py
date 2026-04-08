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
        cfg = ScraperConfig(
            enable_httpx=True,
            enable_jina=bool(jina_api_key or os.environ.get("JINA_API_KEY")),
            enable_browserless=bool(browserless_endpoint or os.environ.get("BROWSERLESS_ENDPOINT")),
            enable_wayback=True,
            browserless_endpoint=browserless_endpoint or os.environ.get("BROWSERLESS_ENDPOINT") or None,
            browserless_token=browserless_token or os.environ.get("BROWSERLESS_TOKEN") or None,
            jina_api_key=jina_api_key or os.environ.get("JINA_API_KEY") or None,
            request_timeout=20,
            browserless_timeout=60,
            jina_timeout=30,
            cache_ttl_sec=3600,
            max_retries=2,
            min_content_words=50,
        )
        self._scraper = MultiStrategyScraper(cfg)

    async def _scrape_url(self, url: str) -> ScrapeResult | None:
        """Fetch URL via MultiStrategyScraper (vendor)."""
        try:
            result = await self._scraper.scrape_url(url)
            if result.success and result.content:
                logger.info(f"[RIScraper] Scraped {url} via {result.strategy} ({len(result.content)} chars)")
                return result
        except Exception as e:
            logger.error(f"[RIScraper] Scrape failed for {url}: {e}")
        return None

    async def _extract_governance(self, content: str, company_name: str, source_url: str) -> GovernanceSnapshot:
        """Call LLM to extract structured governance from markdown content."""
        if not content or len(content.split()) < 30:
            logger.warning(f"[RIScraper] Insufficient content from {source_url}")
            return GovernanceSnapshot()

        # Truncate to avoid exceeding context (gpt-5.4-mini has generous context but let's be conservative)
        max_chars = 12_000
        truncated = content[:max_chars]
        if len(content) > max_chars:
            logger.info(f"[RIScraper] Content truncated {len(content)} → {max_chars} chars")

        user_msg = f"Empresa: {company_name}\n\nConteúdo da página RI:\n\n{truncated}"

        if not try_consume_llm_call("ri_governance_extraction"):
            record_llm_route("ri_governance_extraction", False, "budget_exhausted")
            logger.warning("[RIScraper] LLM extraction skipped (budget) for %s", source_url)
            return GovernanceSnapshot()

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
        except Exception as e:
            logger.error(f"[RIScraper] LLM extraction failed: {e}")
            record_llm_route("ri_governance_extraction", False, f"llm_error:{type(e).__name__}")
            return GovernanceSnapshot()

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
        return snapshot

    def scrape_board(self, ri_url: str, company_name: str) -> GovernanceSnapshot:
        """Sync wrapper for async scrape_board_async."""
        return asyncio.run(self.scrape_board_async(ri_url, company_name))

    async def scrape_board_async(self, ri_url: str, company_name: str) -> GovernanceSnapshot:
        """Main async method: scrape RI page → LLM extract → GovernanceSnapshot."""
        import re
        from urllib.parse import urljoin

        # Try base URL first
        result = await self._scrape_url(ri_url)
        content = result.content if result else ""
        raw_html = result.raw_html if result else ""

        # Intent Crawler base em snapshot HTML: procura sub-links de governança
        if (not content or len(content.split()) < 100) and raw_html:
            found_url = None
            for match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', raw_html, re.IGNORECASE | re.DOTALL):
                href = match.group(1).strip()
                raw_text = match.group(2)
                text = re.sub(r'<[^>]+>', ' ', raw_text).lower()
                if re.search(r"\b(governança|diretoria|conselho|liderança|administração|quem somos)\b", text):
                    candidate = urljoin(ri_url, href)
                    if candidate != ri_url and candidate.startswith('http'):
                        found_url = candidate
                        break
            
            if found_url:
                logger.info(f"[RIScraper] Intent crawler matched local RI link: {found_url}")
                result2 = await self._scrape_url(found_url)
                if result2 and result2.content and len(result2.content.split()) > 100:
                    content = result2.content
                    ri_url = found_url

        # Fallback para sufixos estáticos caso não tenha encontrado nada
        if not content or len(content.split()) < 100:
            governance_suffixes = [
                "/governanca-corporativa/estrutura-de-governanca",
                "/governanca/conselho-de-administracao",
                "/pt/governanca",
                "/governance",
                "/pt/quem-somos/governanca",
            ]
            for suffix in governance_suffixes:
                if not ri_url.endswith(suffix):
                    candidate = ri_url.rstrip("/") + suffix
                    result3 = await self._scrape_url(candidate)
                    if result3 and result3.content and len(result3.content.split()) > 100:
                        content = result3.content
                        ri_url = candidate
                        break

        return await self._extract_governance(content, company_name, ri_url)
