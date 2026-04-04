from __future__ import annotations

"""
Perplexity Sonar Pro — duas consultas fixas para evidência web (complementa RI/Exa).

Requer PERPLEXITY_API_KEY. Desligar com PERPLEXITY_SONAR_DISABLE=1.
Cada chamada consome um slot de try_consume_llm_call (telemetria alinhada ao orçamento).
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Literal

import httpx

from peopledd.runtime.pipeline_context import record_llm_route, try_consume_llm_call

logger = logging.getLogger(__name__)

PERPLEXITY_SONAR_URL = "https://api.perplexity.ai/v1/sonar"
DEFAULT_MODEL = "sonar-pro"

SonarRole = Literal["recent_company_facts", "sector_governance_context"]


@dataclass(frozen=True)
class SonarQueryOutcome:
    role: SonarRole
    body: str
    citation_urls: tuple[str, ...]


def perplexity_sonar_enabled() -> bool:
    if os.environ.get("PERPLEXITY_SONAR_DISABLE", "").strip().lower() in ("1", "true", "yes"):
        return False
    return bool(os.environ.get("PERPLEXITY_API_KEY", "").strip())


def _query_recent_facts(company_name: str, country: str) -> str:
    return f"""Você é analista cobrindo empresas em {country}.

Empresa-alvo: "{company_name}".

Tarefa: liste fatos verificáveis dos últimos 18 meses relevantes para estratégia e governança
(M&A, desinvestimentos, mudanças de CEO/CFO ou conselho, guidance ou revisões materiais,
projetos grandes de capex ou digitalização, litígios ou sanções regulatórias relevantes).

Regras:
- Cite fontes com URL ao final de cada bullet ou frase quando possível.
- Priorize fontes oficiais (RI, CVM, releases) e mídia especializada; evite fóruns anônimos.
- Se não houver dados confiáveis sobre esta empresa, diga explicitamente que não há evidência pública suficiente.
Responda em português, de forma concisa (máx. 12 bullets)."""


def _query_sector_context(company_name: str, sector: str | None, country: str) -> str:
    sec = (sector or "setor não informado").strip()
    return f"""Você é especialista em liderança e governança corporativa em {country}.

Empresa de referência: "{company_name}". Setor/regulação informados: {sec}.

Tarefa: resuma riscos e capacidades de liderança tipicamente críticas para pares desse setor
(operacional, tecnologia, pessoas, compliance/regulatório, ESG quando aplicável).

Regras:
- Baseie-se em fontes públicas (reguladores, associações setoriais, relatórios sectoriais) e cite URLs.
- Não invente fatos específicos sobre a empresa nomeada; foque no contexto setorial.
- Resposta em português, concisa (máx. 10 bullets).
Se o setor for vago, generalize com ressalva de baixa especificidade."""


async def _call_sonar(
    user_prompt: str,
    *,
    budget_step: str,
    model: str,
    search_recency_filter: str | None,
) -> tuple[str, tuple[str, ...]]:
    api_key = os.environ.get("PERPLEXITY_API_KEY", "").strip()
    if not api_key:
        record_llm_route(budget_step, False, "no_perplexity_key")
        return "", ()

    if not try_consume_llm_call(budget_step):
        record_llm_route(budget_step, False, "budget_exhausted")
        return "", ()

    payload: dict = {
        "model": model,
        "messages": [{"role": "user", "content": user_prompt}],
        "temperature": 0.2,
        "search_language_filter": ["pt", "en"],
    }
    if search_recency_filter:
        payload["search_recency_filter"] = search_recency_filter

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=15.0)) as client:
            r = await client.post(
                PERPLEXITY_SONAR_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
            )
            r.raise_for_status()
            data = r.json()
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        content = (msg.get("content") or "").strip()
        raw_cites = data.get("citations")
        urls: list[str] = []
        if isinstance(raw_cites, list):
            for c in raw_cites:
                if isinstance(c, str) and c.startswith("http"):
                    urls.append(c)
        record_llm_route(budget_step, True, "ok")
        return content, tuple(urls)
    except Exception as e:
        logger.warning("[PerplexitySonar] %s failed: %s", budget_step, e)
        record_llm_route(budget_step, False, f"error:{type(e).__name__}")
        return "", ()


async def fetch_sonar_briefs_pair(
    company_name: str,
    *,
    sector: str | None = None,
    country: str = "BR",
    model: str | None = None,
) -> list[SonarQueryOutcome]:
    """
    Executa as duas queries Sonar Pro em paralelo. Retorna 0–2 outcomes (vazios omitidos).
    """
    if not perplexity_sonar_enabled():
        return []

    m = model or os.environ.get("PERPLEXITY_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL

    async def one_recent() -> SonarQueryOutcome:
        body, cites = await _call_sonar(
            _query_recent_facts(company_name, country),
            budget_step="perplexity_sonar_recent_facts",
            model=m,
            search_recency_filter="year",
        )
        return SonarQueryOutcome(role="recent_company_facts", body=body, citation_urls=cites)

    async def one_sector() -> SonarQueryOutcome:
        body, cites = await _call_sonar(
            _query_sector_context(company_name, sector, country),
            budget_step="perplexity_sonar_sector_context",
            model=m,
            search_recency_filter=None,
        )
        return SonarQueryOutcome(role="sector_governance_context", body=body, citation_urls=cites)

    recent, sector_o = await asyncio.gather(one_recent(), one_sector())
    out: list[SonarQueryOutcome] = []
    if recent.body or recent.citation_urls:
        out.append(recent)
    if sector_o.body or sector_o.citation_urls:
        out.append(sector_o)
    return out
