from __future__ import annotations

"""
Market pulse: Exa news + SearXNG (no URL planner), then one strict-schema LLM pass.

Position: after n4 strategy, complements official RI narrative with public-media claims.
"""

import asyncio
import json
import logging
import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from peopledd.models.contracts import (
    CanonicalEntity,
    MarketClaim,
    MarketPulse,
    MarketSourceHit,
    StrategyChallenges,
)
from peopledd.runtime.pipeline_context import record_llm_route, try_consume_llm_call
from peopledd.vendor.search import SearchResult

if TYPE_CHECKING:
    from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)

_MARKET_PULSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "statement": {"type": "string"},
                    "topic": {
                        "type": "string",
                        "enum": [
                            "earnings",
                            "strategy_execution",
                            "leadership",
                            "m_and_a",
                            "sector",
                            "other",
                        ],
                    },
                    "sentiment": {
                        "type": "string",
                        "enum": ["positive", "neutral", "negative", "mixed"],
                    },
                    "confidence": {"type": "number"},
                    "source_urls": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": 3,
                    },
                    "alignment_with_ri": {
                        "type": "string",
                        "enum": ["supports", "contradicts", "orthogonal", "unknown"],
                    },
                },
                "required": [
                    "statement",
                    "topic",
                    "sentiment",
                    "confidence",
                    "source_urls",
                    "alignment_with_ri",
                ],
                "additionalProperties": False,
            },
        },
    },
    "required": ["claims"],
    "additionalProperties": False,
}

_MARKET_SYSTEM = """
Voce extrai alegacoes factuais a partir de titulos e trechos de noticias JA fornecidos.
Regras:
- Cada alegacao deve estar sustentada apenas por URLs que aparecem no contexto abaixo.
- Nao invente numeros, datas ou citacoes que nao estejam implicitas nos trechos.
- Se o texto for fragil, use confidence baixo (0.2-0.45).
- alignment_with_ri: compare mentalmente com as prioridades/desafios oficiais listados;
  use "unknown" se nao der para comparar.
- Portugues do Brasil nas statements.
""".strip()


_openai_client = None


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        try:
            from openai import AsyncOpenAI

            _openai_client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        except ImportError as e:
            raise RuntimeError("openai package not installed") from e
    return _openai_client


def _normalize_url_key(url: str) -> str:
    try:
        p = urlparse(url)
        return (p.netloc + p.path).lower().rstrip("/")
    except Exception:
        return url.lower().strip()


def _dedup_search_results(results: list[SearchResult]) -> list[SearchResult]:
    seen: set[str] = set()
    out: list[SearchResult] = []
    for r in results:
        if not r.url or not str(r.url).strip():
            continue
        key = _normalize_url_key(r.url)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _build_queries(
    display_name: str,
    tickers: list[str],
    analysis_depth: str,
) -> list[str]:
    name = (display_name or "").strip() or "empresa"
    qs = [
        f"{name} resultado lucro receita trimestre",
        f"{name} estrategia investidores mercado",
    ]
    if analysis_depth == "deep":
        qs.append(f"{name} CEO presidencia diretoria")
        t = next((x.strip() for x in (tickers or []) if x and str(x).strip()), "")
        if t:
            qs.append(f"{t} acao bolsa analistas recomendacao")
        else:
            qs.append(f"{name} fusao aquisicao joint venture")
    return qs


def _results_to_hits(results: list[Any], cap: int) -> list[MarketSourceHit]:
    hits: list[MarketSourceHit] = []
    for r in results[:cap]:
        prov = r.source if r.source in ("exa", "searxng") else "exa"
        hits.append(
            MarketSourceHit(
                url=r.url or "",
                title=r.title or "",
                snippet=(r.snippet or "")[:1200],
                provider=prov,
                published_date=(r.published_date or None) or None,
            )
        )
    return hits


def _allowed_urls_from_results(results: list[Any]) -> set[str]:
    s: set[str] = set()
    for r in results:
        u = (r.url or "").strip()
        if u:
            s.add(u)
            s.add(_normalize_url_key(u))
    return s


def _filter_claim_urls(claim: MarketClaim, allowed: set[str]) -> MarketClaim:
    kept: list[str] = []
    for u in claim.source_urls:
        u = u.strip()
        if not u:
            continue
        if u in allowed or _normalize_url_key(u) in allowed:
            kept.append(u)
    return claim.model_copy(update={"source_urls": kept[:3]})


def _strategy_bullets(strategy: StrategyChallenges) -> str:
    lines: list[str] = []
    for p in strategy.strategic_priorities[:10]:
        lines.append(f"- prioridade ({p.time_horizon}): {p.priority}")
    for k in strategy.key_challenges[:10]:
        lines.append(f"- desafio [{k.challenge_type}]: {k.challenge}")
    if strategy.recent_triggers:
        lines.append(f"- gatilhos: {', '.join(strategy.recent_triggers[:6])}")
    return "\n".join(lines) if lines else "(sem resumo oficial)"


async def retrieve_market_pulse_async(
    orchestrator: SearchOrchestrator,
    *,
    company_name: str,
    entity: CanonicalEntity,
    strategy: StrategyChallenges,
    analysis_depth: str,
    country: str = "BR",
    llm_model: str | None = None,
) -> MarketPulse:
    display = (entity.resolved_name or entity.legal_name or company_name).strip()
    queries = _build_queries(display, entity.tickers or [], analysis_depth)

    has_exa = bool(getattr(orchestrator.exa, "api_key", ""))
    has_sx = bool(getattr(orchestrator.searxng, "base_url", ""))
    if not has_exa and not has_sx:
        return MarketPulse(
            claims=[],
            source_hits=[],
            queries_used=queries,
            skipped_reason="no_api_keys",
        )

    n_ex = 5 if analysis_depth == "deep" else 4
    n_sx = 7 if analysis_depth == "deep" else 5

    raw: list[SearchResult] = []
    for q in queries:
        tasks = []
        if has_sx:
            lang = "pt-BR" if (country or "").upper() in ("BR", "BRAZIL", "") else "en"
            tasks.append(orchestrator.searxng.search_async(q, num_results=n_sx, language=lang))
        else:
            async def _empty_sx() -> list[SearchResult]:
                return []

            tasks.append(_empty_sx())
        if has_exa:

            async def _exa_one(query: str) -> list[SearchResult]:
                try:
                    return await orchestrator.exa.search_async(
                        query, num_results=n_ex, category="news"
                    )
                except Exception as e:
                    logger.warning("[market_pulse] Exa search failed: %s", e)
                    return []

            tasks.append(_exa_one(q))
        else:

            async def _empty_ex() -> list[SearchResult]:
                return []

            tasks.append(_empty_ex())

        batches = await asyncio.gather(*tasks, return_exceptions=True)
        for batch in batches:
            if isinstance(batch, BaseException):
                logger.warning("[market_pulse] search batch error: %s", batch)
                continue
            if isinstance(batch, list):
                raw.extend(batch)

    deduped = _dedup_search_results(raw)
    max_hits = 48 if analysis_depth == "deep" else 32
    source_hits = _results_to_hits(deduped, max_hits)

    if not deduped:
        return MarketPulse(
            claims=[],
            source_hits=source_hits,
            queries_used=queries,
            skipped_reason="no_results",
        )

    allowed = _allowed_urls_from_results(deduped)
    context_lines: list[str] = []
    max_chars = 14_000
    for r in deduped[:60]:
        if not r.url:
            continue
        sn = (r.snippet or "").replace("\n", " ").strip()[:500]
        line = f"{r.title or ''} | {r.url} | {sn}"
        context_lines.append(line)
    context_body = "\n".join(context_lines)
    if len(context_body) > max_chars:
        context_body = context_body[:max_chars]

    user_msg = (
        f"Empresa: {display}\n"
        f"Pais contexto: {country}\n\n"
        f"Prioridades e desafios oficiais (n4):\n{_strategy_bullets(strategy)}\n\n"
        f"Trechos de midia (titulo | url | snippet):\n{context_body}\n"
    )

    if not try_consume_llm_call("market_pulse"):
        record_llm_route("market_pulse", False, "budget_exhausted")
        return MarketPulse(
            claims=[],
            source_hits=source_hits,
            queries_used=queries,
            skipped_reason="budget_exhausted",
        )

    model = llm_model or os.environ.get("OPENAI_MARKET_PULSE_MODEL", "gpt-5.4")
    max_claims = 8 if analysis_depth == "deep" else 5

    try:
        client = _get_openai_client()
        response = await client.chat.completions.create(
            model=model,
            temperature=0.1,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "market_pulse_extraction",
                    "strict": True,
                    "schema": _MARKET_PULSE_SCHEMA,
                },
            },
            messages=[
                {"role": "system", "content": _MARKET_SYSTEM},
                {
                    "role": "user",
                    "content": user_msg
                    + f"\n\nRetorne no maximo {max_claims} claims; pode retornar menos se nao houver evidencia.",
                },
            ],
        )
        raw_json = response.choices[0].message.content or "{}"
        parsed = json.loads(raw_json)
        raw_claims = parsed.get("claims") or []
        claims: list[MarketClaim] = []
        for item in raw_claims[:max_claims]:
            if not isinstance(item, dict):
                continue
            try:
                c = MarketClaim(
                    statement=str(item.get("statement", "")).strip(),
                    topic=item.get("topic", "other"),
                    sentiment=item.get("sentiment", "neutral"),
                    confidence=float(item.get("confidence", 0.5)),
                    source_urls=list(item.get("source_urls") or []),
                    alignment_with_ri=item.get("alignment_with_ri", "unknown"),
                )
            except Exception:
                continue
            if not c.statement:
                continue
            claims.append(_filter_claim_urls(c, allowed))
        claims = [c for c in claims if c.source_urls]
        record_llm_route("market_pulse", True, "ok")
        return MarketPulse(
            claims=claims,
            source_hits=source_hits,
            queries_used=queries,
            skipped_reason=None,
        )
    except Exception as e:
        logger.error("[market_pulse] LLM extraction failed: %s", e)
        record_llm_route("market_pulse", False, f"llm_error:{type(e).__name__}")
        return MarketPulse(
            claims=[],
            source_hits=source_hits,
            queries_used=queries,
            skipped_reason="llm_error",
        )


def run_sync(
    orchestrator: SearchOrchestrator,
    *,
    company_name: str,
    entity: CanonicalEntity,
    strategy: StrategyChallenges,
    analysis_depth: str,
    country: str = "BR",
    llm_model: str | None = None,
) -> MarketPulse:
    try:
        return asyncio.run(
            retrieve_market_pulse_async(
                orchestrator,
                company_name=company_name,
                entity=entity,
                strategy=strategy,
                analysis_depth=analysis_depth,
                country=country,
                llm_model=llm_model,
            )
        )
    except RuntimeError:
        logger.warning("[market_pulse] asyncio.run unavailable; returning empty pulse")
        return MarketPulse(skipped_reason="llm_error")
