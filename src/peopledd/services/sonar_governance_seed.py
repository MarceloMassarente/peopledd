from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import httpx

from peopledd.models.common import SourceRef
from peopledd.models.contracts import GovernanceSeed, SeedMember
from peopledd.runtime.pipeline_context import record_llm_route, try_consume_llm_call

logger = logging.getLogger(__name__)

PERPLEXITY_SONAR_URL = "https://api.perplexity.ai/v1/sonar"
DEFAULT_MODEL = "sonar-pro"


def sonar_seed_enabled() -> bool:
    return bool(os.environ.get("PERPLEXITY_API_KEY", "").strip())


def _prompt(company_name: str, country: str) -> str:
    return f"""Empresa-alvo: {company_name} ({country}).

Tarefa:
1) Informe a URL de RI (Relações com Investidores) da empresa.
2) Liste conselheiros (Conselho de Administração).
3) Liste diretoria executiva (CEO/CFO/diretores estatutários).

Regras:
- Use preferencialmente o site oficial de RI da empresa.
- Não invente nomes.
- Retorne APENAS JSON válido no formato:
{{
  "ri_url": "https://...",
  "board_members": [{{"person_name": "...", "role_or_title": "...", "evidence_url": "https://..."}}],
  "executive_members": [{{"person_name": "...", "role_or_title": "...", "evidence_url": "https://..."}}],
  "confidence": 0.0
}}
- confidence de 0 a 1.
"""


def _extract_url(s: str) -> str | None:
    raw = (s or "").strip()
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return None


def _to_members(rows: Any) -> list[SeedMember]:
    out: list[SeedMember] = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("person_name") or "").strip()
        if not name:
            continue
        role = str(row.get("role_or_title") or "").strip() or None
        ev = _extract_url(str(row.get("evidence_url") or ""))
        refs = [SourceRef(source_type="sonar", label="sonar_seed_evidence", url_or_ref=ev)] if ev else []
        out.append(
            SeedMember(
                person_name=name,
                role_or_title=role,
                evidence_url=ev,
                source_refs=refs,
            )
        )
    return out


def fetch_governance_seed(
    company_name: str,
    *,
    country: str = "BR",
    model: str | None = None,
) -> GovernanceSeed | None:
    if not sonar_seed_enabled():
        return None
    if not try_consume_llm_call("sonar_governance_seed"):
        record_llm_route("sonar_governance_seed", False, "budget_exhausted")
        return None

    api_key = os.environ.get("PERPLEXITY_API_KEY", "").strip()
    m = model or os.environ.get("PERPLEXITY_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
    payload = {
        "model": m,
        "messages": [{"role": "user", "content": _prompt(company_name, country)}],
        "temperature": 0.1,
        "search_recency_filter": "month",
    }
    try:
        with httpx.Client(timeout=httpx.Timeout(60.0, connect=15.0)) as client:
            resp = client.post(
                PERPLEXITY_SONAR_URL,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning("[sonar_seed] request failed: %s", e)
        record_llm_route("sonar_governance_seed", False, f"error:{type(e).__name__}")
        return None

    choice = (data.get("choices") or [{}])[0]
    content = str((choice.get("message") or {}).get("content") or "").strip()
    try:
        start = content.find("{")
        end = content.rfind("}")
        parsed = json.loads(content[start : end + 1]) if start >= 0 and end > start else {}
    except Exception:
        parsed = {}

    citations = data.get("citations")
    source_refs: list[SourceRef] = []
    if isinstance(citations, list):
        for c in citations:
            if isinstance(c, str) and c.startswith("http"):
                source_refs.append(SourceRef(source_type="sonar", label="sonar_citation", url_or_ref=c))

    ri = _extract_url(str(parsed.get("ri_url") or ""))
    board = _to_members(parsed.get("board_members"))
    execs = _to_members(parsed.get("executive_members"))
    confidence = float(parsed.get("confidence") or 0.0)
    confidence = min(1.0, max(0.0, confidence))
    if not ri and source_refs:
        ri = source_refs[0].url_or_ref

    seed = GovernanceSeed(
        company_name_queried=company_name,
        ri_url_candidate=ri,
        board_members=board,
        executive_members=execs,
        source_refs=source_refs,
        confidence=confidence,
        provider="perplexity_sonar",
        raw_response_excerpt=content[:800],
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    if not seed.ri_url_candidate and not seed.board_members and not seed.executive_members:
        record_llm_route("sonar_governance_seed", False, "empty_seed")
        return None
    record_llm_route("sonar_governance_seed", True, "ok")
    return seed
