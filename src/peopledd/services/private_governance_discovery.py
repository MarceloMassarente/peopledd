from __future__ import annotations

"""
Private governance discovery via Exa Company Search + optional Exa People validation.

Fills current_governance_snapshot when RI is absent or the RI scrape yields no board/exec.
Does not treat conselho consultivo as Conselho de Administração; advisory members go to committees.
"""

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

from peopledd.models.common import SourceRef
from peopledd.models.contracts import (
    BoardMember,
    Committee,
    CommitteeMember,
    ExecutiveMember,
    GovernanceSnapshot,
)
from peopledd.runtime.pipeline_context import record_llm_route, try_consume_llm_call
from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)

_openai_client: Any = None

_NUM_RESULTS_PER_QUERY = 6
_MAX_SOURCES = 10
_MAX_TEXT_PER_SOURCE = 3500
_MAX_PEOPLE_VALIDATION = 12
_PEOPLE_VALIDATION_CONCURRENCY = 3

_PRIVATE_WEB_EXTRACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "board_members": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "person_name": {"type": "string"},
                    "role": {
                        "type": "string",
                        "enum": ["chair", "vice-chair", "board-member", "unknown"],
                    },
                    "independence_status": {
                        "type": "string",
                        "enum": ["independent", "non_independent", "unknown"],
                    },
                    "evidence_org": {
                        "type": "string",
                        "enum": ["administrative_board", "advisory_board", "unknown"],
                    },
                    "source_index": {"type": "integer"},
                    "term_start": {"type": "string"},
                    "term_end": {"type": "string"},
                },
                "required": [
                    "person_name",
                    "role",
                    "independence_status",
                    "evidence_org",
                    "source_index",
                ],
                "additionalProperties": False,
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
                    "source_index": {"type": "integer"},
                },
                "required": ["person_name", "formal_title", "normalized_role", "source_index"],
                "additionalProperties": False,
            },
        },
        "committee_rows": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "committee_name": {"type": "string"},
                    "person_name": {"type": "string"},
                    "position_in_committee": {
                        "type": "string",
                        "enum": ["chair", "member", "unknown"],
                    },
                    "committee_type": {
                        "type": "string",
                        "enum": ["audit", "people", "finance", "strategy", "risk", "esg", "other"],
                    },
                    "source_index": {"type": "integer"},
                },
                "required": [
                    "committee_name",
                    "person_name",
                    "position_in_committee",
                    "committee_type",
                    "source_index",
                ],
                "additionalProperties": False,
            },
        },
        "as_of_date": {"type": ["string", "null"]},
    },
    "required": ["board_members", "executive_members", "committee_rows"],
    "additionalProperties": False,
}

_EXTRACTION_SYSTEM_PROMPT = """
Voce extrai governanca corporativa a partir de trechos de paginas web (noticias, site institucional, etc.).
A empresa alvo e fornecida. Cada trecho tem um indice [N] e URL.

REGRAS:
1. Inclua APENAS nomes explicitamente citados nos trechos.
2. evidence_org para conselho:
   - administrative_board: Conselho de Administracao (CA), conselheiro de administracao, membro do CA.
   - advisory_board: conselho consultivo, conselheiros consultivos, advisory board.
   - unknown: quando nao da para distinguir com seguranca (nao use para CA; deixe como unknown).
3. Membros com evidence_org advisory_board ou unknown NAO entram em board_members. Para advisory_board,
   use committee_rows com committee_name "Conselho consultivo" (ou o nome exato do texto) e committee_type other.
4. executive_members: cargos de diretoria executiva (CEO, CFO, diretores, etc.) com titulo vindo do texto.
5. source_index: indice inteiro do trecho [N] onde o nome e cargo aparecem (1-based). Se aparecer em varios,
   use o trecho mais especifico.
6. Nao invente nomes. Arrays podem ser vazios.
7. Para comites formais (auditoria, pessoas, risco) use committee_rows com committee_type adequado.
""".strip()


def _get_openai_client() -> Any:
    global _openai_client
    if _openai_client is None:
        try:
            from openai import AsyncOpenAI

            _openai_client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        except ImportError as e:
            raise RuntimeError("openai package not installed. Run: pip install openai") from e
    return _openai_client


def _normalize_company_tokens(company_name: str) -> list[str]:
    raw = re.sub(r"[^\w\s]", " ", (company_name or "").lower())
    tokens = [t for t in raw.split() if len(t) >= 4]
    return list(dict.fromkeys(tokens))


def _snippet_mentions_company(snippet: str, company_name: str) -> bool:
    s = (snippet or "").lower()
    for t in _normalize_company_tokens(company_name):
        if t in s:
            return True
    return False


def _url_host(url: str) -> str:
    try:
        p = urlparse(url)
        h = (p.netloc or "").lower()
        if h.startswith("www."):
            h = h[4:]
        return h
    except Exception:
        return ""


def _official_match(source_url: str, official_host: str | None) -> bool:
    if not official_host:
        return False
    sh = _url_host(source_url)
    if not sh:
        return False
    return sh == official_host or sh.endswith("." + official_host)


def _private_web_queries(company_name: str, country: str) -> list[str]:
    c = (company_name or "").strip()
    co = (country or "BR").strip()
    if not c:
        return []
    return [
        f"diretoria executiva lideranca {c} {co}",
        f"conselho de administracao conselheiros {c} {co}",
        f"conselho consultivo advisory {c} {co}",
        f"equipe executiva quem somos {c} {co}",
        f"nomeacao CEO CFO diretor {c} {co}",
    ]


def _dedupe_results(batches: list[list[Any]]) -> list[Any]:
    merged: dict[str, Any] = {}
    for batch in batches:
        for r in batch:
            u = (getattr(r, "url", None) or "").strip().lower().rstrip("/")
            if not u:
                continue
            prev = merged.get(u)
            if prev is None or float(getattr(r, "score", 0)) > float(getattr(prev, "score", 0)):
                merged[u] = r
    ordered = sorted(merged.values(), key=lambda x: -float(getattr(x, "score", 0)))
    return ordered[:_MAX_SOURCES]


def _build_numbered_corpus(sources: list[Any]) -> tuple[str, list[dict[str, str]]]:
    lines: list[str] = []
    meta: list[dict[str, str]] = []
    for i, r in enumerate(sources, start=1):
        url = getattr(r, "url", "") or ""
        title = getattr(r, "title", "") or ""
        text = (getattr(r, "snippet", "") or "")[:_MAX_TEXT_PER_SOURCE]
        lines.append(f"[{i}] URL: {url}\nTitulo: {title}\n{text}\n")
        meta.append({"url": url, "title": title, "text": text})
    return "\n".join(lines), meta


def _resolve_source_ref(
    idx: int,
    meta: list[dict[str, str]],
) -> SourceRef | None:
    if idx < 1 or idx > len(meta):
        return None
    row = meta[idx - 1]
    url = row.get("url") or ""
    if not url:
        return None
    return SourceRef(
        source_type="web_exa",
        label="Governanca observada (web)",
        url_or_ref=url,
    )


async def _gather_company_hits(
    orchestrator: SearchOrchestrator,
    company_name: str,
    country: str,
    official_host: str | None = None,
) -> list[Any]:
    exa = orchestrator.exa
    if not exa.api_key:
        return []
    
    # Se temos o domínio oficial, tentamos 1 busca restrita nele primeiro (economiza créditos e aumenta precisão)
    if official_host:
        q = f"diretoria conselho equipe quem somos {company_name} {country}"
        try:
            results = await exa.search_company_rich_async(
                q, 
                num_results=_NUM_RESULTS_PER_QUERY, 
                include_domains=[official_host]
            )
            if results:
                # Se encontrou no domínio alvo, não precisa fazer os 5 broadcasts na web aberta
                return _dedupe_results([results])
        except Exception as e:
            logger.warning("[private_governance] targeted domain search failed: %s", e)

    # Fallback: executa as queries normais em toda a web
    queries = _private_web_queries(company_name, country)
    if not queries:
        return []
    batches = await asyncio.gather(
        *[exa.search_company_rich_async(q, num_results=_NUM_RESULTS_PER_QUERY) for q in queries],
        return_exceptions=True,
    )
    clean: list[list[Any]] = []
    for b in batches:
        if isinstance(b, Exception):
            logger.warning("[private_governance] company_rich batch failed: %s", b)
            continue
        clean.append(b)
    return _dedupe_results(clean)


async def _extract_governance_llm_async(
    company_name: str,
    corpus: str,
    llm_model: str,
) -> dict[str, Any] | None:
    if not try_consume_llm_call("private_web_governance_extraction"):
        record_llm_route("private_web_governance_extraction", False, "budget_exhausted")
        logger.warning("[private_governance] LLM extraction skipped (budget)")
        return None

    user_msg = f"Empresa alvo: {company_name}\n\nTrechos numerados:\n\n{corpus}"

    try:
        client = _get_openai_client()
        response = await client.chat.completions.create(
            model=llm_model,
            temperature=0.1,
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "private_web_governance_extraction",
                    "strict": True,
                    "schema": _PRIVATE_WEB_EXTRACTION_SCHEMA,
                },
            },
            messages=[
                {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
        )
        raw_json = response.choices[0].message.content or "{}"
        data = json.loads(raw_json)
        record_llm_route("private_web_governance_extraction", True, "ok")
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.error("[private_governance] LLM extraction failed: %s", e)
        record_llm_route("private_web_governance_extraction", False, f"llm_error:{type(e).__name__}")
        return None


async def _linkedin_supports_association(
    orchestrator: SearchOrchestrator,
    person_name: str,
    company_name: str,
) -> bool:
    exa = orchestrator.exa
    if not exa.api_key or not person_name.strip():
        return False
    c = (company_name or "").strip()
    q1 = f"{person_name} {c}".strip() if c else person_name.strip()
    try:
        results = await exa.search_people_linkedin_async([q1], num_results_per_query=6)
    except Exception as e:
        logger.warning("[private_governance] people validation failed for %s: %s", person_name, e)
        return False
    for r in results[:6]:
        if _snippet_mentions_company(getattr(r, "snippet", "") or "", company_name):
            return True
    return False


async def _validate_members_parallel(
    orchestrator: SearchOrchestrator,
    company_name: str,
    items: list[tuple[str, SourceRef | None]],
) -> set[str]:
    if os.environ.get("PEOPLEDd_PRIVATE_WEB_SKIP_PEOPLE_VALIDATE", "").lower() in ("1", "true", "yes"):
        return {name for name, _ in items}

    sem = asyncio.Semaphore(_PEOPLE_VALIDATION_CONCURRENCY)

    async def one(name: str) -> tuple[str, bool]:
        async with sem:
            ok = await _linkedin_supports_association(orchestrator, name, company_name)
            return name, ok

    limited = items[:_MAX_PEOPLE_VALIDATION]
    pairs = await asyncio.gather(*[one(n) for n, _ in limited])
    return {name for name, ok in pairs if ok}


def build_snapshot_from_llm_data(
    data: dict[str, Any],
    meta: list[dict[str, str]],
    official_host: str | None,
    company_name: str,
    validated_names: set[str],
) -> GovernanceSnapshot:
    """Build snapshot and apply promotion rules. Exposed for unit tests."""
    board_filtered: list[BoardMember] = []
    committees_by_name: dict[str, list[tuple[CommitteeMember, SourceRef | None, str]]] = defaultdict(
        list
    )

    for m in data.get("board_members", []) or []:
        name = (m.get("person_name") or "").strip()
        if not name:
            continue
        ev = m.get("evidence_org") or "unknown"
        idx = int(m.get("source_index") or 1)
        ref = _resolve_source_ref(idx, meta)
        if ev == "advisory_board":
            cm = CommitteeMember(person_name=name, position_in_committee="member")
            committees_by_name["Conselho consultivo"].append((cm, ref, "other"))
            continue
        if ev != "administrative_board" or ref is None:
            continue
        text = meta[idx - 1].get("text", "") if 1 <= idx <= len(meta) else ""
        official = _official_match(ref.url_or_ref or "", official_host)
        mentions_co = _snippet_mentions_company(text, company_name)
        linkedin_ok = name in validated_names
        if official or mentions_co or linkedin_ok:
            board_filtered.append(
                BoardMember(
                    person_name=name,
                    role=m.get("role", "unknown"),
                    independence_status=m.get("independence_status", "unknown"),
                    term_start=m.get("term_start") or None,
                    term_end=m.get("term_end") or None,
                    source_refs=[ref],
                )
            )

    exec_filtered: list[ExecutiveMember] = []
    for m in data.get("executive_members", []) or []:
        name = (m.get("person_name") or "").strip()
        if not name:
            continue
        idx = int(m.get("source_index") or 1)
        ref = _resolve_source_ref(idx, meta)
        if ref is None:
            continue
        text = meta[idx - 1].get("text", "") if 1 <= idx <= len(meta) else ""
        official = _official_match(ref.url_or_ref or "", official_host)
        mentions_co = _snippet_mentions_company(text, company_name)
        linkedin_ok = name in validated_names
        if official or mentions_co or linkedin_ok:
            exec_filtered.append(
                ExecutiveMember(
                    person_name=name,
                    formal_title=m.get("formal_title", ""),
                    normalized_role=m.get("normalized_role", "other"),
                    term_start=m.get("term_start") or None,
                    source_refs=[ref],
                )
            )

    for row in data.get("committee_rows", []) or []:
        cname = (row.get("committee_name") or "").strip()
        pname = (row.get("person_name") or "").strip()
        if not cname or not pname:
            continue
        idx = int(row.get("source_index") or 1)
        ref = _resolve_source_ref(idx, meta)
        pos = row.get("position_in_committee", "unknown")
        ctype = row.get("committee_type", "other")
        committees_by_name[cname].append(
            (CommitteeMember(person_name=pname, position_in_committee=pos), ref, ctype)
        )

    committees_out: list[Committee] = []
    for cname, members_data in committees_by_name.items():
        members: list[CommitteeMember] = []
        refs: list[SourceRef] = []
        ctype_set: set[str] = set()
        for cm, ref, ct in members_data:
            members.append(cm)
            if ref is not None:
                refs.append(ref)
            ctype_set.add(ct)
        ctype: Any = "other"
        for cand in ("audit", "people", "finance", "strategy", "risk", "esg"):
            if cand in ctype_set:
                ctype = cand
                break
        committees_out.append(
            Committee(
                committee_name=cname,
                committee_type=ctype,
                members=members,
                source_refs=refs[:5],
            )
        )

    as_of = data.get("as_of_date")
    as_of_s = as_of if isinstance(as_of, str) and as_of.strip() else None

    return GovernanceSnapshot(
        as_of_date=as_of_s,
        board_members=board_filtered,
        executive_members=exec_filtered,
        committees=committees_out,
    )


def _candidates_for_people_validation(data: dict[str, Any], meta: list[dict[str, str]]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()

    def add(n: str) -> None:
        n = n.strip()
        if n and n not in seen:
            seen.add(n)
            names.append(n)

    for m in data.get("board_members", []) or []:
        if (m.get("evidence_org") or "") != "administrative_board":
            continue
        idx = int(m.get("source_index") or 1)
        if _resolve_source_ref(idx, meta) is None:
            continue
        add(str(m.get("person_name") or ""))

    for m in data.get("executive_members", []) or []:
        idx = int(m.get("source_index") or 1)
        if _resolve_source_ref(idx, meta) is None:
            continue
        add(str(m.get("person_name") or ""))

    return names


async def discover_governance_async(
    orchestrator: SearchOrchestrator,
    company_name: str,
    *,
    country: str = "BR",
    website_hint: str | None = None,
    llm_model: str | None = None,
) -> tuple[GovernanceSnapshot, dict[str, str | None]]:
    """
    Run Exa company rich retrieval, LLM extraction, and optional Exa people validation.

    Returns (snapshot, metadata) with keys anchor_website, official_host, source_count, reason.
    """
    meta_out: dict[str, str | None] = {
        "anchor_website": None,
        "official_host": None,
        "source_count": "0",
        "reason": None,
    }
    cname = (company_name or "").strip()
    if not cname:
        meta_out["reason"] = "empty_company_name"
        return GovernanceSnapshot(), meta_out

    exa = orchestrator.exa
    if not exa.api_key:
        meta_out["reason"] = "no_exa_key"
        return GovernanceSnapshot(), meta_out

    hq = "Brazil" if (country or "").upper() in ("BR", "BRA", "BRASIL") else country
    profile = None
    try:
        profile = await exa.company_lookup_async(cname, hq_country=hq or None, sector=None)
    except Exception as e:
        logger.warning("[private_governance] company_lookup failed: %s", e)

    anchor = (website_hint or "").strip() or (getattr(profile, "website", None) or "").strip()
    official_host = _url_host(anchor) if anchor else None
    meta_out["anchor_website"] = anchor or None
    meta_out["official_host"] = official_host

    sources = await _gather_company_hits(orchestrator, cname, country, official_host)
    meta_out["source_count"] = str(len(sources))
    if not sources:
        meta_out["reason"] = "no_company_hits"
        return GovernanceSnapshot(), meta_out

    corpus, meta = _build_numbered_corpus(sources)
    model = llm_model or os.environ.get("OPENAI_MODEL_MINI") or os.environ.get("OPENAI_MODEL") or "gpt-5.4-mini"

    if not os.environ.get("OPENAI_API_KEY"):
        meta_out["reason"] = "no_openai_key"
        return GovernanceSnapshot(), meta_out

    data = await _extract_governance_llm_async(cname, corpus, model)
    if not data:
        meta_out["reason"] = "llm_extraction_failed"
        return GovernanceSnapshot(), meta_out

    to_validate = [(n, None) for n in _candidates_for_people_validation(data, meta)]
    validated = await _validate_members_parallel(orchestrator, cname, to_validate)

    snap = build_snapshot_from_llm_data(data, meta, official_host, cname, validated)
    meta_out["reason"] = "ok"
    logger.info(
        "[private_governance] snapshot: board=%d exec=%d committees=%d",
        len(snap.board_members),
        len(snap.executive_members),
        len(snap.committees),
    )
    return snap, meta_out


def discover_governance(
    orchestrator: SearchOrchestrator | None,
    company_name: str,
    *,
    country: str = "BR",
    website_hint: str | None = None,
    llm_model: str | None = None,
) -> tuple[GovernanceSnapshot, dict[str, str | None]]:
    """Sync wrapper for discover_governance_async."""
    if orchestrator is None:
        return GovernanceSnapshot(), {
            "reason": "no_orchestrator",
            "anchor_website": None,
            "official_host": None,
            "source_count": "0",
        }
    return asyncio.run(
        discover_governance_async(
            orchestrator,
            company_name,
            country=country,
            website_hint=website_hint,
            llm_model=llm_model,
        )
    )


def eligible_for_private_web_discovery(
    *,
    current_snapshot: GovernanceSnapshot,
    search_orchestrator: Any | None,
    enabled: bool,
    company_mode: str | None = None,
    formal_snapshot: GovernanceSnapshot | None = None,
    completeness_threshold: float = 0.35,
) -> bool:
    """
    Eligible when Exa is configured and either:
    - current track has no board and no executives (legacy), or
    - current_track_completeness is below threshold (sparse RI), or
    - listed company: FRE has board but current RI snapshot has no board (gap vs formal).
    """
    if not enabled or search_orchestrator is None:
        return False
    exa = getattr(search_orchestrator, "exa", None)
    if exa is None or not getattr(exa, "api_key", ""):
        return False

    from peopledd.services.governance_completeness import current_track_completeness

    board_empty = not current_snapshot.board_members
    exec_empty = not current_snapshot.executive_members

    if board_empty and exec_empty:
        return True

    if current_track_completeness(current_snapshot) < completeness_threshold:
        return True

    if (company_mode or "").lower() == "listed_br" and formal_snapshot is not None:
        if formal_snapshot.board_members and board_empty:
            return True

    return False
