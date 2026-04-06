from __future__ import annotations

import json
import os

from peopledd.models.common import ServiceLevel
from peopledd.models.contracts import (
    CanonicalEntity,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
    InputPayload,
    SemanticGovernanceFusion,
)
from peopledd.utils.text import normalize_company_name


def canonical_company_name(entity: CanonicalEntity, input_payload: InputPayload) -> str:
    return (entity.resolved_name or entity.legal_name or input_payload.company_name or "").strip()


def infer_sector_key(entity: CanonicalEntity, input_payload: InputPayload) -> str:
    n = canonical_company_name(entity, input_payload).lower()
    if "banco" in n or "financeir" in n or "credito" in n or "itau" in n or "bradesco" in n:
        return "bancos"
    if "minera" in n or "mining" in n or "vale" in n:
        return "mineracao"
    return "general"


_PRIVATE_WEB_DISCLAIMER = (
    "Governança observada na web via descoberta Exa (company search + validação people)"
)


def _merge_private_web_disclaimer(
    disclaimers: list[str],
    private_web_governance_used: bool,
) -> list[str]:
    out = list(disclaimers)
    if private_web_governance_used and not any(_PRIVATE_WEB_DISCLAIMER in d for d in out):
        out.append(_PRIVATE_WEB_DISCLAIMER)
    return out


def assign_service_level(
    formal_completeness: float,
    current_completeness: float,
    useful_coverage_board: float,
    entity_resolved: bool,
    mode: str,
    private_web_governance_used: bool = False,
) -> tuple[ServiceLevel, list[str], list[str]]:
    degradations: list[str] = []
    disclaimers: list[str] = []

    if not entity_resolved:
        return (
            ServiceLevel.SL5,
            ["entity_scope_ambiguity"],
            _merge_private_web_disclaimer(["Entidade ambígua ou não resolvida"], private_web_governance_used),
        )

    if mode == "private_or_unresolved":
        degradations.append("no_regulatory_backbone")
        base = ["Composição inferida por fontes públicas abertas"]
        if private_web_governance_used:
            base.append(_PRIVATE_WEB_DISCLAIMER)
        return ServiceLevel.SL4, degradations, base

    if formal_completeness < 0.5:
        degradations.append("formal_data_weak")
        return (
            ServiceLevel.SL5,
            degradations,
            _merge_private_web_disclaimer(
                ["Dados formais insuficientes para análise robusta"],
                private_web_governance_used,
            ),
        )

    if current_completeness < 0.5:
        degradations.append("ri_freshness_missing")
        return (
            ServiceLevel.SL2,
            degradations,
            _merge_private_web_disclaimer(
                ["Camada current/RI incompleta"],
                private_web_governance_used,
            ),
        )

    if useful_coverage_board < 0.6:
        degradations.append("low_useful_coverage_board")
        disclaimers.append("Inferência fina de competências limitada")
        return ServiceLevel.SL3, degradations, _merge_private_web_disclaimer(disclaimers, private_web_governance_used)

    return ServiceLevel.SL1, degradations, _merge_private_web_disclaimer(disclaimers, private_web_governance_used)


def semantic_fusion_cache_raw_key(
    ingestion: GovernanceIngestion,
    reconciliation: GovernanceReconciliation,
    company_name: str,
    country: str,
    prefer_llm: bool,
    use_harvest: bool,
) -> str:
    """Stable string for PipelineCache kind semantic_fusion."""

    def snap_fp(snap: GovernanceSnapshot) -> tuple:
        bm = sorted(normalize_company_name(m.person_name) for m in snap.board_members)
        em = sorted(normalize_company_name(e.person_name) for e in snap.executive_members)
        ct = sorted(
            (normalize_company_name(c.committee_name), len(c.members)) for c in snap.committees
        )
        return (bm, em, ct, snap.as_of_date or "")

    payload = {
        "cn": normalize_company_name(company_name),
        "co": (country or "BR").strip().upper(),
        "prefer_llm": prefer_llm,
        "use_harvest": use_harvest,
        "formal": snap_fp(ingestion.formal_governance_snapshot),
        "current": snap_fp(ingestion.current_governance_snapshot),
        "recon_status": reconciliation.reconciliation_status,
        "recon_board_n": len(reconciliation.reconciled_governance_snapshot.board_members),
        "recon_exec_n": len(reconciliation.reconciled_governance_snapshot.executive_members),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def reconciliation_with_fusion_snapshot(
    base: GovernanceReconciliation,
    fusion: SemanticGovernanceFusion,
) -> GovernanceReconciliation:
    """Use n1c resolved snapshot as the reconciled view for n2/n6 while keeping n1b conflicts."""
    rb = dict(base.reporting_basis)
    rb["preferred_view_for_reporting"] = "semantic_fusion_resolved"
    return base.model_copy(
        update={
            "reconciled_governance_snapshot": fusion.resolved_snapshot.model_copy(deep=True),
            "reporting_basis": rb,
        }
    )


def company_domain_host(entity: CanonicalEntity) -> str | None:
    """Registrable domain from Exa website or RI URL, for person search disambiguation."""
    from urllib.parse import urlparse

    if entity.exa_company_enrichment:
        w = entity.exa_company_enrichment.get("website")
        if isinstance(w, str) and w.strip():
            host = (urlparse(w.strip()).netloc or "").lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                return host
    if entity.ri_url and str(entity.ri_url).strip().lower().startswith("http"):
        host = (urlparse(entity.ri_url.strip()).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if host:
            return host
    return None


def build_search_orchestrator():
    exa = os.environ.get("EXA_API_KEY", "")
    searx = os.environ.get("SEARXNG_URL", "") or os.environ.get("SEARXNG_INSTANCE", "")
    if not exa and not searx:
        return None
    from peopledd.vendor.search import SearchOrchestrator

    return SearchOrchestrator(
        searxng_url=searx or None,
        exa_api_key=exa or None,
    )
