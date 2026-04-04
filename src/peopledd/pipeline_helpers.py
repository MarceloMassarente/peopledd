from __future__ import annotations

import os

from peopledd.models.common import ServiceLevel
from peopledd.models.contracts import CanonicalEntity, InputPayload


def canonical_company_name(entity: CanonicalEntity, input_payload: InputPayload) -> str:
    return (entity.resolved_name or entity.legal_name or input_payload.company_name or "").strip()


def infer_sector_key(entity: CanonicalEntity, input_payload: InputPayload) -> str:
    n = canonical_company_name(entity, input_payload).lower()
    if "banco" in n or "financeir" in n or "credito" in n or "itau" in n or "bradesco" in n:
        return "bancos"
    if "minera" in n or "mining" in n or "vale" in n:
        return "mineracao"
    return "general"


def assign_service_level(
    formal_completeness: float,
    current_completeness: float,
    useful_coverage_board: float,
    entity_resolved: bool,
    mode: str,
) -> tuple[ServiceLevel, list[str], list[str]]:
    degradations: list[str] = []
    disclaimers: list[str] = []

    if not entity_resolved:
        return ServiceLevel.SL5, ["entity_scope_ambiguity"], ["Entidade ambígua ou não resolvida"]

    if mode == "private_or_unresolved":
        degradations.append("no_regulatory_backbone")
        return ServiceLevel.SL4, degradations, ["Composição inferida por fontes públicas abertas"]

    if formal_completeness < 0.5:
        degradations.append("formal_data_weak")
        return ServiceLevel.SL5, degradations, ["Dados formais insuficientes para análise robusta"]

    if current_completeness < 0.5:
        degradations.append("ri_freshness_missing")
        return ServiceLevel.SL2, degradations, ["Camada current/RI incompleta"]

    if useful_coverage_board < 0.6:
        degradations.append("low_useful_coverage_board")
        disclaimers.append("Inferência fina de competências limitada")
        return ServiceLevel.SL3, degradations, disclaimers

    return ServiceLevel.SL1, degradations, disclaimers


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
