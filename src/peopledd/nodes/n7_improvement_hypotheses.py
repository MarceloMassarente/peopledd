from __future__ import annotations

import re
from typing import Literal
from peopledd.models.common import ResolutionStatus, ServiceLevel
from peopledd.models.contracts import (
    CoverageScoring,
    DegradationProfile,
    EvidencePack,
    GovernanceReconciliation,
    ImprovementHypothesis,
    PersonProfile,
    PersonResolution,
    StrategyChallenges,
)

_MAX_HYPOTHESES = 7

_CHALLENGE_TO_DIMS: dict[str, list[str]] = {
    "financial": ["capital_allocation", "financas_performance", "risco_credito", "capital_liquidez"],
    "technology": ["transformacao_tecnologia", "tecnologia_dados"],
    "regulatory": ["governanca_risco_compliance", "compliance_regulatorio", "regulacao_licenciamento"],
    "governance": ["governanca_risco_compliance"],
    "people": ["lideranca_pessoas"],
    "operational": ["execucao_operacional", "eficiencia_operacional", "seguranca_operacional"],
    "market": ["desenvolvimento_negocio", "crescimento_cliente", "relacoes_institucionais"],
}


def _dim_slug(dimension: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", dimension.lower()).strip("_")[:48] or "dim"


def _profile_by_person_id(
    people_resolution: list[PersonResolution],
    people_profiles: list[PersonProfile],
) -> dict[str, PersonProfile]:
    by_id = {p.person_id: p for p in people_profiles}
    return {pr.person_id: by_id[pr.person_id] for pr in people_resolution if pr.person_id in by_id}


def _apply_ledger_gates(
    hypotheses: list[ImprovementHypothesis],
    evidence_pack: EvidencePack | None,
    governance_reconciliation: GovernanceReconciliation | None,
) -> list[ImprovementHypothesis]:
    """
    Structural promotion rules: demote high-urgency items without claim anchors when reconciliation is major;
    flag missing claim references.
    """
    if not hypotheses:
        return hypotheses
    major = bool(
        governance_reconciliation
        and governance_reconciliation.reconciliation_status == "major_conflicts"
    )
    known_claims = {c.claim_id for c in (evidence_pack.claims if evidence_pack else [])}
    adjusted: list[ImprovementHypothesis] = []
    for h in hypotheses:
        new_missing = list(h.missing_evidence)
        ledger: Literal["promoted", "demoted"] = "promoted"
        urgency = h.urgency
        conf = h.confidence
        if h.urgency == "high" and not h.evidence_claim_refs and major:
            urgency = "medium"
            conf = min(conf, 0.62)
            new_missing.append("high_urgency_demoted_no_claim_refs_under_major_reconciliation")
            ledger = "demoted"
        for ref in h.evidence_claim_refs:
            if ref and ref not in known_claims:
                new_missing.append(f"unresolved_claim_ref:{ref}")
                ledger = "demoted"
                conf = min(conf, 0.6)
        adjusted.append(
            h.model_copy(
                update={
                    "missing_evidence": new_missing,
                    "urgency": urgency,
                    "confidence": round(min(0.95, conf), 3),
                    "ledger_status": ledger,
                }
            )
        )
    return adjusted


def _claim_ids_for_dimension(pack: EvidencePack | None, prefix: str, dimension: str) -> list[str]:
    if not pack:
        return []
    slug = _dim_slug(dimension)
    want = f"{prefix}_{slug}"
    return [c.claim_id for c in pack.claims if c.claim_id == want]


def run(
    coverage: CoverageScoring,
    strategy: StrategyChallenges,
    analytical_confidence: float,
    evidence_pack: EvidencePack | None = None,
    governance_reconciliation: GovernanceReconciliation | None = None,
    people_resolution: list[PersonResolution] | None = None,
    people_profiles: list[PersonProfile] | None = None,
    degradation_profile: DegradationProfile | None = None,
) -> list[ImprovementHypothesis]:
    """
    Hipóteses multi-gatilho: gaps n6, conflitos n1b, pessoas, cruzamento com desafios estratégicos.
    Ancoradas em EvidenceClaim ids quando disponíveis (pós-n8).
    """
    if degradation_profile and degradation_profile.service_level == ServiceLevel.SL5:
        if "improvement_hypotheses" in (degradation_profile.omitted_sections or []):
            return []

    people_resolution = people_resolution or []
    people_profiles = people_profiles or []
    profiles_map = _profile_by_person_id(people_resolution, people_profiles)

    hypotheses: list[ImprovementHypothesis] = []
    seen: set[tuple[str, str]] = set()

    def push(
        hid: str,
        category: str,
        title: str,
        problem: str,
        action: str,
        benefit: str,
        urgency: str,
        conf: float,
        nontrivial: float,
        basis: list[str],
        claim_refs: list[str],
    ) -> None:
        key = (category, title.strip().lower()[:120])
        if key in seen:
            return
        seen.add(key)
        hypotheses.append(
            ImprovementHypothesis(
                hypothesis_id=hid,
                category=category,
                title=title,
                problem_statement=problem,
                evidence_basis=basis,
                evidence_claim_refs=claim_refs,
                proposed_action=action,
                expected_benefit=benefit,
                urgency=urgency,  # type: ignore[arg-type]
                confidence=round(min(0.95, conf), 3),
                non_triviality_score=round(nontrivial, 2),
            )
        )

    h_counter = 0

    for item in coverage.board_coverage:
        if item.gap_severity == "high":
            h_counter += 1
            cids = _claim_ids_for_dimension(evidence_pack, "C_GAP_BOARD", item.dimension)
            basis = [f"Gap alto em {item.dimension} no conselho (cobertura {item.coverage_ratio:.2f})."]
            if cids:
                basis.append(f"Claim:{cids[0]}")
            action = (
                "Priorizar revisão da matriz de competências do conselho e trilha de sucessão "
                f"para a dimensão {item.dimension}."
            )
            if item.single_point_of_failure:
                action += " Atenção: concentração em poucos perfis (SPOF)."
            push(
                f"H{h_counter}",
                "composicao_conselho",
                f"Reforçar dimensão: {item.dimension}",
                item.rationale or basis[0],
                action,
                "Reduzir risco de lacuna decisória e dependência individual.",
                "high",
                min(0.78, analytical_confidence),
                0.82,
                basis,
                cids,
            )
        elif item.gap_severity == "medium":
            h_counter += 1
            cids = _claim_ids_for_dimension(evidence_pack, "C_GAP_BOARD", item.dimension)
            basis = [f"Gap médio em {item.dimension} no conselho."]
            if cids:
                basis.append(f"Claim:{cids[0]}")
            push(
                f"H{h_counter}",
                "composicao_conselho",
                f"Monitorar dimensão: {item.dimension}",
                item.rationale or basis[0],
                f"Planejar reforço gradual (comitês ou perfis complementares) em {item.dimension}.",
                "Melhorar robustez sem ruptura de composição atual.",
                "medium",
                min(0.65, analytical_confidence),
                0.55,
                basis,
                cids,
            )

    for item in coverage.executive_coverage:
        if item.gap_severity == "high":
            h_counter += 1
            cids = _claim_ids_for_dimension(evidence_pack, "C_GAP_EXEC", item.dimension)
            basis = [f"Gap alto na diretoria na dimensão {item.dimension}."]
            if cids:
                basis.append(f"Claim:{cids[0]}")
            push(
                f"H{h_counter}",
                "diretoria",
                f"Diretoria: reforçar {item.dimension}",
                item.rationale or basis[0],
                f"Avaliar desenho de pares, comitês executivos ou contratação alinhada a {item.dimension}.",
                "Alinhar execução às exigências de capacidade do modelo.",
                "high",
                min(0.72, analytical_confidence),
                0.78,
                basis,
                cids,
            )

    if governance_reconciliation:
        for i, ci in enumerate(governance_reconciliation.conflict_items, start=1):
            if ci.conflict_type not in ("organ_mismatch", "independence_mismatch", "title_mismatch"):
                continue
            h_counter += 1
            cid = f"C_CONFLICT_{i}"
            refs = [cid] if evidence_pack and any(c.claim_id == cid for c in evidence_pack.claims) else []
            basis = [
                f"Conflito de reconciliação: {ci.conflict_type} "
                f"({ci.person_name or 'membro não identificado'})."
            ]
            if refs:
                basis.append(f"Claim:{refs[0]}")
            push(
                f"H{h_counter}",
                "governanca",
                f"Alinhar governança formal vs RI: {ci.conflict_type}",
                f"Formal={ci.formal_value!r}; Current={ci.current_value!r}.",
                "Atualizar disclosure RI ou revisar cadastro formal conforme regra aplicável.",
                "Reduzir risco de inconsistência percebida por investidores e reguladores.",
                "high" if ci.conflict_type == "independence_mismatch" else "medium",
                min(0.7, analytical_confidence, ci.confidence + 0.1),
                0.75,
                basis,
                refs,
            )

    for pr in people_resolution:
        prof = profiles_map.get(pr.person_id)
        if pr.resolution_status == ResolutionStatus.AMBIGUOUS:
            h_counter += 1
            basis = [f"Resolução ambígua para '{pr.observed_name}'."]
            push(
                f"H{h_counter}",
                "dados",
                f"Esclarecer identidade: {pr.observed_name}",
                "Múltiplos candidatos LinkedIn/Harvest com score similar.",
                "Due diligence manual ou confirmação com a companhia antes de usar o perfil em decisões.",
                "Evitar atribuição errada de experiência a conselheiros/executivos.",
                "medium",
                min(0.6, analytical_confidence),
                0.7,
                basis,
                [],
            )
        elif pr.resolution_status == ResolutionStatus.NOT_FOUND:
            h_counter += 1
            basis = [f"Perfil público não encontrado para '{pr.observed_name}'."]
            push(
                f"H{h_counter}",
                "dados",
                f"Enriquecer evidência: {pr.observed_name}",
                "Sem match confiável em fonte estruturada de perfil.",
                "Obter CV/resumo oficial (RI ou FRE) ou validar ortografia do nome na fonte de governança.",
                "Melhorar rastreabilidade de competências individuais.",
                "low",
                min(0.55, analytical_confidence),
                0.5,
                basis,
                [],
            )
        elif prof and "exa_url_only_no_harvest_profile" in prof.blind_spots:
            h_counter += 1
            basis = [f"URL derivada de busca sem perfil Harvest para '{pr.observed_name}'."]
            push(
                f"H{h_counter}",
                "dados",
                f"Confirmar perfil (fonte fraca): {pr.observed_name}",
                "Cobertura de carreira não validada via Harvest.",
                "Priorizar confirmação institucional antes de usar como evidência forte.",
                "Evitar superestimar cobertura analítica.",
                "medium",
                min(0.58, analytical_confidence),
                0.62,
                basis,
                [],
            )

    for ch in strategy.key_challenges:
        if ch.severity not in ("high", "medium") or ch.confidence < 0.45:
            continue
        dims = _CHALLENGE_TO_DIMS.get(ch.challenge_type, [])
        if not dims:
            continue
        related = [x for x in coverage.board_coverage if x.dimension in dims and x.gap_severity != "low"]
        if not related:
            continue
        dim = related[0].dimension
        h_counter += 1
        cids = _claim_ids_for_dimension(evidence_pack, "C_GAP_BOARD", dim)
        basis = [
            f"Desafio estratégico ({ch.challenge_type}): {ch.challenge[:120]}…",
            f"Associado a gap de conselho em {dim}.",
        ]
        if cids:
            basis.append(f"Claim:{cids[0]}")
        push(
            f"H{h_counter}",
            "estrategia",
            f"Conectar desafio '{ch.challenge_type}' à composição",
            ch.challenge,
            f"Explicitar no conselho/comitês como {dim} sustenta o desafio declarado.",
            "Alinhar narrativa de RI com matriz de competências observada.",
            "high" if ch.severity == "high" else "medium",
            min(0.68, analytical_confidence, ch.confidence),
            0.68,
            basis,
            cids,
        )

    if degradation_profile and degradation_profile.service_level in (ServiceLevel.SL4, ServiceLevel.SL5):
        hypotheses = hypotheses[: max(1, _MAX_HYPOTHESES // 2)]

    if analytical_confidence < 0.55 and len(hypotheses) > 2:
        hypotheses = hypotheses[:2]

    hypotheses = hypotheses[:_MAX_HYPOTHESES]
    return _apply_ledger_gates(hypotheses, evidence_pack, governance_reconciliation)
