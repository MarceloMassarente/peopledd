from __future__ import annotations

import uuid
from pathlib import Path

from peopledd.models.common import ServiceLevel
from peopledd.models.contracts import (
    ConfidencePolicy,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    InputPayload,
)
from peopledd.nodes import (
    n0_entity_resolution,
    n1_governance_ingestion,
    n1b_reconciliation,
    n2_person_resolution,
    n3_profile_enrichment,
    n4_strategy_inference,
    n5_required_capability_model,
    n6_coverage_scoring,
    n7_improvement_hypotheses,
    n8_evidence_pack,
    n9_report_builder,
)
from peopledd.services.connectors import CVMConnector, HarvestConnector, RIConnector
from peopledd.utils.io import ensure_dir, write_json, write_text


def _assign_service_level(
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


def run_pipeline(input_payload: InputPayload, output_dir: str = "run") -> FinalReport:
    run_id = str(uuid.uuid4())
    base = Path(output_dir) / run_id
    ensure_dir(base / "cache")

    cvm = CVMConnector()
    ri = RIConnector()
    harvest = HarvestConnector()

    entity = n0_entity_resolution.run(input_payload, cvm, ri)
    ingestion = n1_governance_ingestion.run(input_payload.company_name)
    reconciliation = n1b_reconciliation.run(ingestion)
    people_resolution = n2_person_resolution.run(reconciliation, harvest)
    people_profiles = n3_profile_enrichment.run(people_resolution)
    strategy = n4_strategy_inference.run(input_payload.company_name)
    capability_model = n5_required_capability_model.run("bancos", strategy)

    board_size = len(reconciliation.reconciled_governance_snapshot.board_members)
    exec_size = len(reconciliation.reconciled_governance_snapshot.executive_members)
    coverage = n6_coverage_scoring.run(capability_model, people_profiles, board_size=board_size, executive_size=exec_size)

    useful_board = 0.0
    if people_profiles and board_size:
        board_ids = {p.person_name for p in reconciliation.reconciled_governance_snapshot.board_members}
        board_profiles = [pp for pp, pr in zip(people_profiles, people_resolution) if pr.observed_name in board_ids]
        if board_profiles:
            useful_board = sum(p.profile_quality.useful_coverage_score for p in board_profiles) / len(board_profiles)

    data_completeness = (ingestion.governance_data_quality.formal_completeness + ingestion.governance_data_quality.current_completeness) / 2
    evidence_quality = max(0.0, min(1.0, sum(p.profile_quality.evidence_density for p in people_profiles) / max(1, len(people_profiles))))
    analytical_confidence = max(0.0, min(1.0, (data_completeness * 0.4) + (evidence_quality * 0.6)))

    hypotheses = n7_improvement_hypotheses.run(coverage, strategy, analytical_confidence)

    sl, degradations, disclaimers = _assign_service_level(
        formal_completeness=ingestion.governance_data_quality.formal_completeness,
        current_completeness=ingestion.governance_data_quality.current_completeness,
        useful_coverage_board=useful_board,
        entity_resolved=entity.resolution_status in {"resolved", "partial"},
        mode=entity.company_mode,
    )

    degradation_profile = DegradationProfile(
        service_level=sl,
        degradations=degradations,
        omitted_sections=[] if sl != ServiceLevel.SL5 else ["improvement_hypotheses"],
        mandatory_disclaimers=disclaimers,
    )

    confidence_policy = ConfidencePolicy(
        data_completeness_score=round(data_completeness, 2),
        evidence_quality_score=round(evidence_quality, 2),
        analytical_confidence_score=round(analytical_confidence, 2),
    )

    draft_report = FinalReport(
        input_payload=input_payload,
        entity_resolution=entity,
        governance=ingestion,
        governance_reconciliation=reconciliation,
        people_resolution=people_resolution,
        people_profiles=people_profiles,
        strategy_and_challenges=strategy,
        required_capability_model=capability_model,
        coverage_scoring=coverage,
        improvement_hypotheses=hypotheses,
        evidence_pack=EvidencePack(documents=[], claims=[]),
        degradation_profile=degradation_profile,
        confidence_policy=confidence_policy,
    )

    evidence = n8_evidence_pack.run(
        partial_report=draft_report,
        docs=[
            {"doc_id": "D1", "source_type": "cvm_fre_structured", "title": "FRE Structured", "url_or_ref": "cvm://fre/stub"},
            {"doc_id": "D2", "source_type": "ri", "title": "RI Governance Page", "url_or_ref": entity.ri_url or "ri://stub"},
        ],
        claims=[
            {
                "claim_id": "C1",
                "claim_text": "Board e Diretoria observados via dual-track",
                "claim_type": "fact",
                "source_refs": ["D1", "D2"],
                "confidence": 0.75,
            }
        ],
    )

    final_report = draft_report.model_copy(update={"evidence_pack": evidence})
    md_report = n9_report_builder.to_markdown(final_report)

    write_json(base / "input.json", input_payload.model_dump(mode="json"))
    write_json(base / "entity_resolution.json", entity.model_dump(mode="json"))
    write_json(base / "governance_formal.json", ingestion.formal_governance_snapshot.model_dump(mode="json"))
    write_json(base / "governance_current.json", ingestion.current_governance_snapshot.model_dump(mode="json"))
    write_json(base / "governance_reconciliation.json", reconciliation.model_dump(mode="json"))
    write_json(base / "people_resolution.json", [p.model_dump(mode="json") for p in people_resolution])
    write_json(base / "people_profiles.json", [p.model_dump(mode="json") for p in people_profiles])
    write_json(base / "strategy_and_challenges.json", strategy.model_dump(mode="json"))
    write_json(base / "required_capability_model.json", capability_model.model_dump(mode="json"))
    write_json(base / "coverage_scoring.json", coverage.model_dump(mode="json"))
    write_json(base / "improvement_hypotheses.json", [h.model_dump(mode="json") for h in hypotheses])
    write_json(base / "evidence_pack.json", evidence.model_dump(mode="json"))
    write_json(base / "degradation_profile.json", degradation_profile.model_dump(mode="json"))
    write_json(base / "final_report.json", final_report.model_dump(mode="json"))
    write_text(base / "final_report.md", md_report)
    write_json(base / "run_log.json", {"run_id": run_id, "status": "ok"})

    return final_report
