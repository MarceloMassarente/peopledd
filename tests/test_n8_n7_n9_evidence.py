from __future__ import annotations

from peopledd.models.common import CompanyMode, ResolutionStatus, ServiceLevel
from peopledd.models.contracts import (
    BoardMember,
    CanonicalEntity,
    ConfidencePolicy,
    ConflictItem,
    CoverageItem,
    CoverageScoring,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
    InputPayload,
    PersonProfile,
    PersonResolution,
    ProfileQuality,
    RequiredCapabilityModel,
    StrategyChallenges,
)
from peopledd.nodes import n7_improvement_hypotheses, n8_evidence_pack, n9_report_builder


def _rich_final_report() -> FinalReport:
    entity = CanonicalEntity(
        entity_id="e1",
        input_company_name="Acme",
        resolved_name="Acme SA",
        company_mode=CompanyMode.LISTED_BR,
        cnpj="123",
        ri_url="https://ri.acme.example/gov",
        resolution_status=ResolutionStatus.RESOLVED,
        resolution_confidence=0.9,
        analysis_scope_entity="Acme SA",
        resolution_evidence=[],
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="Alice")],
            as_of_date="2024-12-01",
        ),
        current_governance_snapshot=GovernanceSnapshot(board_members=[BoardMember(person_name="Alice")]),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.9,
            current_completeness=0.8,
            freshness_score=0.9,
        ),
        ingestion_metadata={"fre_source_url": "https://cvm.example/fre.zip", "fre_year": "2024"},
    )
    recon = GovernanceReconciliation(
        reconciliation_status="minor_conflicts",
        conflict_items=[
            ConflictItem(
                conflict_type="title_mismatch",
                person_name="Alice",
                formal_value="Chair",
                current_value="Member",
                resolution_rule_applied="prefer_formal",
                confidence=0.7,
            )
        ],
        reconciled_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="Alice")],
            executive_members=[],
        ),
    )
    pr = PersonResolution(
        person_id="p1",
        observed_name="Alice",
        resolution_status=ResolutionStatus.RESOLVED,
        resolution_confidence=0.9,
    )
    pp = PersonProfile(
        person_id="p1",
        profile_quality=ProfileQuality(useful_coverage_score=0.7, evidence_density=0.5),
        blind_spots=[],
    )
    coverage = CoverageScoring(
        board_coverage=[
            CoverageItem(
                dimension="governanca_risco_compliance",
                required_level=4,
                observed_level=1.0,
                confidence_adjusted_level=1.0,
                coverage_ratio=0.25,
                gap_severity="high",
                single_point_of_failure=True,
                rationale="test",
            )
        ],
        executive_coverage=[],
    )
    deg = DegradationProfile(
        service_level=ServiceLevel.SL3,
        degradations=["low_useful_coverage_board"],
        mandatory_disclaimers=["Inferência limitada"],
    )
    conf = ConfidencePolicy(
        data_completeness_score=0.85,
        evidence_quality_score=0.7,
        analytical_confidence_score=0.75,
    )
    return FinalReport(
        input_payload=InputPayload(company_name="Acme"),
        entity_resolution=entity,
        governance=ingestion,
        governance_reconciliation=recon,
        people_resolution=[pr],
        people_profiles=[pp],
        strategy_and_challenges=StrategyChallenges(),
        required_capability_model=RequiredCapabilityModel(),
        coverage_scoring=coverage,
        improvement_hypotheses=[],
        evidence_pack=EvidencePack(),
        degradation_profile=deg,
        confidence_policy=conf,
    )


def test_n8_builds_documents_and_many_claims():
    report = _rich_final_report()
    pack = n8_evidence_pack.run(partial_report=report, run_id="run-test-1")
    assert len(pack.documents) >= 4
    assert len(pack.claims) >= 6
    ids = {c.claim_id for c in pack.claims}
    assert "C_ENTITY_SCOPE" in ids
    assert "C_PIPELINE_RUN" in ids
    assert any(x.startswith("C_CONFLICT_") for x in ids)
    assert any(x.startswith("C_GAP_BOARD_") for x in ids)


def test_n7_uses_evidence_claim_refs():
    report = _rich_final_report()
    pack = n8_evidence_pack.run(partial_report=report, run_id="run-test-2")
    hypos = n7_improvement_hypotheses.run(
        report.coverage_scoring,
        report.strategy_and_challenges,
        report.confidence_policy.analytical_confidence_score,
        evidence_pack=pack,
        governance_reconciliation=report.governance_reconciliation,
        people_resolution=report.people_resolution,
        people_profiles=report.people_profiles,
        degradation_profile=report.degradation_profile,
    )
    assert hypos
    assert any(h.evidence_claim_refs for h in hypos)


def test_n9_contains_new_sections():
    report = _rich_final_report()
    pack = n8_evidence_pack.run(partial_report=report, run_id="run-test-3")
    hypos = n7_improvement_hypotheses.run(
        report.coverage_scoring,
        report.strategy_and_challenges,
        0.75,
        evidence_pack=pack,
        governance_reconciliation=report.governance_reconciliation,
        people_resolution=report.people_resolution,
        people_profiles=report.people_profiles,
        degradation_profile=report.degradation_profile,
    )
    final = report.model_copy(update={"evidence_pack": pack, "improvement_hypotheses": hypos})
    md = n9_report_builder.to_markdown(final)
    assert "## 1. Sumário executivo" in md
    assert "## 6. Pessoas" in md
    assert "## 9. Pacote de evidências" in md
    assert "## 10. Confiança" in md
