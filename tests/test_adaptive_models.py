from __future__ import annotations

import pytest
from pydantic import ValidationError

from peopledd.models.contracts import FinalReport, PipelineTelemetry
from peopledd.runtime.adaptive_models import (
    AdaptiveDecisionRecord,
    AssessmentGap,
    FindUrlsParams,
    PhaseAssessment,
    PipelineSearchPlanState,
    SearchAttemptRecord,
)


def test_adaptive_decision_round_trip_telemetry() -> None:
    rec = AdaptiveDecisionRecord(
        sequence=1,
        checkpoint="n1_post_ingestion",
        action="retry_n1_fre_extended",
        rationale="formal completeness below threshold",
        rule_based=True,
        recovery_key="n1_fre_extended",
    )
    tel = PipelineTelemetry(
        run_id="r1",
        adaptive_decisions=[rec.model_dump(mode="json")],
        search_attempts=[
            SearchAttemptRecord(
                purpose="strategy_find_urls",
                attempt_index=0,
                url_count=3,
                empty_pool=False,
                topic_excerpt="estratégia",
            ).model_dump(mode="json")
        ],
    )
    raw = tel.model_dump(mode="json")
    tel2 = PipelineTelemetry.model_validate(raw)
    assert len(tel2.adaptive_decisions) == 1
    assert tel2.adaptive_decisions[0]["action"] == "retry_n1_fre_extended"


def test_final_report_without_adaptive_fields() -> None:
    """Backward compatibility: older JSON shapes omit adaptive lists."""
    from peopledd.models.common import CompanyMode, ResolutionStatus, ServiceLevel
    from peopledd.models.contracts import (
        CanonicalEntity,
        ConfidencePolicy,
        CoverageScoring,
        DegradationProfile,
        EvidencePack,
        GovernanceIngestion,
        GovernanceReconciliation,
        InputPayload,
        PersonProfile,
        RequiredCapabilityModel,
        StrategyChallenges,
    )

    entity = CanonicalEntity(
        entity_id="e1",
        input_company_name="X",
        company_mode=CompanyMode.LISTED_BR,
        resolution_status=ResolutionStatus.RESOLVED,
    )
    payload = InputPayload(company_name="X")
    tel = PipelineTelemetry(run_id="r")
    fr = FinalReport(
        input_payload=payload,
        entity_resolution=entity,
        governance=GovernanceIngestion(),
        governance_reconciliation=GovernanceReconciliation(),
        people_resolution=[],
        people_profiles=[],
        strategy_and_challenges=StrategyChallenges(),
        required_capability_model=RequiredCapabilityModel(),
        coverage_scoring=CoverageScoring(),
        improvement_hypotheses=[],
        evidence_pack=EvidencePack(),
        degradation_profile=DegradationProfile(service_level=ServiceLevel.SL1),
        confidence_policy=ConfidencePolicy(),
        pipeline_telemetry=tel,
    )
    dumped = fr.model_dump(mode="json")
    assert "pipeline_telemetry" in dumped


def test_invalid_search_attempt_purpose_rejected() -> None:
    with pytest.raises(ValidationError):
        SearchAttemptRecord(
            purpose="invalid",  # type: ignore[arg-type]
            attempt_index=0,
        )


def test_phase_assessment_model() -> None:
    pa = PhaseAssessment(
        checkpoint="n4_post_strategy",
        metrics={"priorities": 0},
        gaps=[AssessmentGap(kind="strategy_empty", detail="no items")],
    )
    assert pa.gaps[0].kind == "strategy_empty"


def test_pipeline_search_plan_state_escalation() -> None:
    st = PipelineSearchPlanState()
    assert st.person_params.query_style == "default"
    st.escalate_person_secondary()
    assert st.person_params.query_style == "company_first"
    st.escalate_strategy_find_urls()
    assert st.find_urls_params.max_searx_queries == 3


def test_find_urls_params_escalated_differs_from_default() -> None:
    d = FindUrlsParams.default()
    e = FindUrlsParams.escalated()
    assert d.max_searx_queries < e.max_searx_queries
    assert e.exa_num_results > d.exa_num_results
