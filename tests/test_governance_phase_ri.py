"""Tests for RI URL consistency: n1 FRE retry and shared effective_ri helper."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.models.contracts import (
    CanonicalEntity,
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
    GovernanceSeed,
    InputPayload,
    SemanticGovernanceFusion,
)
from peopledd.runtime.adaptive_models import PipelineSearchPlanState
from peopledd.runtime.phases import governance_phase
from peopledd.runtime.pipeline_state import PipelineState


def _entity_no_ri() -> CanonicalEntity:
    return CanonicalEntity(
        entity_id="e-fre",
        input_company_name="Acme SA",
        resolved_name="Acme SA",
        company_mode=CompanyMode.PRIVATE_OR_UNRESOLVED,
        ri_url=None,
        resolution_status=ResolutionStatus.PARTIAL,
        resolution_confidence=0.5,
    )


def _ingestion(formal: float) -> GovernanceIngestion:
    return GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(),
        current_governance_snapshot=GovernanceSnapshot(),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=formal,
            current_completeness=0.0,
            freshness_score=0.0,
        ),
    )


def test_governance_fre_retry_passes_seed_ri_url_not_entity_ri_url() -> None:
    seed_url = "https://seed.example/ri"
    seed = GovernanceSeed(ri_url_candidate=seed_url, company_name_queried="Acme")

    n1_kwargs: list[dict] = []

    def n1_capture(*_a, **kw) -> GovernanceIngestion:
        n1_kwargs.append(dict(kw))
        if len(n1_kwargs) == 1:
            return _ingestion(0.1)
        return _ingestion(0.9)

    recon = GovernanceReconciliation()
    fusion = SemanticGovernanceFusion()

    runner = MagicMock()
    runner.search_orch = object()
    runner.ctx.llm_calls_used = 0
    runner.ctx.max_llm_calls = 24
    runner.ctx.llm_budget_skips = []
    runner.adaptive_policy.build_n1_assessment.return_value = MagicMock()
    runner.adaptive_policy.decide_n1_fre_extended.return_value = (
        "retry_n1_fre_extended",
        "probe",
        "fre_rk",
    )

    payload = InputPayload(company_name="Acme SA")
    state = PipelineState()
    plan = PipelineSearchPlanState()

    with (
        patch(
            "peopledd.runtime.phases.governance_phase.fetch_governance_seed",
            return_value=seed,
        ),
        patch(
            "peopledd.runtime.phases.governance_phase.n0_entity_resolution.run",
            return_value=_entity_no_ri(),
        ),
        patch(
            "peopledd.runtime.phases.governance_phase.n1_governance_ingestion.run",
            side_effect=n1_capture,
        ),
        patch(
            "peopledd.runtime.phases.governance_phase.n1b_reconciliation.run",
            return_value=recon,
        ),
        patch(
            "peopledd.runtime.phases.governance_phase.n1c_semantic_fusion.run",
            return_value=fusion,
        ),
    ):
        governance_phase.run(runner, payload, state, plan)

    assert len(n1_kwargs) == 2
    assert n1_kwargs[0]["ri_url"] == seed_url
    assert n1_kwargs[0]["fre_extended_probe"] is False
    assert n1_kwargs[1]["ri_url"] == seed_url, "FRE retry must keep same effective RI as first pass"
    assert n1_kwargs[1]["fre_extended_probe"] is True
