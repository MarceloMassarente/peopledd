from __future__ import annotations

from pathlib import Path

from peopledd.models.contracts import (
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
    PersonProfile,
    PersonResolution,
    ProfileQuality,
    StrategyChallenges,
)
from peopledd.models.common import ResolutionStatus
from peopledd.runtime.adaptive_policy import DefaultAdaptivePolicy
from peopledd.runtime.circuit_breaker import default_breaker_set
from peopledd.runtime.context import RunContext
from peopledd.runtime.phase_assessment import assess_after_n4_strategy


def test_decide_n1_fre_extended_when_formal_weak_and_cnpj() -> None:
    policy = DefaultAdaptivePolicy()
    ingestion = GovernanceIngestion(
        governance_data_quality=GovernanceDataQuality(formal_completeness=0.3, current_completeness=0.5),
        formal_governance_snapshot=GovernanceSnapshot(),
    )
    ctx = RunContext(run_id="p1", output_base=Path("."))
    br = default_breaker_set()
    a = policy.build_n1_assessment(ingestion, has_cnpj=True, search_orchestrator_configured=False)
    act, _, rk = policy.decide_n1_fre_extended(a, ingestion, True, ctx, br)
    assert act == "retry_n1_fre_extended"
    assert rk == "n1_fre_extended"


def test_n1_assessment_uses_causal_ri_gap_when_metadata_present() -> None:
    policy = DefaultAdaptivePolicy()
    ingestion = GovernanceIngestion(
        governance_data_quality=GovernanceDataQuality(formal_completeness=0.5, current_completeness=0.3),
        formal_governance_snapshot=GovernanceSnapshot(),
        ingestion_metadata={"ri_primary_failure_mode": "anti_bot"},
    )
    a = policy.build_n1_assessment(ingestion, has_cnpj=True, search_orchestrator_configured=True)
    kinds = {g.kind for g in a.gaps}
    assert "ri_anti_bot" in kinds
    assert "current_governance_weak" not in kinds


def test_n1_assessment_current_weak_without_ri_mode_stays_generic() -> None:
    policy = DefaultAdaptivePolicy()
    ingestion = GovernanceIngestion(
        governance_data_quality=GovernanceDataQuality(formal_completeness=0.5, current_completeness=0.3),
        formal_governance_snapshot=GovernanceSnapshot(),
    )
    a = policy.build_n1_assessment(ingestion, has_cnpj=True, search_orchestrator_configured=True)
    assert any(g.kind == "current_governance_weak" for g in a.gaps)


def test_decide_n1_fre_extended_unchanged_with_ri_anti_bot_gap() -> None:
    policy = DefaultAdaptivePolicy()
    ingestion = GovernanceIngestion(
        governance_data_quality=GovernanceDataQuality(formal_completeness=0.3, current_completeness=0.3),
        formal_governance_snapshot=GovernanceSnapshot(),
        ingestion_metadata={"ri_primary_failure_mode": "anti_bot"},
    )
    ctx = RunContext(run_id="p1", output_base=Path("."))
    br = default_breaker_set()
    a = policy.build_n1_assessment(ingestion, has_cnpj=True, search_orchestrator_configured=False)
    act, _, rk = policy.decide_n1_fre_extended(a, ingestion, True, ctx, br)
    assert act == "retry_n1_fre_extended"
    assert rk == "n1_fre_extended"


def test_decide_n2_escalation_skips_when_llm_budget_exhausted() -> None:
    policy = DefaultAdaptivePolicy()
    ctx = RunContext(run_id="p1", output_base=Path("."), llm_calls_used=24, max_llm_calls=24)
    br = default_breaker_set()
    pr = PersonProfile(
        person_id="p1",
        profile_quality=ProfileQuality(evidence_density=0.01, useful_coverage_score=0.1),
    )
    people_res = [
        PersonResolution(
            person_id="p1",
            observed_name="A",
            resolution_status=ResolutionStatus.RESOLVED,
        )
    ]
    a = policy.build_n2n3_assessment([pr], people_res, board_names={"A", "B", "C"})
    act, rationale, rk = policy.decide_n2_person_search_escalation(
        a, ctx, br, search_orchestrator_configured=True, person_escalation_already_applied=False
    )
    assert act == "degrade_and_continue"
    assert "llm_call_budget" in rationale
    assert rk is None


def test_decide_n4_widen_when_strategy_empty() -> None:
    policy = DefaultAdaptivePolicy()
    ctx = RunContext(run_id="p2", output_base=Path("."))
    br = default_breaker_set()
    strat = StrategyChallenges()
    a = assess_after_n4_strategy(strat)
    act, _, rk = policy.decide_n4_widen_pages(a, ctx, br)
    assert act == "retry_n4_widen_pages"
    assert rk == "n4_widen_pages"
