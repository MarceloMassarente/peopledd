from __future__ import annotations

from pathlib import Path

from peopledd.models.contracts import (
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
    StrategyChallenges,
)
from peopledd.runtime.adaptive_models import PhaseAssessment
from peopledd.runtime.circuit_breaker import default_breaker_set
from peopledd.runtime.context import RunContext
from peopledd.runtime.recovery_planner import RecoveryAction, RecoveryPlanner, default_recovery_catalog
from peopledd.runtime.phase_assessment import assess_after_n4_strategy


def test_planner_picks_higher_score_when_multiple_eligible() -> None:
    def always(**_: object) -> bool:
        return True

    hi = RecoveryAction("high", "continue", None, 0.0, 10.0, always)
    lo = RecoveryAction("low", "degrade_and_continue", None, 0.0, 5.0, always)
    planner = RecoveryPlanner({"test_phase": [lo, hi]})
    act, rationale, rk = planner._pick("test_phase")
    assert act == "continue"
    assert rationale == "high"
    assert rk is None


def test_planner_empty_catalog_phase_degrades() -> None:
    planner = RecoveryPlanner({"n1_fre_extended": []})
    ingestion = GovernanceIngestion(
        governance_data_quality=GovernanceDataQuality(formal_completeness=0.3, current_completeness=0.5),
        formal_governance_snapshot=GovernanceSnapshot(),
    )
    ctx = RunContext(run_id="r", output_base=Path("."))
    a = PhaseAssessment(checkpoint="n1_post_ingestion", gaps=[])
    act, rationale, _ = planner.decide_n1_fre_extended(a, ingestion, True, ctx, default_breaker_set())
    assert act == "degrade_and_continue"
    assert "no_eligible_action" in rationale


def test_default_catalog_has_all_phases() -> None:
    cat = default_recovery_catalog()
    assert {"n1_fre_extended", "n2_person_search_escalation", "n4_widen_pages", "n4_search_escalation"} <= cat.keys()


def test_n4_widen_matches_assess_after_n4_empty_strategy() -> None:
    planner = RecoveryPlanner()
    ctx = RunContext(run_id="r", output_base=Path("."))
    br = default_breaker_set()
    a = assess_after_n4_strategy(StrategyChallenges())
    act, _, rk = planner.decide_n4_widen_pages(a, ctx, br)
    assert act == "retry_n4_widen_pages"
    assert rk == "n4_widen_pages"
