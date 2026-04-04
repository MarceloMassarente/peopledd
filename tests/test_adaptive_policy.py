from __future__ import annotations

from pathlib import Path

from peopledd.models.contracts import (
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
    StrategyChallenges,
)
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


def test_decide_n4_widen_when_strategy_empty() -> None:
    policy = DefaultAdaptivePolicy()
    ctx = RunContext(run_id="p2", output_base=Path("."))
    br = default_breaker_set()
    strat = StrategyChallenges()
    a = assess_after_n4_strategy(strat)
    act, _, rk = policy.decide_n4_widen_pages(a, ctx, br)
    assert act == "retry_n4_widen_pages"
    assert rk == "n4_widen_pages"
