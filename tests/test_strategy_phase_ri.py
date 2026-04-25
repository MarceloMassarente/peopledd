"""n4 uses same effective RI as n1 (entity or governance seed)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.models.contracts import (
    CanonicalEntity,
    GovernanceSeed,
    InputPayload,
    MarketPulse,
    StrategicPriority,
    StrategyChallenges,
)
from peopledd.runtime.adaptive_models import PipelineSearchPlanState
from peopledd.runtime.phases import strategy_phase
from peopledd.runtime.pipeline_state import PipelineState


def _non_empty_strategy() -> StrategyChallenges:
    return StrategyChallenges(
        strategic_priorities=[
            StrategicPriority(priority="Test", time_horizon="short", confidence=0.5)
        ],
        key_challenges=[],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )


def test_strategy_n4_receives_seed_ri_when_entity_ri_url_empty() -> None:
    seed_url = "https://sonar.candidate/ri"
    entity = CanonicalEntity(
        entity_id="e-s4",
        input_company_name="Co",
        company_mode=CompanyMode.PRIVATE_OR_UNRESOLVED,
        ri_url=None,
        resolution_status=ResolutionStatus.PARTIAL,
    )
    state = PipelineState(
        company_name="Co",
        entity=entity,
        governance_seed=GovernanceSeed(ri_url_candidate=seed_url),
    )
    plan = PipelineSearchPlanState()
    runner = MagicMock()
    runner.search_orch = object()
    runner.adaptive_policy.build_n4_assessment.return_value = MagicMock()
    runner.adaptive_policy.decide_n4_widen_pages.return_value = ("continue", "ok", None)
    captured: dict = {}

    def n4_capture(*_a, **kw) -> StrategyChallenges:
        captured["ri_url"] = kw.get("ri_url")
        return _non_empty_strategy()

    payload = InputPayload(company_name="Co")

    with (
        patch(
            "peopledd.runtime.phases.strategy_phase.n4_strategy_inference.run",
            side_effect=n4_capture,
        ),
        patch(
            "peopledd.runtime.phases.strategy_phase.run_market_pulse",
            return_value=MarketPulse(),
        ),
    ):
        strategy_phase.run(runner, payload, state, plan)

    assert captured.get("ri_url") == seed_url
