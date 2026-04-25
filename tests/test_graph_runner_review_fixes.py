from __future__ import annotations

import pytest

from peopledd.models.contracts import InputPayload, StrategyChallenges
from peopledd.runtime.graph_runner import GraphRunner
from peopledd.runtime.pipeline_merge import merge_strategy_challenges


def test_merge_strategy_preserves_base_company_phase_when_retry_empty() -> None:
    base = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        company_phase_hypothesis={"phase": "mature", "confidence": 0.72},
    )
    retry = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        company_phase_hypothesis={},
    )
    out = merge_strategy_challenges(base, retry)
    assert out.company_phase_hypothesis.get("phase") == "mature"
    assert out.company_phase_hypothesis.get("confidence") == 0.72


def test_merge_strategy_retry_overwrites_overlapping_phase_keys() -> None:
    base = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        company_phase_hypothesis={"phase": "mature", "confidence": 0.5},
    )
    retry = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        company_phase_hypothesis={"phase": "startup", "confidence": 0.9},
    )
    out = merge_strategy_challenges(base, retry)
    assert out.company_phase_hypothesis.get("phase") == "startup"
    assert out.company_phase_hypothesis.get("confidence") == 0.9


def test_run_batch_rejects_duplicate_run_id() -> None:
    payloads = [
        InputPayload(company_name="A", run_id="same-id"),
        InputPayload(company_name="B", run_id="same-id"),
    ]
    with pytest.raises(ValueError, match="duplicate run_id"):
        GraphRunner.run_batch(payloads, "run_out", concurrency=2)
