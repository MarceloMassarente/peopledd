from __future__ import annotations

from pathlib import Path

from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, SearchAttemptRecord
from peopledd.runtime.context import RunContext


def test_recovery_allowed_per_key_and_global_cap() -> None:
    ctx = RunContext(run_id="t1", output_base=Path("/tmp/x"))
    ctx.max_recovery_steps = 3
    assert ctx.recovery_allowed("a") is True
    ctx.bump_recovery("a")
    ctx.bump_recovery("a")
    assert ctx.recovery_counts["a"] == 2
    assert ctx.recovery_allowed("a") is False
    assert ctx.recovery_allowed("b") is True
    ctx.bump_recovery("b")
    assert sum(ctx.recovery_counts.values()) >= ctx.max_recovery_steps
    assert ctx.recovery_allowed("c") is False


def test_record_adaptive_decision_increments_sequence() -> None:
    ctx = RunContext(run_id="t2", output_base=Path("/tmp/y"))
    ctx.record_adaptive_decision(
        AdaptiveDecisionRecord(
            sequence=0,
            checkpoint="n1_post_ingestion",
            action="continue",
            rationale="ok",
        )
    )
    ctx.record_adaptive_decision(
        AdaptiveDecisionRecord(
            sequence=0,
            checkpoint="n4_post_strategy",
            action="continue",
            rationale="ok",
        )
    )
    assert len(ctx.adaptive_decisions) == 2
    assert ctx.adaptive_decisions[0]["sequence"] == 1
    assert ctx.adaptive_decisions[1]["sequence"] == 2


def test_record_search_attempt_appends() -> None:
    ctx = RunContext(run_id="t3", output_base=Path("/tmp/z"))
    ctx.record_search_attempt(
        SearchAttemptRecord(
            purpose="person_exa_people",
            attempt_index=0,
            url_count=2,
            empty_pool=False,
        )
    )
    assert len(ctx.search_attempts) == 1
    assert ctx.search_attempts[0]["purpose"] == "person_exa_people"
