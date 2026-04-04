from __future__ import annotations

from pathlib import Path

from peopledd.models.contracts import PipelineTelemetry
from peopledd.runtime.context import RunContext
from peopledd.runtime.pipeline_context import attach_run_context, detach_run_context, try_consume_llm_call


def test_pipeline_telemetry_adaptive_fields_default_empty() -> None:
    t = PipelineTelemetry(run_id="x")
    assert t.adaptive_decisions == []
    assert t.search_attempts == []


def test_try_consume_llm_call_respects_budget() -> None:
    ctx = RunContext(run_id="lm1", output_base=Path("."))
    ctx.max_llm_calls = 1
    tok = attach_run_context(ctx)
    try:
        assert try_consume_llm_call("step_a") is True
        assert try_consume_llm_call("step_b") is False
        assert len(ctx.llm_budget_skips) == 1
    finally:
        detach_run_context(tok)
