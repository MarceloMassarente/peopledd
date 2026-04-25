from __future__ import annotations

from pathlib import Path
from typing import Any

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.runtime.adaptive_policy import DefaultAdaptivePolicy
from peopledd.runtime.batch_runner import run_pipeline_batch
from peopledd.runtime.circuit_breaker import (
    WeightedCircuitBreaker,
    default_breaker_set,
    failure_weight_for_mode,
)
from peopledd.runtime.context import RunContext
from peopledd.runtime.phases import governance_phase, people_phase, scoring_phase, strategy_phase
from peopledd.runtime.pipeline_context import attach_run_context, detach_run_context
from peopledd.runtime.pipeline_linear import execute_linear_pipeline
from peopledd.runtime.pipeline_state import PipelineState
from peopledd.runtime.run_limits import resolve_run_limits
from peopledd.runtime.source_attempt import SourceAttemptResult
from peopledd.runtime.source_memory import SourceMemoryStore
from peopledd.runtime.run_metadata import build_error_run_summary
from peopledd.pipeline_helpers import build_search_orchestrator
from peopledd.services.connectors import CVMConnector, RIConnector
from peopledd.services.harvest_adapter import HarvestAdapter
from peopledd.utils.io import ensure_dir, validate_output_base_dir, write_json


class GraphRunner:
    """
    Linear pipeline executor (n0-n9) with rubric-driven adaptive recovery.
    Macro-phases live under peopledd.runtime.phases.
    """

    def __init__(
        self,
        ctx: RunContext,
        cvm: CVMConnector,
        ri: RIConnector,
        harvest: HarvestAdapter,
        search_orch: Any,
        breakers: dict[str, WeightedCircuitBreaker] | None = None,
        adaptive_policy: DefaultAdaptivePolicy | None = None,
    ):
        self.ctx = ctx
        self.cvm = cvm
        self.ri = ri
        self.harvest = harvest
        self.search_orch = search_orch
        self.breakers = breakers or default_breaker_set()
        self.adaptive_policy = adaptive_policy or DefaultAdaptivePolicy()

    def _breaker_success(self, key: str) -> None:
        b = self.breakers[key]
        b.record_success()
        snap = b.snapshot()
        self.ctx.log(
            "circuit",
            key,
            "record_success",
            state=str(snap["state"]),
            failures=int(snap["failures"]),
            health_score=float(snap["health_score"]),
        )

    def _breaker_failure(self, key: str, weight: float = 1.0) -> None:
        b = self.breakers[key]
        b.record_failure(weight=weight)
        snap = b.snapshot()
        self.ctx.log(
            "circuit",
            key,
            "record_failure",
            state=str(snap["state"]),
            failures=int(snap["failures"]),
            health_score=float(snap["health_score"]),
            weight=weight,
        )

    def _log_ri_scrape_attempt(self, attempt: SourceAttemptResult) -> None:
        self.ctx.log(
            "gap",
            "n1",
            "ri_scrape_attempt",
            success=attempt.success,
            failure_mode=attempt.failure_mode,
            source_url=attempt.source_url,
            strategy_used=attempt.strategy_used,
            content_words=attempt.content_words,
            governance_found=attempt.governance_found,
        )
        if attempt.success:
            self._breaker_success("ri")
        else:
            self._breaker_failure("ri", weight=failure_weight_for_mode(attempt.failure_mode))

    def _write_emergency_trace(self, input_payload: InputPayload, exc: Exception) -> None:
        ctx = self.ctx
        base = ctx.output_base
        try:
            ensure_dir(base)
            write_json(base / "run_trace.json", ctx.trace_to_json())
            write_json(
                base / "run_log.json",
                {
                    "run_id": ctx.run_id,
                    "status": "error",
                    "recovery_counts": dict(ctx.recovery_counts),
                    "llm_calls_used": ctx.llm_calls_used,
                    "output_mode": input_payload.output_mode,
                },
            )
            write_json(
                base / "run_summary.json",
                build_error_run_summary(
                    ctx.run_id,
                    base,
                    output_mode=input_payload.output_mode,
                    llm_calls_used=ctx.llm_calls_used,
                    recovery_counts=dict(ctx.recovery_counts),
                    exc=exc,
                    trace_events=ctx.trace_to_json(),
                    checkpoint_meta=dict(getattr(ctx, "checkpoint_meta", {}) or {}),
                    per_phase_durations_ms=dict(getattr(ctx, "per_phase_durations_ms", {}) or {}),
                ),
            )
        except OSError:
            pass

    def _write_error_run_summary_artifact_write(
        self, base: Path, mode: str, exc: Exception
    ) -> None:
        ctx = self.ctx
        try:
            ensure_dir(base)
            write_json(
                base / "run_summary.json",
                build_error_run_summary(
                    ctx.run_id,
                    base,
                    output_mode=mode,
                    llm_calls_used=ctx.llm_calls_used,
                    recovery_counts=dict(ctx.recovery_counts),
                    exc=exc,
                    trace_events=ctx.trace_to_json(),
                    checkpoint_meta=dict(getattr(ctx, "checkpoint_meta", {}) or {}),
                    per_phase_durations_ms=dict(getattr(ctx, "per_phase_durations_ms", {}) or {}),
                ),
            )
        except OSError:
            pass

    def run(self, input_payload: InputPayload) -> FinalReport:
        ctx = self.ctx
        base = ctx.output_base
        ensure_dir(base)
        cache_dir = base / "cache"
        ensure_dir(cache_dir)

        token = attach_run_context(ctx)
        try:
            try:
                return self._run_pipeline(input_payload, base)
            except Exception as e:
                self._write_emergency_trace(input_payload, e)
                raise
        finally:
            detach_run_context(token)

    def _run_governance_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: Any,
    ) -> None:
        governance_phase.run(self, input_payload, state, search_plan)

    def _run_people_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: Any,
    ) -> None:
        people_phase.run(self, input_payload, state, search_plan)

    def _run_strategy_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: Any,
    ) -> None:
        strategy_phase.run(self, input_payload, state, search_plan)

    def _run_scoring_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        base: Path,
    ) -> FinalReport:
        return scoring_phase.run(self, input_payload, state, base)

    def _run_pipeline(self, input_payload: InputPayload, base: Path) -> FinalReport:
        return execute_linear_pipeline(self, input_payload, base)

    @staticmethod
    def run_batch(
        payloads: list[InputPayload],
        output_dir: str,
        *,
        concurrency: int = 3,
        resume_on_failure: bool = True,
    ) -> list[FinalReport | Exception]:
        return run_pipeline_batch(
            payloads,
            output_dir,
            concurrency=concurrency,
            resume_on_failure=resume_on_failure,
        )


def run_pipeline_graph(input_payload: InputPayload, output_dir: str = "run") -> FinalReport:
    """Entry used by orchestrator: build context, deps, GraphRunner."""
    validate_output_base_dir(output_dir)
    max_llm, max_rec = resolve_run_limits(input_payload)
    ctx = RunContext.create(
        output_dir,
        run_id=input_payload.run_id,
        max_llm_calls=max_llm,
        max_recovery_steps=max_rec,
    )
    ctx.source_memory = SourceMemoryStore(Path(output_dir) / "_source_memory")
    cache_dir = ctx.output_base / "cache"
    ensure_dir(cache_dir)

    cvm = CVMConnector()
    ri = RIConnector()
    harvest = HarvestAdapter(pipeline_cache_db_path=str(cache_dir / "pipeline.sqlite"))
    search_orch = build_search_orchestrator()

    runner = GraphRunner(ctx, cvm, ri, harvest, search_orch)
    return runner.run(input_payload)
