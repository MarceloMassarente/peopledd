from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, SearchAttemptRecord
from peopledd.runtime.source_memory import SourceMemoryStore

TracePhase = Literal["start", "end", "policy", "recovery", "gap", "circuit"]


@dataclass
class RunTraceEvent:
    """Single append-only trace entry for run_trace.json."""

    phase: TracePhase
    node: str
    detail: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "node": self.node,
            "detail": self.detail,
            "payload": self.payload,
        }


@dataclass
class RunContext:
    """Per-run limits, trace, and recovery counters."""

    run_id: str
    output_base: Path
    max_recovery_steps: int = 8
    max_llm_calls: int = 24
    llm_calls_used: int = 0
    llm_budget_skips: list[str] = field(default_factory=list)
    llm_routes: list[dict[str, Any]] = field(default_factory=list)
    recovery_counts: dict[str, int] = field(default_factory=dict)
    trace: list[RunTraceEvent] = field(default_factory=list)
    adaptive_decisions: list[dict[str, Any]] = field(default_factory=list)
    search_attempts: list[dict[str, Any]] = field(default_factory=list)
    source_memory: SourceMemoryStore | None = None
    _adaptive_seq: int = field(default=0, repr=False)

    @classmethod
    def create(cls, output_dir: str, run_id: str | None = None) -> RunContext:
        rid = run_id or str(uuid.uuid4())
        return cls(run_id=rid, output_base=Path(output_dir) / rid)

    def log(self, phase: TracePhase, node: str, detail: str, **payload: Any) -> None:
        self.trace.append(RunTraceEvent(phase=phase, node=node, detail=detail, payload=dict(payload)))

    def recovery_allowed(self, key: str) -> bool:
        used = sum(self.recovery_counts.values())
        if used >= self.max_recovery_steps:
            return False
        return self.recovery_counts.get(key, 0) < 2

    def bump_recovery(self, key: str) -> None:
        self.recovery_counts[key] = self.recovery_counts.get(key, 0) + 1

    def trace_to_json(self) -> list[dict[str, Any]]:
        return [e.to_json_dict() for e in self.trace]

    def next_adaptive_sequence(self) -> int:
        self._adaptive_seq += 1
        return self._adaptive_seq

    def record_adaptive_decision(self, rec: AdaptiveDecisionRecord) -> None:
        seq = self.next_adaptive_sequence()
        payload = rec.model_copy(update={"sequence": seq}).model_dump(mode="json")
        self.adaptive_decisions.append(payload)
        self.log(
            "policy",
            "adaptive",
            "decision",
            checkpoint=payload.get("checkpoint"),
            action=payload.get("action"),
            rationale=payload.get("rationale"),
            recovery_key=payload.get("recovery_key"),
            sequence=seq,
        )

    def record_search_attempt(self, rec: SearchAttemptRecord) -> None:
        payload = rec.model_dump(mode="json")
        self.search_attempts.append(payload)
        self.log(
            "policy",
            "search",
            "attempt",
            purpose=payload.get("purpose"),
            attempt_index=payload.get("attempt_index"),
            escalation_level=payload.get("escalation_level"),
            url_count=payload.get("url_count"),
            empty_pool=payload.get("empty_pool"),
        )
