from __future__ import annotations

from time import monotonic
from typing import Literal

from peopledd.runtime.source_attempt import SourceFailureMode

State = Literal["closed", "open"]


def failure_weight_for_mode(mode: SourceFailureMode | None) -> float:
    """Plan weights: timeout 1.5, anti_bot 2.0, network 1.0, low_content 0.3, parse 0.5, else 1.0."""
    if mode is None:
        return 1.0
    match mode:
        case "timeout":
            return 1.5
        case "anti_bot":
            return 2.0
        case "network_error":
            return 1.0
        case "low_content":
            return 0.3
        case "parse_error":
            return 0.5
        case "pdf_only" | "llm_extract_failed" | "schema_mismatch" | "not_found" | "budget_exhausted":
            return 1.0


class WeightedCircuitBreaker:
    """
    Sliding-window weighted failures: health_score = 1 - (sum weights in window) / threshold_weight.
    Opens when health_score <= 0. record_success clears the window.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout_sec: float = 60.0,
        *,
        window_sec: float | None = None,
        threshold_weight: float | None = None,
    ) -> None:
        self.name = name
        tw = float(threshold_weight) if threshold_weight is not None else float(max(1, failure_threshold))
        if tw <= 0:
            raise ValueError("threshold_weight must be positive")
        self.threshold_weight = tw
        self.window_sec = float(window_sec if window_sec is not None else reset_timeout_sec)
        self._events: list[tuple[float, float]] = []

    def _prune(self) -> None:
        now = monotonic()
        cutoff = now - self.window_sec
        self._events = [(t, w) for t, w in self._events if t >= cutoff]

    @property
    def weighted_load(self) -> float:
        self._prune()
        return sum(w for _, w in self._events)

    @property
    def health_score(self) -> float:
        load = self.weighted_load
        h = 1.0 - load / self.threshold_weight
        return max(0.0, min(1.0, h))

    @property
    def state(self) -> State:
        return "open" if self.health_score <= 0.0 else "closed"

    def allow(self) -> bool:
        return self.health_score > 0.0

    def record_success(self) -> None:
        self._events.clear()

    def record_failure(self, weight: float = 1.0) -> None:
        self._prune()
        w = max(0.0, float(weight))
        self._events.append((monotonic(), w))

    def snapshot(self) -> dict[str, str | int | float]:
        self._prune()
        return {
            "name": self.name,
            "state": self.state,
            "failures": len(self._events),
            "health_score": round(self.health_score, 4),
            "weighted_load": round(self.weighted_load, 4),
        }


SourceCircuitBreaker = WeightedCircuitBreaker


def default_breaker_set() -> dict[str, WeightedCircuitBreaker]:
    return {
        "cvm": WeightedCircuitBreaker("cvm", failure_threshold=5, reset_timeout_sec=120.0),
        "ri": WeightedCircuitBreaker("ri", failure_threshold=4, reset_timeout_sec=90.0),
        "harvest": WeightedCircuitBreaker("harvest", failure_threshold=6, reset_timeout_sec=60.0),
        "fre": WeightedCircuitBreaker("fre", failure_threshold=3, reset_timeout_sec=180.0),
        "strategy_llm": WeightedCircuitBreaker("strategy_llm", failure_threshold=4, reset_timeout_sec=120.0),
    }
