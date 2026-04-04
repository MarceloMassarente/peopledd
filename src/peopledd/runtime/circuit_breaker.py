from __future__ import annotations

from time import monotonic
from typing import Literal

State = Literal["closed", "open", "half_open"]


class SourceCircuitBreaker:
    """
    Per-source circuit breaker: opens after consecutive failures, half-open probe after reset_timeout_sec.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        reset_timeout_sec: float = 60.0,
    ):
        self.name = name
        self.failure_threshold = max(1, failure_threshold)
        self.reset_timeout_sec = reset_timeout_sec
        self._failures = 0
        self._last_failure_mono: float | None = None
        self._state: State = "closed"

    @property
    def state(self) -> State:
        if self._state == "open" and self._last_failure_mono is not None:
            if monotonic() - self._last_failure_mono >= self.reset_timeout_sec:
                self._state = "half_open"
        return self._state

    def allow(self) -> bool:
        return self.state != "open"

    def record_success(self) -> None:
        self._failures = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failures += 1
        self._last_failure_mono = monotonic()
        if self._failures >= self.failure_threshold:
            self._state = "open"

    def snapshot(self) -> dict[str, str | int]:
        return {
            "name": self.name,
            "state": self.state,
            "failures": self._failures,
        }


def default_breaker_set() -> dict[str, SourceCircuitBreaker]:
    return {
        "cvm": SourceCircuitBreaker("cvm", failure_threshold=5, reset_timeout_sec=120.0),
        "ri": SourceCircuitBreaker("ri", failure_threshold=4, reset_timeout_sec=90.0),
        "harvest": SourceCircuitBreaker("harvest", failure_threshold=6, reset_timeout_sec=60.0),
        "fre": SourceCircuitBreaker("fre", failure_threshold=3, reset_timeout_sec=180.0),
        "strategy_llm": SourceCircuitBreaker("strategy_llm", failure_threshold=4, reset_timeout_sec=120.0),
    }
