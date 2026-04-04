from __future__ import annotations

from peopledd.runtime.circuit_breaker import SourceCircuitBreaker, default_breaker_set


def test_breaker_opens_after_threshold() -> None:
    b = SourceCircuitBreaker("test", failure_threshold=2, reset_timeout_sec=3600.0)
    assert b.allow() is True
    b.record_failure()
    assert b.state != "open"
    b.record_failure()
    assert b.state == "open"
    assert b.allow() is False


def test_breaker_reset_on_success() -> None:
    b = SourceCircuitBreaker("test2", failure_threshold=3, reset_timeout_sec=3600.0)
    b.record_failure()
    b.record_success()
    assert b.state == "closed"
    assert b.allow() is True


def test_default_breaker_set_keys() -> None:
    s = default_breaker_set()
    assert set(s.keys()) == {"cvm", "ri", "harvest", "fre", "strategy_llm"}
    for b in s.values():
        assert b.allow() is True
