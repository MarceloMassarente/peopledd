from __future__ import annotations

from unittest.mock import patch

import pytest

from peopledd.runtime.circuit_breaker import (
    WeightedCircuitBreaker,
    default_breaker_set,
    failure_weight_for_mode,
)


def test_breaker_opens_after_weighted_threshold() -> None:
    b = WeightedCircuitBreaker("test", failure_threshold=2, reset_timeout_sec=3600.0)
    assert b.allow() is True
    b.record_failure(1.0)
    assert b.state != "open"
    b.record_failure(1.0)
    assert b.state == "open"
    assert b.allow() is False


def test_breaker_reset_on_success() -> None:
    b = WeightedCircuitBreaker("test2", failure_threshold=3, reset_timeout_sec=3600.0)
    b.record_failure(1.0)
    b.record_success()
    assert b.state == "closed"
    assert b.allow() is True


def test_light_failures_do_not_open_until_load_reaches_threshold() -> None:
    b = WeightedCircuitBreaker("w", failure_threshold=4, reset_timeout_sec=3600.0)
    for _ in range(13):
        b.record_failure(0.3)
    assert b.allow()
    b.record_failure(0.3)
    assert not b.allow()


def test_anti_bot_weights_open_ri_default_breaker_fast() -> None:
    b = WeightedCircuitBreaker("ri", failure_threshold=4, reset_timeout_sec=3600.0)
    w = failure_weight_for_mode("anti_bot")
    assert w == 2.0
    b.record_failure(w)
    b.record_failure(w)
    assert not b.allow()


def test_window_prunes_old_events() -> None:
    b = WeightedCircuitBreaker("win", failure_threshold=4, window_sec=10.0, threshold_weight=4.0)
    with patch("peopledd.runtime.circuit_breaker.monotonic", side_effect=[0.0, 0.0, 0.0, 20.0]):
        b.record_failure(4.0)
        assert not b.allow()
        assert b.allow()


def test_threshold_weight_zero_raises() -> None:
    with pytest.raises(ValueError, match="threshold_weight"):
        WeightedCircuitBreaker("bad", threshold_weight=0.0, window_sec=60.0)


def test_snapshot_includes_health_score() -> None:
    b = WeightedCircuitBreaker("snap", failure_threshold=4, reset_timeout_sec=3600.0)
    s = b.snapshot()
    assert s["health_score"] == 1.0
    b.record_failure(2.0)
    s2 = b.snapshot()
    assert 0.0 <= float(s2["health_score"]) <= 1.0
    assert "weighted_load" in s2


def test_default_breaker_set_keys() -> None:
    s = default_breaker_set()
    assert set(s.keys()) == {"cvm", "ri", "harvest", "fre", "strategy_llm"}
    for b in s.values():
        assert b.allow() is True
