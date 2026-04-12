from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from peopledd.runtime.run_inspect import diff_runs, list_runs, read_run_summary


def test_list_runs_orders_by_mtime(tmp_path: Path) -> None:
    old = tmp_path / "old-run"
    new = tmp_path / "new-run"
    old.mkdir()
    new.mkdir()
    (old / "run_summary.json").write_text("{}", encoding="utf-8")
    time.sleep(0.05)
    (new / "run_log.json").write_text('{"status":"error"}', encoding="utf-8")
    rows = list_runs(tmp_path)
    assert [r[0] for r in rows] == ["new-run", "old-run"]


def test_read_run_summary_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_run_summary(tmp_path, "nope")


def test_diff_runs_uses_final_report(tmp_path: Path) -> None:
    a = tmp_path / "a"
    b = tmp_path / "b"
    a.mkdir()
    b.mkdir()
    fr_a = {
        "entity_resolution": {
            "input_company_name": "Co",
            "resolved_name": "Co SA",
            "resolution_status": "resolved",
        },
        "degradation_profile": {"service_level": "SL2"},
        "market_pulse": {"claims": [{"statement": "x"}]},
    }
    fr_b = {
        "entity_resolution": {
            "input_company_name": "Co",
            "resolved_name": "Co SA",
            "resolution_status": "resolved",
        },
        "degradation_profile": {"service_level": "SL3"},
        "market_pulse": {"claims": []},
    }
    (a / "final_report.json").write_text(json.dumps(fr_a), encoding="utf-8")
    (b / "final_report.json").write_text(json.dumps(fr_b), encoding="utf-8")
    d = diff_runs(tmp_path, "a", "b")
    assert d["service_level_a"] == "SL2"
    assert d["service_level_b"] == "SL3"
    assert d["market_pulse_claims_count_a"] == 1
    assert d["market_pulse_claims_count_b"] == 0
