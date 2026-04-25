from __future__ import annotations

import json
from pathlib import Path

from peopledd.tools.runs_health import build_runs_health_report


def test_runs_health_aggregates_ok_run(tmp_path: Path) -> None:
    run_id = "r1"
    run_dir = tmp_path / run_id
    run_dir.mkdir()
    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "ok",
                "telemetry": {"llm_calls_used": 3},
                "per_phase_durations_ms": {"governance": 100.0},
                "checkpoint": {"used": False, "written": True, "phase": "post_people"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    rep = build_runs_health_report(tmp_path)
    assert rep["runs_with_run_summary"] == 1
    assert rep["status_distribution"]["ok"] == 1
    assert rep["llm_calls_used_sum"] == 3
