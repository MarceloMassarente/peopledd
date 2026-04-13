from __future__ import annotations

import json
from pathlib import Path

from peopledd.tools.calibrate import build_calibration_report, main


def _write_run(
    base: Path,
    run_id: str,
    *,
    sl: str,
    gap_kind: str,
    action: str,
) -> None:
    d = base / run_id
    d.mkdir(parents=True)
    payload = {
        "degradation_profile": {"service_level": sl},
        "pipeline_telemetry": {
            "adaptive_decisions": [
                {
                    "checkpoint": "n1_post_ingestion",
                    "gap_kind": gap_kind,
                    "action": action,
                }
            ]
        },
    }
    (d / "final_report.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_calibration_report_hit_rate(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    root.mkdir()
    for i in range(6):
        _write_run(
            root,
            f"r{i}",
            sl="SL3",
            gap_kind="ri_low_content",
            action="retry_n1_fre_extended",
        )
    for i in range(4):
        _write_run(
            root,
            f"f{i}",
            sl="SL1",
            gap_kind="ri_low_content",
            action="retry_n1_fre_extended",
        )
    rep = build_calibration_report(root)
    rows = { (r["gap_kind"], r["action"]): r for r in rep["combinations"]}
    row = rows.get(("ri_low_content", "retry_n1_fre_extended"))
    assert row is not None
    assert row["run_hits"] == 10
    assert row["hit_rate"] == 0.6


def test_calibration_empty_dir(tmp_path: Path) -> None:
    root = tmp_path / "empty"
    root.mkdir()
    rep = build_calibration_report(root)
    assert rep["combinations"] == []


def test_main_missing_runs_dir(tmp_path: Path) -> None:
    code = main(["--runs-dir", str(tmp_path / "nope")])
    assert code == 2


def test_main_writes_reports(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    root.mkdir()
    _write_run(root, "a1", sl="SL2", gap_kind="g", action="continue")
    out = tmp_path / "out"
    out.mkdir()
    code = main(["--runs-dir", str(root), "--output-dir", str(out)])
    assert code == 0
    assert (out / "calibration_report.json").is_file()
    assert (out / "calibration_report.md").is_file()
