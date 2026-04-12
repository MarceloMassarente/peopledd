from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def list_runs(output_dir: Path) -> list[tuple[str, float]]:
    """Return (run_id, marker_mtime) sorted newest first."""
    base = output_dir.expanduser().resolve()
    if not base.is_dir():
        return []
    out: list[tuple[str, float]] = []
    for p in base.iterdir():
        if not p.is_dir():
            continue
        marker = p / "run_summary.json"
        if not marker.is_file():
            marker = p / "run_log.json"
        if not marker.is_file():
            continue
        try:
            m = marker.stat().st_mtime
        except OSError:
            continue
        out.append((p.name, m))
    out.sort(key=lambda x: -x[1])
    return out


def read_run_summary(output_dir: Path, run_id: str) -> str:
    path = output_dir.expanduser().resolve() / run_id / "run_summary.json"
    if not path.is_file():
        raise FileNotFoundError(f"No run_summary.json under {path.parent}")
    return path.read_text(encoding="utf-8")


def diff_runs(output_dir: Path, run_id_a: str, run_id_b: str) -> dict[str, Any]:
    """Compare two completed runs using final_report.json when present."""
    base = output_dir.expanduser().resolve()

    def load_report(rid: str) -> dict[str, Any] | None:
        p = base / rid / "final_report.json"
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def load_summary(rid: str) -> dict[str, Any] | None:
        p = base / rid / "run_summary.json"
        if not p.is_file():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    a = load_report(run_id_a)
    b = load_report(run_id_b)
    sa = load_summary(run_id_a)
    sb = load_summary(run_id_b)
    if a is None and sa is None:
        raise FileNotFoundError(f"No final_report.json or run_summary.json for run {run_id_a!r}")
    if b is None and sb is None:
        raise FileNotFoundError(f"No final_report.json or run_summary.json for run {run_id_b!r}")

    def pick_entity(rep: dict[str, Any] | None, summ: dict[str, Any] | None) -> dict[str, Any]:
        if rep and "entity_resolution" in rep:
            e = rep["entity_resolution"]
            return {
                "input_company_name": e.get("input_company_name"),
                "resolved_name": e.get("resolved_name"),
                "resolution_status": e.get("resolution_status"),
            }
        return {}

    def pick_sl(rep: dict[str, Any] | None, summ: dict[str, Any] | None) -> str | None:
        if rep and rep.get("degradation_profile"):
            return rep["degradation_profile"].get("service_level")
        if summ and summ.get("service_level"):
            return summ["service_level"]
        return None

    def pulse_claims(rep: dict[str, Any] | None) -> int:
        if not rep or "market_pulse" not in rep:
            return 0
        mp = rep["market_pulse"]
        return len(mp.get("claims") or [])

    return {
        "diff_runs_version": 1,
        "run_a": run_id_a,
        "run_b": run_id_b,
        "entity_a": pick_entity(a, sa),
        "entity_b": pick_entity(b, sb),
        "service_level_a": pick_sl(a, sa),
        "service_level_b": pick_sl(b, sb),
        "market_pulse_claims_count_a": pulse_claims(a),
        "market_pulse_claims_count_b": pulse_claims(b),
        "source_a": "final_report.json" if a else "run_summary.json",
        "source_b": "final_report.json" if b else "run_summary.json",
    }
