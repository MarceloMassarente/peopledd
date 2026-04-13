from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _sl_to_num(raw: str | None) -> float:
    if not raw:
        return 0.0
    s = str(raw).upper().replace(" ", "")
    if s.startswith("SL"):
        tail = s[2:]
        if tail.isdigit():
            return float(tail)
    return 0.0


def _service_level_from_final_report(data: dict[str, Any]) -> float:
    deg = data.get("degradation_profile") or {}
    return _sl_to_num(deg.get("service_level"))


def _iter_runs(runs_dir: Path) -> list[Path]:
    if not runs_dir.is_dir():
        return []
    out: list[Path] = []
    for p in runs_dir.iterdir():
        if p.is_dir():
            out.append(p)
    return sorted(out)


def _load_run_sl_and_decisions(run_dir: Path) -> tuple[float, list[dict[str, Any]]] | None:
    fr = run_dir / "final_report.json"
    if fr.is_file():
        try:
            data: dict[str, Any] = json.loads(fr.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("calibrate: skip corrupt final_report %s: %s", fr, e)
            return None
        tel = data.get("pipeline_telemetry") or {}
        decisions = list(tel.get("adaptive_decisions") or [])
        return _service_level_from_final_report(data), decisions
    summ = run_dir / "run_summary.json"
    if summ.is_file():
        try:
            data = json.loads(summ.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("calibrate: skip corrupt run_summary %s: %s", summ, e)
            return None
        return _sl_to_num(data.get("service_level")), []
    return None


def build_calibration_report(runs_dir: Path) -> dict[str, Any]:
    by_pair: dict[tuple[str, str], list[float]] = defaultdict(list)
    by_pair_degrade: dict[tuple[str, str], list[bool]] = defaultdict(list)

    for run_dir in _iter_runs(runs_dir):
        parsed = _load_run_sl_and_decisions(run_dir)
        if parsed is None:
            continue
        sl, decisions = parsed
        if not decisions:
            continue
        for d in decisions:
            gap = str(d.get("gap_kind") or d.get("checkpoint") or "unknown")
            act = str(d.get("action") or "unknown")
            key = (gap, act)
            by_pair[key].append(sl)
            by_pair_degrade[key].append(act == "degrade_and_continue")

    combinations: list[dict[str, Any]] = []
    for (gap, act), sl_list in sorted(by_pair.items()):
        n = len(sl_list)
        avg_sl = sum(sl_list) / n if n else 0.0
        degrade_flags = by_pair_degrade.get((gap, act), [])
        recovery_attempts = sum(1 for deg in degrade_flags if not deg)
        high_sl = sum(1 for s, deg in zip(sl_list, degrade_flags) if (not deg) and s >= 3.0)
        hit_rate = (high_sl / recovery_attempts) if recovery_attempts else 0.0
        combinations.append(
            {
                "gap_kind": gap,
                "action": act,
                "run_hits": n,
                "avg_sl": round(avg_sl, 4),
                "hit_rate": round(hit_rate, 4),
            }
        )

    sl_values = [s for vals in by_pair.values() for s in vals]
    sl_values.sort()
    p75 = sl_values[int(0.75 * (len(sl_values) - 1))] if sl_values else 0.0

    return {
        "runs_dir": str(runs_dir.resolve()),
        "combinations": combinations,
        "suggested_sl_floor_from_p75": round(p75, 4),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Calibration report",
        "",
        f"Runs directory: `{report.get('runs_dir', '')}`",
        f"Suggested SL floor (p75 of observed runs): **{report.get('suggested_sl_floor_from_p75')}**",
        "",
        "| gap_kind | action | samples | avg_sl | hit_rate |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in report.get("combinations", []):
        lines.append(
            f"| {row.get('gap_kind')} | {row.get('action')} | {row.get('run_hits')} | "
            f"{row.get('avg_sl')} | {row.get('hit_rate')} |"
        )
    lines.append("")
    lines.append("hit_rate counts recovery actions (non-degrade) that ended with service level SL3 or higher.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING)
    p = argparse.ArgumentParser(description="Offline calibration from run artifacts (read-only).")
    p.add_argument("--runs-dir", type=Path, required=True, help="Directory containing one subfolder per run")
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write calibration_report.json/.md (default: runs-dir)",
    )
    args = p.parse_args(argv)
    runs_dir = args.runs_dir.expanduser().resolve()
    if not runs_dir.is_dir():
        print(f"calibrate: runs directory not found: {runs_dir}", file=sys.stderr)
        return 2
    out_dir = (args.output_dir or runs_dir).expanduser().resolve()
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"calibrate: cannot create output dir {out_dir}: {e}", file=sys.stderr)
        return 2

    report = build_calibration_report(runs_dir)
    json_path = out_dir / "calibration_report.json"
    md_path = out_dir / "calibration_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
