from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _iter_run_dirs(runs_dir: Path) -> list[Path]:
    if not runs_dir.is_dir():
        return []
    return sorted(p for p in runs_dir.iterdir() if p.is_dir())


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("runs_health: skip corrupt %s: %s", path, e)
        return None


def _trace_checkpoint_flags(trace: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for ev in trace:
        if ev.get("node") != "pipeline":
            continue
        detail = str(ev.get("detail") or "")
        if detail == "resume_from_checkpoint":
            counts["resume_from_checkpoint"] += 1
        elif detail == "checkpoint_fingerprint_mismatch":
            counts["checkpoint_fingerprint_mismatch"] += 1
        elif detail == "checkpoint_missing_fingerprint":
            counts["checkpoint_missing_fingerprint"] += 1
    return dict(counts)


def _per_phase_from_trace(trace: list[dict[str, Any]]) -> dict[str, float]:
    """Sum duration_ms from phase_end events (node=pipeline, detail=phase_end)."""
    out: dict[str, float] = defaultdict(float)
    for ev in trace:
        if ev.get("phase") != "phase_end":
            continue
        if ev.get("node") != "pipeline":
            continue
        name = str(ev.get("detail") or "")
        pl = ev.get("payload") or {}
        ms = pl.get("duration_ms") if isinstance(pl, dict) else None
        if name and isinstance(ms, (int, float)):
            out[name] += float(ms)
    return dict(out)


def _per_phase_from_summary(summary: dict[str, Any]) -> dict[str, float]:
    raw = summary.get("per_phase_durations_ms")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        if isinstance(v, (int, float)):
            out[str(k)] = float(v)
    return out


def _analyze_run(run_dir: Path) -> dict[str, Any] | None:
    summary = _load_json(run_dir / "run_summary.json")
    trace_path = run_dir / "run_trace.json"
    trace_raw = _load_json(trace_path)
    trace: list[dict[str, Any]] = trace_raw if isinstance(trace_raw, list) else []

    if summary is None:
        return None

    status = str(summary.get("status") or "unknown")
    row: dict[str, Any] = {
        "run_id": str(summary.get("run_id") or run_dir.name),
        "status": status,
        "run_directory": str(run_dir.resolve()),
    }

    tel = summary.get("telemetry") or {}
    if isinstance(tel, dict):
        row["llm_calls_used"] = int(tel.get("llm_calls_used") or 0)

    cp = summary.get("checkpoint")
    if isinstance(cp, dict):
        row["checkpoint"] = {
            "used": bool(cp.get("used")),
            "written": bool(cp.get("written")),
            "phase": cp.get("phase"),
            "reason_skipped": cp.get("reason_skipped"),
        }

    phases = _per_phase_from_summary(summary)
    if not phases and trace:
        phases = _per_phase_from_trace(trace)
    row["per_phase_durations_ms"] = phases

    row["trace_checkpoint_events"] = _trace_checkpoint_flags(trace)

    if status == "error":
        row["error_phase"] = summary.get("error_phase")
        err = summary.get("error")
        if isinstance(err, dict):
            row["error_type"] = err.get("type")
            row["error_message"] = (err.get("message") or "")[:200]

    return row


def build_runs_health_report(runs_dir: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for run_dir in _iter_run_dirs(runs_dir):
        parsed = _analyze_run(run_dir)
        if parsed is not None:
            rows.append(parsed)

    status_counts = Counter(str(r.get("status")) for r in rows)
    llm_total = sum(int(r.get("llm_calls_used") or 0) for r in rows)
    resume_total = sum((r.get("trace_checkpoint_events") or {}).get("resume_from_checkpoint", 0) for r in rows)
    mismatch_total = sum(
        (r.get("trace_checkpoint_events") or {}).get("checkpoint_fingerprint_mismatch", 0) for r in rows
    )

    phase_ms: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        for ph, ms in (r.get("per_phase_durations_ms") or {}).items():
            phase_ms[ph].append(float(ms))

    phase_summary: dict[str, dict[str, float]] = {}
    for ph, vals in phase_ms.items():
        if not vals:
            continue
        phase_summary[ph] = {
            "count": float(len(vals)),
            "sum_ms": float(sum(vals)),
            "avg_ms": float(sum(vals) / len(vals)),
            "max_ms": float(max(vals)),
        }

    error_by_phase: Counter[str] = Counter()
    for r in rows:
        if r.get("status") != "error":
            continue
        ep = str(r.get("error_phase") or "unknown")
        error_by_phase[ep] += 1

    return {
        "runs_dir": str(runs_dir.resolve()),
        "runs_with_run_summary": len(rows),
        "status_distribution": dict(status_counts),
        "errors_by_error_phase": dict(error_by_phase),
        "llm_calls_used_sum": llm_total,
        "trace_checkpoint_resume_hits": resume_total,
        "trace_checkpoint_fingerprint_mismatch_hits": mismatch_total,
        "per_phase_aggregate_ms": phase_summary,
        "runs": rows,
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Runs health report",
        "",
        f"Runs directory: `{report.get('runs_dir', '')}`",
        f"Runs with `run_summary.json`: **{report.get('runs_with_run_summary', 0)}**",
        "",
        "## Status distribution",
        "",
    ]
    for k, v in sorted((report.get("status_distribution") or {}).items()):
        lines.append(f"- `{k}`: {v}")
    lines.extend(["", "## Errors by error_phase (status=error)", ""])
    err_ph = report.get("errors_by_error_phase") or {}
    if not err_ph:
        lines.append("(none)")
    else:
        for k, v in sorted(err_ph.items(), key=lambda x: -x[1]):
            lines.append(f"- `{k}`: {v}")
    lines.extend(
        [
            "",
            "## Checkpoint (from trace)",
            "",
            f"- `resume_from_checkpoint` events: **{report.get('trace_checkpoint_resume_hits', 0)}**",
            f"- `checkpoint_fingerprint_mismatch` events: **{report.get('trace_checkpoint_fingerprint_mismatch_hits', 0)}**",
            "",
            "## LLM calls (sum over runs)",
            "",
            f"- **{report.get('llm_calls_used_sum', 0)}**",
            "",
            "## Per-phase duration (from run_summary or trace)",
            "",
        ]
    )
    agg = report.get("per_phase_aggregate_ms") or {}
    if not agg:
        lines.append("(no per-phase data)")
    else:
        lines.append("| phase | runs | sum_ms | avg_ms | max_ms |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for ph in sorted(agg.keys()):
            s = agg[ph]
            lines.append(
                f"| {ph} | {int(s.get('count', 0))} | {s.get('sum_ms', 0):.1f} | "
                f"{s.get('avg_ms', 0):.1f} | {s.get('max_ms', 0):.1f} |"
            )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING)
    p = argparse.ArgumentParser(
        description="Offline aggregate of run health from run_summary.json and run_trace.json (read-only)."
    )
    p.add_argument("--runs-dir", type=Path, required=True, help="Directory containing one subfolder per run")
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write runs_health.json/.md (default: runs-dir)",
    )
    args = p.parse_args(argv)
    runs_dir = args.runs_dir.expanduser().resolve()
    if not runs_dir.is_dir():
        print(f"runs_health: runs directory not found: {runs_dir}", file=sys.stderr)
        return 2
    out_dir = (args.output_dir or runs_dir).expanduser().resolve()
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"runs_health: cannot create output dir {out_dir}: {e}", file=sys.stderr)
        return 2

    report = build_runs_health_report(runs_dir)
    json_path = out_dir / "runs_health.json"
    md_path = out_dir / "runs_health.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
