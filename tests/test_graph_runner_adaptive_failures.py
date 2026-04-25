from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from peopledd.models.contracts import InputPayload
from peopledd.runtime.context import RunContext
from peopledd.runtime.graph_runner import GraphRunner


def test_emergency_trace_written_when_pipeline_raises(tmp_path: Path) -> None:
    ctx = RunContext.create(str(tmp_path))
    harvest = MagicMock()
    runner = GraphRunner(ctx, MagicMock(), MagicMock(), harvest, None)

    with patch("peopledd.nodes.n0_entity_resolution.run", side_effect=RuntimeError("n0 boom")):
        with pytest.raises(RuntimeError, match="n0 boom"):
            runner.run(InputPayload(company_name="X"))

    run_dir = tmp_path / ctx.run_id
    trace_path = run_dir / "run_trace.json"
    assert trace_path.is_file()
    data = json.loads(trace_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    summary_path = run_dir / "run_summary.json"
    assert summary_path.is_file()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "error"
    assert summary["run_id"] == ctx.run_id
    assert summary.get("error", {}).get("type") == "RuntimeError"
    assert "n0 boom" in (summary.get("error", {}).get("message") or "")
    log_path = run_dir / "run_log.json"
    assert log_path.is_file()
    log_data = json.loads(log_path.read_text(encoding="utf-8"))
    assert log_data["status"] == "error"
