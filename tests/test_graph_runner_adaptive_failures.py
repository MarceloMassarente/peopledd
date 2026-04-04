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

    with patch("peopledd.runtime.graph_runner.n0_entity_resolution.run", side_effect=RuntimeError("n0 boom")):
        with pytest.raises(RuntimeError, match="n0 boom"):
            runner.run(InputPayload(company_name="X"))

    run_dir = tmp_path / ctx.run_id
    trace_path = run_dir / "run_trace.json"
    assert trace_path.is_file()
    data = json.loads(trace_path.read_text(encoding="utf-8"))
    assert isinstance(data, list)
