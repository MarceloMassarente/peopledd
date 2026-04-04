from __future__ import annotations

"""
Thin facade: pipeline execution lives in runtime.graph_runner (policy + trace + recovery).
"""

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.runtime.graph_runner import run_pipeline_graph


def run_pipeline(input_payload: InputPayload, output_dir: str = "run") -> FinalReport:
    return run_pipeline_graph(input_payload, output_dir=output_dir)
