from __future__ import annotations

from peopledd.runtime.circuit_breaker import SourceCircuitBreaker, WeightedCircuitBreaker, default_breaker_set
from peopledd.runtime.context import RunContext, RunTraceEvent
from peopledd.runtime.staleness import compute_staleness_and_sl_dimensions

__all__ = [
    "RunContext",
    "RunTraceEvent",
    "SourceCircuitBreaker",
    "WeightedCircuitBreaker",
    "default_breaker_set",
    "GraphRunner",
    "run_pipeline_graph",
    "compute_staleness_and_sl_dimensions",
]


def __getattr__(name: str):
    if name == "GraphRunner":
        from peopledd.runtime.graph_runner import GraphRunner

        return GraphRunner
    if name == "run_pipeline_graph":
        from peopledd.runtime.graph_runner import run_pipeline_graph

        return run_pipeline_graph
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
