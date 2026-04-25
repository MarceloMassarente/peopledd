from __future__ import annotations

"""
Minimal LangGraph spike: linear two-node graph (no LLM, no I/O).

Install optional extra: pip install -e ".[langgraph-spike]"
Run: python -m peopledd.experimental.langgraph_linear_spike
"""

from typing import TypedDict


class _DemoState(TypedDict, total=False):
    step: str
    counter: int


def main() -> int:
    try:
        from langgraph.graph import END, StateGraph
    except ImportError:
        print("langgraph not installed; use: pip install -e \".[langgraph-spike]\"")
        return 0

    def node_a(state: _DemoState) -> _DemoState:
        return {"step": "a_done", "counter": int(state.get("counter", 0)) + 1}

    def node_b(state: _DemoState) -> _DemoState:
        return {"step": "b_done", "counter": int(state.get("counter", 0)) + 1}

    g = StateGraph(_DemoState)
    g.add_node("a", node_a)
    g.add_node("b", node_b)
    g.set_entry_point("a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    app = g.compile()
    out = app.invoke({"counter": 0})
    assert out.get("step") == "b_done"
    assert out.get("counter") == 2
    print("langgraph_linear_spike: ok", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
