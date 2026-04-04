from __future__ import annotations

from contextvars import ContextVar
from typing import Any

from peopledd.runtime.context import RunContext

_attached: ContextVar[RunContext | None] = ContextVar("peopledd_run_context", default=None)


def attach_run_context(ctx: RunContext) -> object:
    return _attached.set(ctx)


def detach_run_context(token: object) -> None:
    _attached.reset(token)


def get_attached_run_context() -> RunContext | None:
    return _attached.get()


def try_consume_llm_call(step: str) -> bool:
    """
    Reserve one LLM call against RunContext.max_llm_calls when a pipeline run is active.
    Outside a run (no attached context), always returns True.
    """
    ctx = get_attached_run_context()
    if ctx is None:
        return True
    if ctx.llm_calls_used >= ctx.max_llm_calls:
        ctx.llm_budget_skips.append(f"{step}:budget_exhausted")
        return False
    ctx.llm_calls_used += 1
    return True


def record_llm_route(channel: str, used_llm: bool, reason: str = "") -> None:
    ctx = get_attached_run_context()
    if ctx is None:
        return
    ctx.llm_routes.append({"channel": channel, "used_llm": used_llm, "reason": reason})
