from __future__ import annotations

import os

from peopledd.models.contracts import InputPayload


def resolve_run_limits(payload: InputPayload) -> tuple[int, int]:
    def from_env(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None or not str(raw).strip():
            return default
        try:
            return max(1, int(raw))
        except ValueError:
            return default

    max_llm = (
        int(payload.max_llm_calls)
        if payload.max_llm_calls is not None
        else from_env("PEOPLEDD_MAX_LLM_CALLS", 24)
    )
    max_rec = (
        int(payload.max_recovery_steps)
        if payload.max_recovery_steps is not None
        else from_env("PEOPLEDD_MAX_RECOVERY_STEPS", 8)
    )
    return max(1, max_llm), max(1, max_rec)


def env_post_strategy_checkpoint() -> bool:
    v = os.environ.get("PEOPLEDD_POST_STRATEGY_CHECKPOINT", "")
    return str(v).strip().lower() in ("1", "true", "yes", "on")
