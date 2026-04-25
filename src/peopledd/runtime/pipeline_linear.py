from __future__ import annotations

from pathlib import Path
from typing import Any

from peopledd.models.contracts import InputPayload
from peopledd.runtime.adaptive_models import PipelineSearchPlanState
from peopledd.runtime.artifact_policy import validate_output_mode
from peopledd.runtime.pipeline_state import (
    CheckpointPhase,
    PipelineState,
    checkpoint_input_fingerprint,
    read_checkpoint,
    remove_checkpoint,
    write_checkpoint,
)
from peopledd.runtime.run_limits import env_post_strategy_checkpoint


def execute_linear_pipeline(runner: Any, input_payload: InputPayload, base: Path) -> Any:
    """Checkpoint resume, macro-phases, optional post-strategy checkpoint; delegates phase work to runner."""
    ctx = runner.ctx
    mode = input_payload.output_mode
    validate_output_mode(mode)

    ctx.log("start", "pipeline", "run_begin", run_id=ctx.run_id)

    search_plan = PipelineSearchPlanState()
    expected_fp = checkpoint_input_fingerprint(input_payload)
    resume = read_checkpoint(base)
    resume_ok = False
    resume_phase: CheckpointPhase | None = None
    state = PipelineState()
    cp_reason: str | None = None

    if resume is not None:
        rid, resume_phase, loaded_state, loaded_plan, stored_fp = resume
        if rid != ctx.run_id:
            cp_reason = "run_id_mismatch"
        elif stored_fp is None:
            ctx.log("gap", "pipeline", "checkpoint_missing_fingerprint", run_id=rid)
            cp_reason = "missing_fingerprint"
        elif stored_fp != expected_fp:
            ctx.log("gap", "pipeline", "checkpoint_fingerprint_mismatch", run_id=rid)
            remove_checkpoint(base)
            cp_reason = "fingerprint_mismatch"
        else:
            resume_ok = True
            state = loaded_state
            search_plan = loaded_plan
            ctx.log("start", "pipeline", "resume_from_checkpoint", phase=resume_phase)

    ctx.checkpoint_meta = {
        "used": resume_ok,
        "written": False,
        "phase": str(resume_phase if resume_ok and resume_phase else "post_people"),
        "reason_skipped": cp_reason,
    }

    if resume_ok and resume_phase == "post_strategy":
        ctx.begin_phase("scoring")
        try:
            return runner._run_scoring_phase(input_payload, state, base)
        finally:
            ctx.end_phase("scoring")

    if not resume_ok:
        ctx.begin_phase("governance")
        try:
            runner._run_governance_phase(input_payload, state, search_plan)
        finally:
            ctx.end_phase("governance")

        ctx.begin_phase("people")
        try:
            runner._run_people_phase(input_payload, state, search_plan)
        finally:
            ctx.end_phase("people")

        write_checkpoint(
            base,
            ctx.run_id,
            "post_people",
            state,
            search_plan,
            input_fingerprint=expected_fp,
        )
        ctx.checkpoint_meta["written"] = True
        ctx.checkpoint_meta["phase"] = "post_people"
        ctx.checkpoint_meta["reason_skipped"] = None

    ctx.begin_phase("strategy")
    try:
        runner._run_strategy_phase(input_payload, state, search_plan)
    finally:
        ctx.end_phase("strategy")

    if env_post_strategy_checkpoint():
        write_checkpoint(
            base,
            ctx.run_id,
            "post_strategy",
            state,
            search_plan,
            input_fingerprint=expected_fp,
        )
        ctx.checkpoint_meta["written"] = True
        ctx.checkpoint_meta["phase"] = "post_strategy"

    ctx.begin_phase("scoring")
    try:
        return runner._run_scoring_phase(input_payload, state, base)
    finally:
        ctx.end_phase("scoring")
