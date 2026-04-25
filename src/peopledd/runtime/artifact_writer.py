from __future__ import annotations

from pathlib import Path

from peopledd.models.contracts import FinalReport, GovernanceSeed, InputPayload
from peopledd.runtime.artifact_policy import DD_BRIEF_FILENAME, artifact_include
from peopledd.runtime.context import RunContext
from peopledd.runtime.run_metadata import build_dd_brief, build_run_summary
from peopledd.utils.io import write_json, write_text


def write_success_pipeline_artifacts(
    base: Path,
    mode: str,
    ctx: RunContext,
    input_payload: InputPayload,
    final_report: FinalReport,
    md_report: str,
    *,
    governance_seed: GovernanceSeed | None = None,
) -> None:
    """Write per-run JSON/MD artifacts and run_summary for a successful pipeline."""
    entity = final_report.entity_resolution
    ingestion = final_report.governance
    reconciliation = final_report.governance_reconciliation
    semantic_fusion = final_report.semantic_governance_fusion
    people_resolution = final_report.people_resolution
    people_profiles = final_report.people_profiles
    strategy = final_report.strategy_and_challenges
    market_pulse = final_report.market_pulse
    capability_model = final_report.required_capability_model
    coverage = final_report.coverage_scoring
    evidence = final_report.evidence_pack
    degradation_profile = final_report.degradation_profile

    if artifact_include("input", mode):
        write_json(base / "input.json", input_payload.model_dump(mode="json"))
    if artifact_include("entity_resolution", mode):
        write_json(base / "entity_resolution.json", entity.model_dump(mode="json"))
    if artifact_include("governance_seed", mode) and governance_seed is not None:
        write_json(base / "governance_seed.json", governance_seed.model_dump(mode="json"))
    if artifact_include("governance_formal", mode):
        write_json(
            base / "governance_formal.json",
            ingestion.formal_governance_snapshot.model_dump(mode="json"),
        )
    if artifact_include("governance_current", mode):
        write_json(
            base / "governance_current.json",
            ingestion.current_governance_snapshot.model_dump(mode="json"),
        )
    if artifact_include("governance_reconciliation", mode):
        write_json(base / "governance_reconciliation.json", reconciliation.model_dump(mode="json"))
    if artifact_include("semantic_governance_fusion", mode):
        write_json(
            base / "semantic_governance_fusion.json",
            semantic_fusion.model_dump(mode="json"),
        )
    if artifact_include("people_resolution", mode):
        write_json(
            base / "people_resolution.json", [p.model_dump(mode="json") for p in people_resolution]
        )
    if artifact_include("people_profiles", mode):
        write_json(
            base / "people_profiles.json", [p.model_dump(mode="json") for p in people_profiles]
        )
    if artifact_include("strategy_and_challenges", mode):
        write_json(base / "strategy_and_challenges.json", strategy.model_dump(mode="json"))
    if artifact_include("market_pulse", mode):
        write_json(base / "market_pulse.json", market_pulse.model_dump(mode="json"))
    if artifact_include("required_capability_model", mode):
        write_json(base / "required_capability_model.json", capability_model.model_dump(mode="json"))
    if artifact_include("coverage_scoring", mode):
        write_json(base / "coverage_scoring.json", coverage.model_dump(mode="json"))
    if artifact_include("improvement_hypotheses", mode):
        write_json(
            base / "improvement_hypotheses.json",
            [h.model_dump(mode="json") for h in final_report.improvement_hypotheses],
        )
    if artifact_include("evidence_pack", mode):
        write_json(base / "evidence_pack.json", evidence.model_dump(mode="json"))
    if artifact_include("degradation_profile", mode):
        write_json(base / "degradation_profile.json", degradation_profile.model_dump(mode="json"))
    if artifact_include("final_report_json", mode):
        write_json(base / "final_report.json", final_report.model_dump(mode="json"))
    if artifact_include("final_report_md", mode):
        write_text(base / "final_report.md", md_report)
    if artifact_include("run_log", mode):
        write_json(
            base / "run_log.json",
            {
                "run_id": ctx.run_id,
                "status": "ok",
                "recovery_counts": ctx.recovery_counts,
                "llm_calls_used": ctx.llm_calls_used,
                "output_mode": mode,
            },
        )
    if artifact_include("run_trace", mode):
        write_json(base / "run_trace.json", ctx.trace_to_json())
    write_json(
        base / "run_summary.json",
        build_run_summary(final_report, ctx.run_id, base, mode, "ok"),
    )
    write_json(base / DD_BRIEF_FILENAME, build_dd_brief(final_report, ctx.run_id))
