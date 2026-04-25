from __future__ import annotations

from pathlib import Path
from typing import Any

from peopledd.models.common import ServiceLevel
from peopledd.models.contracts import (
    ConfidencePolicy,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    InputPayload,
    PipelineTelemetry,
)
from peopledd.nodes import (
    n5_required_capability_model,
    n6_coverage_scoring,
    n7_improvement_hypotheses,
    n8_evidence_pack,
    n9_report_builder,
)
from peopledd.pipeline_helpers import assign_service_level, infer_sector_key
from peopledd.runtime.artifact_writer import write_success_pipeline_artifacts
from peopledd.runtime.pipeline_merge import aggregate_harvest_recall_totals
from peopledd.runtime.pipeline_state import PipelineState, remove_checkpoint
from peopledd.runtime.staleness import compute_staleness_and_sl_dimensions


def run(runner: Any, input_payload: InputPayload, state: PipelineState, base: Path) -> FinalReport:
    ctx = runner.ctx
    mode = input_payload.output_mode
    entity = state.entity
    ingestion = state.ingestion
    reconciliation = state.reconciliation
    semantic_fusion = state.semantic_fusion
    people_resolution = state.people_resolution
    people_profiles = state.people_profiles
    strategy = state.strategy
    market_pulse = state.market_pulse
    assert entity is not None and ingestion is not None and reconciliation is not None
    assert semantic_fusion is not None and strategy is not None and market_pulse is not None

    sector_key = infer_sector_key(entity, input_payload)

    ctx.log("start", "n5", "capability_model")
    capability_model = n5_required_capability_model.run(sector_key, strategy)
    ctx.log("end", "n5", "capability_model_ok")
    state.capability_model = capability_model

    board_size = len(reconciliation.reconciled_governance_snapshot.board_members)
    exec_size = len(reconciliation.reconciled_governance_snapshot.executive_members)
    ctx.log("start", "n6", "coverage_scoring")
    coverage = n6_coverage_scoring.run(
        capability_model, people_profiles, board_size=board_size, executive_size=exec_size
    )
    ctx.log("end", "n6", "coverage_scoring_ok")
    state.coverage = coverage

    useful_board = 0.0
    if people_profiles and board_size:
        board_ids = {p.person_name for p in reconciliation.reconciled_governance_snapshot.board_members}
        board_profiles = [
            pp for pp, pr in zip(people_profiles, people_resolution) if pr.observed_name in board_ids
        ]
        if board_profiles:
            useful_board = sum(p.profile_quality.useful_coverage_score for p in board_profiles) / len(
                board_profiles
            )

    data_completeness = (
        ingestion.governance_data_quality.formal_completeness
        + ingestion.governance_data_quality.current_completeness
    ) / 2
    evidence_quality = max(
        0.0,
        min(
            1.0,
            sum(p.profile_quality.evidence_density for p in people_profiles) / max(1, len(people_profiles)),
        ),
    )
    analytical_confidence = max(0.0, min(1.0, (data_completeness * 0.4) + (evidence_quality * 0.6)))

    private_web_used = ingestion.ingestion_metadata.get("private_web_discovery") == "1"

    sl, degradations, disclaimers = assign_service_level(
        formal_completeness=ingestion.governance_data_quality.formal_completeness,
        current_completeness=ingestion.governance_data_quality.current_completeness,
        useful_coverage_board=useful_board,
        entity_resolved=entity.resolution_status in {"resolved", "partial"},
        mode=entity.company_mode.value,
        private_web_governance_used=private_web_used,
    )

    staleness_flags, sl_by_dim = compute_staleness_and_sl_dimensions(ingestion, people_profiles, sl)

    degradation_profile = DegradationProfile(
        service_level=sl,
        degradations=degradations,
        omitted_sections=[] if sl != ServiceLevel.SL5 else ["improvement_hypotheses"],
        mandatory_disclaimers=disclaimers,
        sl_by_dimension=sl_by_dim,
        staleness_by_dimension=staleness_flags,
    )
    state.degradation_profile = degradation_profile

    confidence_policy = ConfidencePolicy(
        data_completeness_score=round(data_completeness, 2),
        evidence_quality_score=round(evidence_quality, 2),
        analytical_confidence_score=round(analytical_confidence, 2),
    )
    state.confidence_policy = confidence_policy

    draft_report = FinalReport(
        input_payload=input_payload,
        entity_resolution=entity,
        governance=ingestion,
        governance_reconciliation=reconciliation,
        semantic_governance_fusion=semantic_fusion,
        people_resolution=people_resolution,
        people_profiles=people_profiles,
        strategy_and_challenges=strategy,
        market_pulse=market_pulse,
        required_capability_model=capability_model,
        coverage_scoring=coverage,
        improvement_hypotheses=[],
        evidence_pack=EvidencePack(documents=[], claims=[]),
        degradation_profile=degradation_profile,
        confidence_policy=confidence_policy,
    )

    ctx.log("start", "n8", "evidence_pack")
    evidence = n8_evidence_pack.run(partial_report=draft_report, run_id=ctx.run_id)
    ctx.log("end", "n8", "evidence_pack_ok", documents=len(evidence.documents))
    state.evidence = evidence

    ctx.log("start", "n7", "improvement_hypotheses")
    hypotheses = n7_improvement_hypotheses.run(
        coverage,
        strategy,
        analytical_confidence,
        evidence_pack=evidence,
        governance_reconciliation=reconciliation,
        people_resolution=people_resolution,
        people_profiles=people_profiles,
        degradation_profile=degradation_profile,
    )
    ctx.log("end", "n7", "improvement_hypotheses_ok", count=len(hypotheses))
    state.hypotheses = hypotheses
    ctx.log("end", "pipeline", "run_complete")

    telemetry = PipelineTelemetry(
        run_id=ctx.run_id,
        trace_events=ctx.trace_to_json(),
        recovery_counts=dict(ctx.recovery_counts),
        circuit_states={k: str(v.snapshot()["state"]) for k, v in runner.breakers.items()},
        harvest_recall_totals=aggregate_harvest_recall_totals(people_resolution),
        llm_calls_used=ctx.llm_calls_used,
        llm_budget_skips=list(ctx.llm_budget_skips),
        llm_routes=list(ctx.llm_routes),
        adaptive_decisions=list(ctx.adaptive_decisions),
        search_attempts=list(ctx.search_attempts),
        per_phase_durations_ms=dict(ctx.per_phase_durations_ms),
        checkpoint_meta=dict(ctx.checkpoint_meta),
    )

    final_report = draft_report.model_copy(
        update={
            "evidence_pack": evidence,
            "improvement_hypotheses": hypotheses,
            "pipeline_telemetry": telemetry,
        }
    )

    md_report = n9_report_builder.to_markdown(final_report)

    try:
        write_success_pipeline_artifacts(
            base,
            mode,
            ctx,
            input_payload,
            final_report,
            md_report,
            governance_seed=state.governance_seed,
        )
    except OSError as exc:
        runner._write_error_run_summary_artifact_write(base, mode, exc)
        raise
    remove_checkpoint(base)
    return final_report
