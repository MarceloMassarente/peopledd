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
    n0_entity_resolution,
    n1_governance_ingestion,
    n1b_reconciliation,
    n2_person_resolution,
    n3_profile_enrichment,
    n4_strategy_inference,
    n5_required_capability_model,
    n6_coverage_scoring,
    n7_improvement_hypotheses,
    n8_evidence_pack,
    n9_report_builder,
)
from peopledd.pipeline_helpers import (
    assign_service_level,
    build_search_orchestrator,
    canonical_company_name,
    infer_sector_key,
)
from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, PipelineSearchPlanState
from peopledd.runtime.adaptive_policy import DefaultAdaptivePolicy
from peopledd.runtime.circuit_breaker import SourceCircuitBreaker, default_breaker_set
from peopledd.runtime.context import RunContext
from peopledd.runtime.pipeline_context import attach_run_context, detach_run_context
from peopledd.runtime.staleness import compute_staleness_and_sl_dimensions
from peopledd.services.connectors import CVMConnector, RIConnector
from peopledd.services.harvest_adapter import HarvestAdapter
from peopledd.utils.io import ensure_dir, write_json, write_text

def _strategy_is_empty(raw_sc: Any) -> bool:
    return not raw_sc.strategic_priorities and not raw_sc.key_challenges


def _aggregate_harvest_recall_totals(people_resolution: list[Any]) -> dict[str, int]:
    totals: dict[str, int] = {
        "raw_hits_profile_search_sum": 0,
        "after_filter_count_sum": 0,
        "anonymized_dropped_count_sum": 0,
        "people_with_profile_search_retry": 0,
        "people_with_secondary_web_sourcing": 0,
        "people_with_resolution_attempted": 0,
    }
    for pr in people_resolution:
        h = getattr(pr, "harvest_recall", None)
        if h is None:
            continue
        totals["raw_hits_profile_search_sum"] += int(h.raw_hits_profile_search)
        totals["after_filter_count_sum"] += int(h.after_filter_count)
        totals["anonymized_dropped_count_sum"] += int(h.anonymized_dropped_count)
        if h.profile_search_retry_used:
            totals["people_with_profile_search_retry"] += 1
        if h.secondary_web_sourcing_used:
            totals["people_with_secondary_web_sourcing"] += 1
        if h.resolution_attempted:
            totals["people_with_resolution_attempted"] += 1
    return totals


_REPORT_ARTIFACT_KEYS = frozenset({
    "input",
    "run_trace",
    "run_log",
    "final_report_json",
    "final_report_md",
    "degradation_profile",
})


def _artifact_include(artifact_key: str, mode: str) -> bool:
    if mode == "both":
        return True
    if mode == "json":
        return artifact_key != "final_report_md"
    if mode == "report":
        return artifact_key in _REPORT_ARTIFACT_KEYS
    return True


class GraphRunner:
    """
    Linear pipeline executor (n0-n9) with rubric-driven adaptive recovery:
    n1 FRE extended, n4 wider crawl plus search-parameter escalation, optional n2/n3
    person LinkedIn query escalation. Policy is implemented by DefaultAdaptivePolicy
    (injectable for tests).
    """

    def __init__(
        self,
        ctx: RunContext,
        cvm: CVMConnector,
        ri: RIConnector,
        harvest: HarvestAdapter,
        search_orch: Any,
        breakers: dict[str, SourceCircuitBreaker] | None = None,
        adaptive_policy: DefaultAdaptivePolicy | None = None,
    ):
        self.ctx = ctx
        self.cvm = cvm
        self.ri = ri
        self.harvest = harvest
        self.search_orch = search_orch
        self.breakers = breakers or default_breaker_set()
        self.adaptive_policy = adaptive_policy or DefaultAdaptivePolicy()

    def _breaker_success(self, key: str) -> None:
        b = self.breakers[key]
        b.record_success()
        snap = b.snapshot()
        self.ctx.log(
            "circuit",
            key,
            "record_success",
            state=str(snap["state"]),
            failures=int(snap["failures"]),
        )

    def _breaker_failure(self, key: str) -> None:
        b = self.breakers[key]
        b.record_failure()
        snap = b.snapshot()
        self.ctx.log(
            "circuit",
            key,
            "record_failure",
            state=str(snap["state"]),
            failures=int(snap["failures"]),
        )

    def _write_emergency_trace(self, status: str) -> None:
        ctx = self.ctx
        base = ctx.output_base
        try:
            ensure_dir(base)
            write_json(base / "run_trace.json", ctx.trace_to_json())
            write_json(
                base / "run_log.json",
                {
                    "run_id": ctx.run_id,
                    "status": status,
                    "recovery_counts": dict(ctx.recovery_counts),
                    "llm_calls_used": ctx.llm_calls_used,
                },
            )
        except OSError:
            pass

    def run(self, input_payload: InputPayload) -> FinalReport:
        ctx = self.ctx
        base = ctx.output_base
        ensure_dir(base)
        cache_dir = base / "cache"
        ensure_dir(cache_dir)

        token = attach_run_context(ctx)
        try:
            try:
                return self._run_pipeline(input_payload, base)
            except Exception:
                self._write_emergency_trace("error")
                raise
        finally:
            detach_run_context(token)

    def _run_pipeline(self, input_payload: InputPayload, base: Path) -> FinalReport:
        ctx = self.ctx
        mode = input_payload.output_mode

        ctx.log("start", "pipeline", "run_begin", run_id=ctx.run_id)

        # n0
        ctx.log("start", "n0", "entity_resolution")
        try:
            entity = n0_entity_resolution.run(input_payload, self.cvm, self.ri)
            self._breaker_success("cvm")
        except Exception:
            self._breaker_failure("cvm")
            ctx.log("end", "n0", "entity_resolution_failed")
            raise
        ctx.log("end", "n0", "entity_resolution_ok", status=entity.resolution_status.value)

        company_name = canonical_company_name(entity, input_payload) or input_payload.company_name
        search_plan = PipelineSearchPlanState()
        policy = self.adaptive_policy

        # n1 with optional extended FRE recovery (adaptive policy)
        ctx.log("start", "n1", "governance_ingestion")
        ingestion = n1_governance_ingestion.run(
            company_name,
            cnpj=entity.cnpj,
            ri_url=entity.ri_url,
            fre_extended_probe=False,
        )
        ctx.log("end", "n1", "governance_ingestion_first_pass", formal=ingestion.governance_data_quality.formal_completeness)

        a1 = policy.build_n1_assessment(ingestion, bool(entity.cnpj), self.search_orch is not None)
        act1, rationale1, rk1 = policy.decide_n1_fre_extended(
            a1, ingestion, bool(entity.cnpj), ctx, self.breakers
        )
        ctx.record_adaptive_decision(
            AdaptiveDecisionRecord(
                sequence=0,
                checkpoint="n1_post_ingestion",
                action=act1,
                rationale=rationale1,
                recovery_key=rk1,
            )
        )

        if act1 == "retry_n1_fre_extended" and rk1:
            ctx.log("policy", "n1", "retry_fre_extended_probe")
            ctx.bump_recovery(rk1)
            ingestion_retry = n1_governance_ingestion.run(
                company_name,
                cnpj=entity.cnpj,
                ri_url=entity.ri_url,
                fre_extended_probe=True,
            )
            if ingestion_retry.governance_data_quality.formal_completeness > ingestion.governance_data_quality.formal_completeness:
                ingestion = ingestion_retry
                self._breaker_success("fre")
                ctx.log("recovery", "n1", "fre_extended_improved", formal=ingestion.governance_data_quality.formal_completeness)
            else:
                self._breaker_failure("fre")
                ctx.log("recovery", "n1", "fre_extended_no_gain")

        # n1b
        ctx.log("start", "n1b", "reconciliation")
        reconciliation = n1b_reconciliation.run(ingestion)
        ctx.log("end", "n1b", "reconciliation_ok", conflicts=len(reconciliation.conflict_items))

        if self.search_orch is None:
            ctx.log(
                "gap",
                "n2",
                "search_orchestrator_not_configured",
                reason="EXA_API_KEY or SEARXNG_URL missing",
            )

        # n2, n3
        ctx.log("start", "n2", "person_resolution")
        people_resolution = n2_person_resolution.run(
            reconciliation,
            self.harvest,
            company_name=company_name,
            search_orchestrator=self.search_orch,
            use_harvest=input_payload.use_harvest,
            person_search_params=search_plan.person_params,
        )
        ctx.log("end", "n2", "person_resolution_ok", count=len(people_resolution))

        ctx.log("start", "n3", "profile_enrichment")
        people_profiles = n3_profile_enrichment.run(
            people_resolution,
            self.harvest,
            use_harvest=input_payload.use_harvest,
        )
        ctx.log("end", "n3", "profile_enrichment_ok", count=len(people_profiles))

        board_names = {m.person_name for m in reconciliation.reconciled_governance_snapshot.board_members}
        a2 = policy.build_n2n3_assessment(people_profiles, people_resolution, board_names)
        act2, rationale2, rk2 = policy.decide_n2_person_search_escalation(
            a2,
            ctx,
            self.breakers,
            self.search_orch is not None,
            person_escalation_already_applied=search_plan.person_params.escalation_level > 0,
        )
        ctx.record_adaptive_decision(
            AdaptiveDecisionRecord(
                sequence=0,
                checkpoint="n2n3_post_profiles",
                action=act2,
                rationale=rationale2,
                recovery_key=rk2,
            )
        )

        if act2 == "rerun_n2n3_person_search_escalation" and rk2:
            ctx.log("policy", "n2", "person_search_escalation_rerun")
            search_plan.escalate_person_secondary()
            ctx.bump_recovery(rk2)
            ctx.log("start", "n2", "person_resolution_recovery")
            people_resolution = n2_person_resolution.run(
                reconciliation,
                self.harvest,
                company_name=company_name,
                search_orchestrator=self.search_orch,
                use_harvest=input_payload.use_harvest,
                person_search_params=search_plan.person_params,
            )
            ctx.log("end", "n2", "person_resolution_recovery_ok", count=len(people_resolution))
            ctx.log("start", "n3", "profile_enrichment_recovery")
            people_profiles = n3_profile_enrichment.run(
                people_resolution,
                self.harvest,
                use_harvest=input_payload.use_harvest,
            )
            ctx.log("end", "n3", "profile_enrichment_recovery_ok", count=len(people_profiles))

        sector_key = infer_sector_key(entity, input_payload)

        strategy_attempt_idx = 0
        find_escalation_level = 0

        def _run_n4(max_pages: int | None, find_fp: Any, attempt_idx: int, esc_level: int) -> Any:
            return n4_strategy_inference.run(
                company_name,
                ri_url=entity.ri_url,
                sector=sector_key,
                country=input_payload.country,
                strategy_max_pages=max_pages,
                search_orchestrator=self.search_orch,
                find_urls_params=find_fp,
                strategy_search_attempt_index=attempt_idx,
                find_urls_escalation_level=esc_level,
            )

        ctx.log("start", "n4", "strategy_inference")
        strategy = _run_n4(
            search_plan.strategy_max_pages,
            search_plan.find_urls_params,
            strategy_attempt_idx,
            find_escalation_level,
        )
        strategy_attempt_idx += 1
        ctx.log("end", "n4", "strategy_first_pass", priorities=len(strategy.strategic_priorities))
        sonar_briefs_fallback = list(strategy.external_sonar_briefs)

        a4 = policy.build_n4_assessment(strategy)
        act4, rationale4, rk4 = policy.decide_n4_widen_pages(a4, ctx, self.breakers)
        ctx.record_adaptive_decision(
            AdaptiveDecisionRecord(
                sequence=0,
                checkpoint="n4_post_strategy",
                action=act4,
                rationale=rationale4,
                recovery_key=rk4,
            )
        )

        widen_attempted = False
        if act4 == "retry_n4_widen_pages" and rk4:
            widen_attempted = True
            ctx.log("policy", "n4", "retry_wider_strategy_crawl")
            ctx.bump_recovery(rk4)
            search_plan.strategy_max_pages = 8
            strategy_retry = _run_n4(
                8,
                search_plan.find_urls_params,
                strategy_attempt_idx,
                find_escalation_level,
            )
            strategy_attempt_idx += 1
            if not _strategy_is_empty(strategy_retry):
                strategy = strategy_retry
                self._breaker_success("strategy_llm")
                ctx.log("recovery", "n4", "strategy_retry_populated")
            else:
                strategy = strategy_retry
                self._breaker_failure("strategy_llm")
                ctx.log("recovery", "n4", "strategy_retry_still_empty")

        if _strategy_is_empty(strategy):
            a4b = policy.build_n4_assessment(strategy)
            act5, rationale5, rk5 = policy.decide_n4_search_escalation(
                a4b, ctx, self.breakers, widen_attempted
            )
            ctx.record_adaptive_decision(
                AdaptiveDecisionRecord(
                    sequence=0,
                    checkpoint="n4_post_strategy",
                    action=act5,
                    rationale=rationale5,
                    recovery_key=rk5,
                )
            )
            if act5 == "retry_n4_search_escalation" and rk5:
                ctx.log("policy", "n4", "retry_search_escalation")
                ctx.bump_recovery(rk5)
                search_plan.escalate_strategy_find_urls()
                find_escalation_level = 1
                max_p = search_plan.strategy_max_pages if search_plan.strategy_max_pages is not None else 8
                strategy_retry_b = _run_n4(
                    max_p,
                    search_plan.find_urls_params,
                    strategy_attempt_idx,
                    find_escalation_level,
                )
                if not _strategy_is_empty(strategy_retry_b):
                    strategy = strategy_retry_b
                    self._breaker_success("strategy_llm")
                    ctx.log("recovery", "n4", "strategy_search_escalation_populated")
                else:
                    strategy = strategy_retry_b
                    self._breaker_failure("strategy_llm")
                    ctx.log("recovery", "n4", "strategy_search_escalation_still_empty")

        if not strategy.external_sonar_briefs and sonar_briefs_fallback:
            strategy = strategy.model_copy(update={"external_sonar_briefs": sonar_briefs_fallback})

        ctx.log("start", "n5", "capability_model")
        capability_model = n5_required_capability_model.run(sector_key, strategy)
        ctx.log("end", "n5", "capability_model_ok")

        board_size = len(reconciliation.reconciled_governance_snapshot.board_members)
        exec_size = len(reconciliation.reconciled_governance_snapshot.executive_members)
        ctx.log("start", "n6", "coverage_scoring")
        coverage = n6_coverage_scoring.run(capability_model, people_profiles, board_size=board_size, executive_size=exec_size)
        ctx.log("end", "n6", "coverage_scoring_ok")

        useful_board = 0.0
        if people_profiles and board_size:
            board_ids = {p.person_name for p in reconciliation.reconciled_governance_snapshot.board_members}
            board_profiles = [pp for pp, pr in zip(people_profiles, people_resolution) if pr.observed_name in board_ids]
            if board_profiles:
                useful_board = sum(p.profile_quality.useful_coverage_score for p in board_profiles) / len(board_profiles)

        data_completeness = (
            ingestion.governance_data_quality.formal_completeness + ingestion.governance_data_quality.current_completeness
        ) / 2
        evidence_quality = max(
            0.0,
            min(1.0, sum(p.profile_quality.evidence_density for p in people_profiles) / max(1, len(people_profiles))),
        )
        analytical_confidence = max(0.0, min(1.0, (data_completeness * 0.4) + (evidence_quality * 0.6)))

        sl, degradations, disclaimers = assign_service_level(
            formal_completeness=ingestion.governance_data_quality.formal_completeness,
            current_completeness=ingestion.governance_data_quality.current_completeness,
            useful_coverage_board=useful_board,
            entity_resolved=entity.resolution_status in {"resolved", "partial"},
            mode=entity.company_mode.value,
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

        confidence_policy = ConfidencePolicy(
            data_completeness_score=round(data_completeness, 2),
            evidence_quality_score=round(evidence_quality, 2),
            analytical_confidence_score=round(analytical_confidence, 2),
        )

        draft_report = FinalReport(
            input_payload=input_payload,
            entity_resolution=entity,
            governance=ingestion,
            governance_reconciliation=reconciliation,
            people_resolution=people_resolution,
            people_profiles=people_profiles,
            strategy_and_challenges=strategy,
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
        ctx.log("end", "pipeline", "run_complete")

        telemetry = PipelineTelemetry(
            run_id=ctx.run_id,
            trace_events=ctx.trace_to_json(),
            recovery_counts=dict(ctx.recovery_counts),
            circuit_states={k: str(v.snapshot()["state"]) for k, v in self.breakers.items()},
            harvest_recall_totals=_aggregate_harvest_recall_totals(people_resolution),
            llm_calls_used=ctx.llm_calls_used,
            llm_budget_skips=list(ctx.llm_budget_skips),
            llm_routes=list(ctx.llm_routes),
            adaptive_decisions=list(ctx.adaptive_decisions),
            search_attempts=list(ctx.search_attempts),
        )

        final_report = draft_report.model_copy(
            update={
                "evidence_pack": evidence,
                "improvement_hypotheses": hypotheses,
                "pipeline_telemetry": telemetry,
            }
        )

        md_report = n9_report_builder.to_markdown(final_report)

        if _artifact_include("input", mode):
            write_json(base / "input.json", input_payload.model_dump(mode="json"))
        if _artifact_include("entity_resolution", mode):
            write_json(base / "entity_resolution.json", entity.model_dump(mode="json"))
        if _artifact_include("governance_formal", mode):
            write_json(base / "governance_formal.json", ingestion.formal_governance_snapshot.model_dump(mode="json"))
        if _artifact_include("governance_current", mode):
            write_json(base / "governance_current.json", ingestion.current_governance_snapshot.model_dump(mode="json"))
        if _artifact_include("governance_reconciliation", mode):
            write_json(base / "governance_reconciliation.json", reconciliation.model_dump(mode="json"))
        if _artifact_include("people_resolution", mode):
            write_json(base / "people_resolution.json", [p.model_dump(mode="json") for p in people_resolution])
        if _artifact_include("people_profiles", mode):
            write_json(base / "people_profiles.json", [p.model_dump(mode="json") for p in people_profiles])
        if _artifact_include("strategy_and_challenges", mode):
            write_json(base / "strategy_and_challenges.json", strategy.model_dump(mode="json"))
        if _artifact_include("required_capability_model", mode):
            write_json(base / "required_capability_model.json", capability_model.model_dump(mode="json"))
        if _artifact_include("coverage_scoring", mode):
            write_json(base / "coverage_scoring.json", coverage.model_dump(mode="json"))
        if _artifact_include("improvement_hypotheses", mode):
            write_json(
                base / "improvement_hypotheses.json",
                [h.model_dump(mode="json") for h in final_report.improvement_hypotheses],
            )
        if _artifact_include("evidence_pack", mode):
            write_json(base / "evidence_pack.json", evidence.model_dump(mode="json"))
        if _artifact_include("degradation_profile", mode):
            write_json(base / "degradation_profile.json", degradation_profile.model_dump(mode="json"))
        if _artifact_include("final_report_json", mode):
            write_json(base / "final_report.json", final_report.model_dump(mode="json"))
        if _artifact_include("final_report_md", mode):
            write_text(base / "final_report.md", md_report)
        if _artifact_include("run_log", mode):
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
        if _artifact_include("run_trace", mode):
            write_json(base / "run_trace.json", ctx.trace_to_json())
        return final_report


def run_pipeline_graph(input_payload: InputPayload, output_dir: str = "run") -> FinalReport:
    """Entry used by orchestrator: build context, deps, GraphRunner."""
    ctx = RunContext.create(output_dir)
    cache_dir = ctx.output_base / "cache"
    ensure_dir(cache_dir)

    cvm = CVMConnector()
    ri = RIConnector()
    harvest = HarvestAdapter(pipeline_cache_db_path=str(cache_dir / "pipeline.sqlite"))
    search_orch = build_search_orchestrator()

    runner = GraphRunner(ctx, cvm, ri, harvest, search_orch)
    return runner.run(input_payload)
