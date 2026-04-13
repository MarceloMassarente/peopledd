from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from peopledd.models.common import ResolutionStatus, ServiceLevel
from peopledd.models.contracts import (
    ConfidencePolicy,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    InputPayload,
    KeyChallenge,
    MarketPulse,
    PersonProfile,
    PersonResolution,
    PipelineTelemetry,
    ProfileQuality,
    StrategicPriority,
    StrategyChallenges,
)
from peopledd.nodes import (
    n0_entity_resolution,
    n1_governance_ingestion,
    n1b_reconciliation,
    n1c_semantic_fusion,
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
from peopledd.runtime.artifact_policy import DD_BRIEF_FILENAME, artifact_include, validate_output_mode
from peopledd.runtime.circuit_breaker import SourceCircuitBreaker, default_breaker_set
from peopledd.runtime.context import RunContext
from peopledd.runtime.pipeline_context import attach_run_context, detach_run_context
from peopledd.runtime.pipeline_state import (
    PipelineState,
    checkpoint_input_fingerprint,
    read_checkpoint,
    remove_checkpoint,
    write_checkpoint,
)
from peopledd.runtime.run_metadata import (
    build_dd_brief,
    build_error_run_summary,
    build_run_summary,
)
from peopledd.runtime.staleness import compute_staleness_and_sl_dimensions
from peopledd.services.connectors import CVMConnector, RIConnector
from peopledd.services.harvest_adapter import HarvestAdapter
from peopledd.services.market_pulse_retriever import run_sync as run_market_pulse
from peopledd.utils.io import ensure_dir, validate_output_base_dir, write_json, write_text

_RESOLUTION_RANK: dict[ResolutionStatus, int] = {
    ResolutionStatus.RESOLVED: 4,
    ResolutionStatus.PARTIAL: 3,
    ResolutionStatus.AMBIGUOUS: 2,
    ResolutionStatus.NOT_FOUND: 1,
}


def _resolution_rank(pr: PersonResolution) -> int:
    return _RESOLUTION_RANK.get(pr.resolution_status, 0)


def _strategy_is_empty(raw_sc: Any) -> bool:
    return not raw_sc.strategic_priorities and not raw_sc.key_challenges


def _priority_key(p: StrategicPriority) -> tuple[str, str]:
    return (p.priority.strip().lower(), p.time_horizon)


def _challenge_key(c: KeyChallenge) -> tuple[str, str]:
    return (c.challenge.strip().lower(), c.challenge_type)


def _merge_strategy_challenges(base: StrategyChallenges, retry: StrategyChallenges) -> StrategyChallenges:
    """Union of priorities, challenges, and sonar briefs (retry wins same role)."""
    seen_p = {_priority_key(p) for p in base.strategic_priorities}
    merged_p = list(base.strategic_priorities)
    for p in retry.strategic_priorities:
        k = _priority_key(p)
        if k not in seen_p:
            seen_p.add(k)
            merged_p.append(p)

    seen_c = {_challenge_key(c) for c in base.key_challenges}
    merged_c = list(base.key_challenges)
    for c in retry.key_challenges:
        k = _challenge_key(c)
        if k not in seen_c:
            seen_c.add(k)
            merged_c.append(c)

    sonar_by_role: dict[str, Any] = {}
    for b in base.external_sonar_briefs:
        sonar_by_role[b.role] = b
    for b in retry.external_sonar_briefs:
        sonar_by_role[b.role] = b
    merged_sonar = list(sonar_by_role.values())

    merged_triggers = list(dict.fromkeys([*base.recent_triggers, *retry.recent_triggers]))
    merged_phase = {**base.company_phase_hypothesis, **retry.company_phase_hypothesis}
    return retry.model_copy(
        update={
            "strategic_priorities": merged_p,
            "key_challenges": merged_c,
            "external_sonar_briefs": merged_sonar,
            "recent_triggers": merged_triggers,
            "company_phase_hypothesis": merged_phase,
        }
    )


def _merge_people_resolution(base: list[PersonResolution], retry: list[PersonResolution]) -> list[PersonResolution]:
    base_map = {pr.observed_name: pr for pr in base}
    for pr in retry:
        existing = base_map.get(pr.observed_name)
        if existing is None or _resolution_rank(pr) > _resolution_rank(existing):
            base_map[pr.observed_name] = pr
    return list(base_map.values())


def _merge_people_phase_outputs(
    base_res: list[PersonResolution],
    retry_res: list[PersonResolution],
    base_prof: list[PersonProfile],
    retry_prof: list[PersonProfile],
) -> tuple[list[PersonResolution], list[PersonProfile]]:
    merged_res = _merge_people_resolution(base_res, retry_res)
    merged_prof: dict[str, PersonProfile] = {}
    for p in base_prof:
        merged_prof[p.person_id] = p
    for p in retry_prof:
        existing = merged_prof.get(p.person_id)
        if existing is None or p.profile_quality.useful_coverage_score > existing.profile_quality.useful_coverage_score:
            merged_prof[p.person_id] = p
    ordered: list[PersonProfile] = []
    for r in merged_res:
        if r.person_id in merged_prof:
            ordered.append(merged_prof[r.person_id])
        else:
            ordered.append(
                PersonProfile(
                    person_id=r.person_id,
                    career_summary={},
                    profile_quality=ProfileQuality(),
                    blind_spots=["profile_not_found"],
                )
            )
    return merged_res, ordered


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

    def _write_emergency_trace(self, input_payload: InputPayload, exc: Exception) -> None:
        ctx = self.ctx
        base = ctx.output_base
        try:
            ensure_dir(base)
            write_json(base / "run_trace.json", ctx.trace_to_json())
            write_json(
                base / "run_log.json",
                {
                    "run_id": ctx.run_id,
                    "status": "error",
                    "recovery_counts": dict(ctx.recovery_counts),
                    "llm_calls_used": ctx.llm_calls_used,
                    "output_mode": input_payload.output_mode,
                },
            )
            write_json(
                base / "run_summary.json",
                build_error_run_summary(
                    ctx.run_id,
                    base,
                    output_mode=input_payload.output_mode,
                    llm_calls_used=ctx.llm_calls_used,
                    recovery_counts=dict(ctx.recovery_counts),
                    exc=exc,
                    trace_events=ctx.trace_to_json(),
                ),
            )
        except OSError:
            pass

    def _write_error_run_summary_artifact_write(
        self, base: Path, mode: str, exc: Exception
    ) -> None:
        ctx = self.ctx
        try:
            ensure_dir(base)
            write_json(
                base / "run_summary.json",
                build_error_run_summary(
                    ctx.run_id,
                    base,
                    output_mode=mode,
                    llm_calls_used=ctx.llm_calls_used,
                    recovery_counts=dict(ctx.recovery_counts),
                    exc=exc,
                    trace_events=ctx.trace_to_json(),
                ),
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
            except Exception as e:
                self._write_emergency_trace(input_payload, e)
                raise
        finally:
            detach_run_context(token)

    def _run_governance_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: PipelineSearchPlanState,
    ) -> None:
        ctx = self.ctx
        policy = self.adaptive_policy

        ctx.log("start", "n0", "entity_resolution")
        try:
            entity = n0_entity_resolution.run(input_payload, self.cvm, self.ri)
            self._breaker_success("cvm")
        except Exception:
            self._breaker_failure("cvm")
            ctx.log("end", "n0", "entity_resolution_failed")
            raise
        ctx.log("end", "n0", "entity_resolution_ok", status=entity.resolution_status.value)
        state.entity = entity

        company_name = canonical_company_name(entity, input_payload) or input_payload.company_name
        state.company_name = company_name

        ctx.log("start", "n1", "governance_ingestion")
        website_hint = None
        if entity.exa_company_enrichment:
            website_hint = entity.exa_company_enrichment.get("website")

        ingestion = n1_governance_ingestion.run(
            company_name,
            cnpj=entity.cnpj,
            ri_url=entity.ri_url,
            fre_extended_probe=False,
            company_mode=entity.company_mode.value,
            search_orchestrator=self.search_orch,
            website_hint=website_hint,
            country=input_payload.country,
        )
        ctx.log(
            "end",
            "n1",
            "governance_ingestion_first_pass",
            formal=ingestion.governance_data_quality.formal_completeness,
        )

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
                company_mode=entity.company_mode.value,
                search_orchestrator=self.search_orch,
                website_hint=website_hint,
                country=input_payload.country,
            )
            if ingestion_retry.governance_data_quality.formal_completeness > ingestion.governance_data_quality.formal_completeness:
                ingestion = ingestion_retry
                self._breaker_success("fre")
                ctx.log(
                    "recovery",
                    "n1",
                    "fre_extended_improved",
                    formal=ingestion.governance_data_quality.formal_completeness,
                )
            else:
                self._breaker_failure("fre")
                ctx.log("recovery", "n1", "fre_extended_no_gain")

        state.ingestion = ingestion

        ctx.log("start", "n1b", "reconciliation")
        reconciliation = n1b_reconciliation.run(ingestion)
        ctx.log("end", "n1b", "reconciliation_ok", conflicts=len(reconciliation.conflict_items))
        state.reconciliation = reconciliation

        effective_prefer_llm = input_payload.prefer_llm
        if effective_prefer_llm and ctx.llm_calls_used >= ctx.max_llm_calls:
            effective_prefer_llm = False
            ctx.llm_budget_skips.append("n1c_semantic_fusion:prefer_llm_budget_exhausted")
            ctx.log("gap", "n1c", "llm_budget_skip_prefer_llm")

        ctx.log("start", "n1c", "semantic_fusion")
        semantic_fusion = n1c_semantic_fusion.run(
            ingestion,
            reconciliation,
            company_name=company_name,
            harvest=self.harvest,
            search_orchestrator=self.search_orch,
            use_harvest=input_payload.use_harvest,
            prefer_llm=effective_prefer_llm,
        )
        ctx.log(
            "end",
            "n1c",
            "semantic_fusion_ok",
            observations=len(semantic_fusion.observations),
            decisions=len(semantic_fusion.fusion_decisions),
        )
        state.semantic_fusion = semantic_fusion

    def _run_people_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: PipelineSearchPlanState,
    ) -> None:
        ctx = self.ctx
        policy = self.adaptive_policy
        reconciliation = state.reconciliation
        assert reconciliation is not None
        company_name = state.company_name

        if self.search_orch is None:
            ctx.log(
                "gap",
                "n2",
                "search_orchestrator_not_configured",
                reason="EXA_API_KEY or SEARXNG_URL missing",
            )

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
            retry_res = n2_person_resolution.run(
                reconciliation,
                self.harvest,
                company_name=company_name,
                search_orchestrator=self.search_orch,
                use_harvest=input_payload.use_harvest,
                person_search_params=search_plan.person_params,
            )
            ctx.log("end", "n2", "person_resolution_recovery_ok", count=len(retry_res))
            ctx.log("start", "n3", "profile_enrichment_recovery")
            retry_prof = n3_profile_enrichment.run(
                retry_res,
                self.harvest,
                use_harvest=input_payload.use_harvest,
            )
            ctx.log("end", "n3", "profile_enrichment_recovery_ok", count=len(retry_prof))
            people_resolution, people_profiles = _merge_people_phase_outputs(
                people_resolution, retry_res, people_profiles, retry_prof
            )

        state.people_resolution = people_resolution
        state.people_profiles = people_profiles
        state.people_phase_completed = True

    def _run_strategy_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        search_plan: PipelineSearchPlanState,
    ) -> None:
        ctx = self.ctx
        policy = self.adaptive_policy
        entity = state.entity
        assert entity is not None
        company_name = state.company_name
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
            strategy = _merge_strategy_challenges(strategy, strategy_retry)
            if not _strategy_is_empty(strategy_retry):
                self._breaker_success("strategy_llm")
                ctx.log("recovery", "n4", "strategy_retry_populated")
            else:
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
                strategy = _merge_strategy_challenges(strategy, strategy_retry_b)
                if not _strategy_is_empty(strategy_retry_b):
                    self._breaker_success("strategy_llm")
                    ctx.log("recovery", "n4", "strategy_search_escalation_populated")
                else:
                    self._breaker_failure("strategy_llm")
                    ctx.log("recovery", "n4", "strategy_search_escalation_still_empty")

        state.strategy = strategy

        if self.search_orch is not None:
            ctx.log("start", "market_pulse", "retrieve")
            market_pulse = run_market_pulse(
                self.search_orch,
                company_name=company_name,
                entity=entity,
                strategy=strategy,
                analysis_depth=input_payload.analysis_depth,
                country=input_payload.country,
            )
            ctx.log(
                "end",
                "market_pulse",
                "done",
                claims=len(market_pulse.claims),
                skipped=market_pulse.skipped_reason or "",
            )
        else:
            market_pulse = MarketPulse(skipped_reason="no_search_orchestrator")
            ctx.log("skip", "market_pulse", "no_search_orchestrator")
        state.market_pulse = market_pulse

    def _run_scoring_phase(
        self,
        input_payload: InputPayload,
        state: PipelineState,
        base: Path,
    ) -> FinalReport:
        ctx = self.ctx
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

        try:
            if artifact_include("input", mode):
                write_json(base / "input.json", input_payload.model_dump(mode="json"))
            if artifact_include("entity_resolution", mode):
                write_json(base / "entity_resolution.json", entity.model_dump(mode="json"))
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
        except OSError as exc:
            self._write_error_run_summary_artifact_write(base, mode, exc)
            raise
        remove_checkpoint(base)
        return final_report

    def _run_pipeline(self, input_payload: InputPayload, base: Path) -> FinalReport:
        ctx = self.ctx
        mode = input_payload.output_mode
        validate_output_mode(mode)

        ctx.log("start", "pipeline", "run_begin", run_id=ctx.run_id)

        search_plan = PipelineSearchPlanState()
        expected_fp = checkpoint_input_fingerprint(input_payload)
        resume = read_checkpoint(base)
        resume_ok = False
        state = PipelineState()
        if resume is not None:
            rid, _phase, loaded_state, loaded_plan, stored_fp = resume
            if rid != ctx.run_id:
                pass
            elif stored_fp is None:
                ctx.log("gap", "pipeline", "checkpoint_missing_fingerprint", run_id=rid)
            elif stored_fp != expected_fp:
                ctx.log("gap", "pipeline", "checkpoint_fingerprint_mismatch", run_id=rid)
                remove_checkpoint(base)
            else:
                resume_ok = True
                state = loaded_state
                search_plan = loaded_plan
                ctx.log("start", "pipeline", "resume_from_checkpoint", phase="post_people")

        if not resume_ok:
            self._run_governance_phase(input_payload, state, search_plan)
            self._run_people_phase(input_payload, state, search_plan)
            write_checkpoint(
                base,
                ctx.run_id,
                "post_people",
                state,
                search_plan,
                input_fingerprint=expected_fp,
            )

        self._run_strategy_phase(input_payload, state, search_plan)
        return self._run_scoring_phase(input_payload, state, base)

    @staticmethod
    def run_batch(
        payloads: list[InputPayload],
        output_dir: str,
        *,
        concurrency: int = 3,
        resume_on_failure: bool = True,
    ) -> list[FinalReport | Exception]:
        """
        Run multiple InputPayloads under output_dir (each gets its own run subfolder).

        resume_on_failure does not auto-retry failed items; when True, operators may re-run a failed
        company with the same InputPayload.run_id so post-people checkpoint.json applies (fingerprint
        must still match the payload).
        """
        validate_output_base_dir(output_dir)
        seen_ids: dict[str, int] = {}
        for i, p in enumerate(payloads):
            if p.run_id is None:
                continue
            if p.run_id in seen_ids:
                raise ValueError(
                    f"duplicate run_id {p.run_id!r} at indices {seen_ids[p.run_id]} and {i}"
                )
            seen_ids[p.run_id] = i

        out_root = Path(output_dir)
        ensure_dir(out_root)
        results: list[FinalReport | Exception | None] = [None] * len(payloads)

        def _one(idx: int, payload: InputPayload) -> tuple[int, FinalReport | Exception]:
            try:
                report = run_pipeline_graph(payload, output_dir=output_dir)
                return idx, report
            except Exception as e:
                return idx, e

        workers = max(1, min(concurrency, len(payloads)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_one, i, p): i for i, p in enumerate(payloads)}
            for fut in as_completed(futs):
                idx, item = fut.result()
                results[idx] = item

        batch_rows: list[dict[str, Any]] = []
        for i, p in enumerate(payloads):
            item = results[i]
            row: dict[str, Any] = {
                "index": i,
                "company_name": p.company_name,
                "run_id": p.run_id,
            }
            if isinstance(item, FinalReport):
                tel = item.pipeline_telemetry
                row["status"] = "ok"
                row["run_id_resolved"] = tel.run_id if tel else None
                row["service_level"] = item.degradation_profile.service_level.value
            elif isinstance(item, Exception):
                row["status"] = "error"
                row["error"] = f"{type(item).__name__}: {item}"
            else:
                row["status"] = "unknown"
            batch_rows.append(row)

        summary_path = out_root / "batch_summary.json"
        try:
            summary_path.write_text(
                json.dumps(
                    {
                        "runs": batch_rows,
                        "resume_on_failure": resume_on_failure,
                        "note": (
                            "resume_on_failure is manual: re-run the same run_id with matching "
                            "payload fingerprint to use checkpoint.json after a partial failure."
                        ),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        except OSError:
            pass

        return [r if r is not None else RuntimeError("missing batch slot") for r in results]


def run_pipeline_graph(input_payload: InputPayload, output_dir: str = "run") -> FinalReport:
    """Entry used by orchestrator: build context, deps, GraphRunner."""
    validate_output_base_dir(output_dir)
    ctx = RunContext.create(output_dir, run_id=input_payload.run_id)
    cache_dir = ctx.output_base / "cache"
    ensure_dir(cache_dir)

    cvm = CVMConnector()
    ri = RIConnector()
    harvest = HarvestAdapter(pipeline_cache_db_path=str(cache_dir / "pipeline.sqlite"))
    search_orch = build_search_orchestrator()

    runner = GraphRunner(ctx, cvm, ri, harvest, search_orch)
    return runner.run(input_payload)
