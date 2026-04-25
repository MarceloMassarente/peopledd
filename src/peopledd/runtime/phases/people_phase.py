from __future__ import annotations

from typing import Any

from peopledd.nodes import n2_person_resolution, n3_profile_enrichment
from peopledd.pipeline_helpers import reconciliation_with_fusion_snapshot
from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, PipelineSearchPlanState
from peopledd.runtime.pipeline_merge import merge_people_phase_outputs
from peopledd.runtime.pipeline_state import PipelineState
from peopledd.models.contracts import InputPayload


def run(runner: Any, input_payload: InputPayload, state: PipelineState, search_plan: PipelineSearchPlanState) -> None:
    ctx = runner.ctx
    policy = runner.adaptive_policy
    reconciliation = state.reconciliation
    semantic_fusion = state.semantic_fusion
    assert reconciliation is not None
    assert semantic_fusion is not None
    company_name = state.company_name
    effective_reconciliation = reconciliation_with_fusion_snapshot(reconciliation, semantic_fusion)
    state.reconciliation = effective_reconciliation

    if runner.search_orch is None:
        ctx.log(
            "gap",
            "n2",
            "search_orchestrator_not_configured",
            reason="EXA_API_KEY or SEARXNG_URL missing",
        )

    ctx.log("start", "n2", "person_resolution")
    people_resolution = n2_person_resolution.run(
        effective_reconciliation,
        runner.harvest,
        company_name=company_name,
        search_orchestrator=runner.search_orch,
        use_harvest=input_payload.use_harvest,
        person_search_params=search_plan.person_params,
    )
    ctx.log("end", "n2", "person_resolution_ok", count=len(people_resolution))

    ctx.log("start", "n3", "profile_enrichment")
    people_profiles = n3_profile_enrichment.run(
        people_resolution,
        runner.harvest,
        use_harvest=input_payload.use_harvest,
    )
    ctx.log("end", "n3", "profile_enrichment_ok", count=len(people_profiles))

    board_names = {m.person_name for m in effective_reconciliation.reconciled_governance_snapshot.board_members}
    a2 = policy.build_n2n3_assessment(people_profiles, people_resolution, board_names)
    act2, rationale2, rk2 = policy.decide_n2_person_search_escalation(
        a2,
        ctx,
        runner.breakers,
        runner.search_orch is not None,
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
            effective_reconciliation,
            runner.harvest,
            company_name=company_name,
            search_orchestrator=runner.search_orch,
            use_harvest=input_payload.use_harvest,
            person_search_params=search_plan.person_params,
        )
        ctx.log("end", "n2", "person_resolution_recovery_ok", count=len(retry_res))
        ctx.log("start", "n3", "profile_enrichment_recovery")
        retry_prof = n3_profile_enrichment.run(
            retry_res,
            runner.harvest,
            use_harvest=input_payload.use_harvest,
        )
        ctx.log("end", "n3", "profile_enrichment_recovery_ok", count=len(retry_prof))
        people_resolution, people_profiles = merge_people_phase_outputs(
            people_resolution, retry_res, people_profiles, retry_prof
        )

    state.people_resolution = people_resolution
    state.people_profiles = people_profiles
    state.people_phase_completed = True
