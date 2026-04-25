from __future__ import annotations

from typing import Any

from peopledd.nodes import n4_strategy_inference
from peopledd.pipeline_helpers import infer_sector_key
from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, PipelineSearchPlanState
from peopledd.runtime.pipeline_merge import (
    effective_ri_url_for_pipeline,
    merge_strategy_challenges,
    strategy_is_empty,
)
from peopledd.runtime.pipeline_state import PipelineState
from peopledd.models.contracts import InputPayload, MarketPulse
from peopledd.services.market_pulse_retriever import run_sync as run_market_pulse


def run(runner: Any, input_payload: InputPayload, state: PipelineState, search_plan: PipelineSearchPlanState) -> None:
    ctx = runner.ctx
    policy = runner.adaptive_policy
    entity = state.entity
    assert entity is not None
    company_name = state.company_name
    sector_key = infer_sector_key(entity, input_payload)

    strategy_attempt_idx = 0
    find_escalation_level = 0
    ri_for_strategy = effective_ri_url_for_pipeline(entity, state.governance_seed)

    def _run_n4(max_pages: int | None, find_fp: Any, attempt_idx: int, esc_level: int) -> Any:
        return n4_strategy_inference.run(
            company_name,
            ri_url=ri_for_strategy,
            sector=sector_key,
            country=input_payload.country,
            strategy_max_pages=max_pages,
            search_orchestrator=runner.search_orch,
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
    act4, rationale4, rk4 = policy.decide_n4_widen_pages(a4, ctx, runner.breakers)
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
        strategy = merge_strategy_challenges(strategy, strategy_retry)
        if not strategy_is_empty(strategy_retry):
            runner._breaker_success("strategy_llm")
            ctx.log("recovery", "n4", "strategy_retry_populated")
        else:
            runner._breaker_failure("strategy_llm")
            ctx.log("recovery", "n4", "strategy_retry_still_empty")

    if strategy_is_empty(strategy):
        a4b = policy.build_n4_assessment(strategy)
        act5, rationale5, rk5 = policy.decide_n4_search_escalation(
            a4b, ctx, runner.breakers, widen_attempted
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
            strategy = merge_strategy_challenges(strategy, strategy_retry_b)
            if not strategy_is_empty(strategy_retry_b):
                runner._breaker_success("strategy_llm")
                ctx.log("recovery", "n4", "strategy_search_escalation_populated")
            else:
                runner._breaker_failure("strategy_llm")
                ctx.log("recovery", "n4", "strategy_search_escalation_still_empty")

    state.strategy = strategy

    if runner.search_orch is not None:
        ctx.log("start", "market_pulse", "retrieve")
        market_pulse = run_market_pulse(
            runner.search_orch,
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
