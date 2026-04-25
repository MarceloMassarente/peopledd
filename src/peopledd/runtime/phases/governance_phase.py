from __future__ import annotations

from typing import Any

from peopledd.nodes import n0_entity_resolution, n1_governance_ingestion, n1b_reconciliation, n1c_semantic_fusion
from peopledd.pipeline_helpers import canonical_company_name
from peopledd.runtime.adaptive_models import AdaptiveDecisionRecord, PipelineSearchPlanState
from peopledd.runtime.pipeline_merge import effective_ri_url_for_pipeline
from peopledd.runtime.pipeline_state import PipelineState
from peopledd.models.contracts import InputPayload
from peopledd.services.sonar_governance_seed import fetch_governance_seed


def run(runner: Any, input_payload: InputPayload, state: PipelineState, search_plan: PipelineSearchPlanState) -> None:
    ctx = runner.ctx
    policy = runner.adaptive_policy

    ctx.log("start", "n0s", "sonar_governance_seed")
    seed = fetch_governance_seed(input_payload.company_name, country=input_payload.country)
    if seed is None:
        ctx.log("end", "n0s", "sonar_governance_seed_empty")
    else:
        ctx.log(
            "end",
            "n0s",
            "sonar_governance_seed_ok",
            ri_url=seed.ri_url_candidate or "",
            board=len(seed.board_members),
            executive=len(seed.executive_members),
            confidence=seed.confidence,
        )
    state.governance_seed = seed

    ctx.log("start", "n0", "entity_resolution")
    try:
        entity = n0_entity_resolution.run(input_payload, runner.cvm, runner.ri)
        runner._breaker_success("cvm")
    except Exception:
        runner._breaker_failure("cvm")
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
    effective_ri_url = effective_ri_url_for_pipeline(entity, seed)
    if (
        not (entity.ri_url and str(entity.ri_url).strip())
        and seed
        and (seed.ri_url_candidate or "").strip()
        and effective_ri_url
    ):
        ctx.log("hint", "n1", "ri_url_seed_applied", ri_url=effective_ri_url)

    ingestion = n1_governance_ingestion.run(
        company_name,
        cnpj=entity.cnpj,
        ri_url=effective_ri_url,
        fre_extended_probe=False,
        company_mode=entity.company_mode.value,
        search_orchestrator=runner.search_orch,
        website_hint=website_hint,
        country=input_payload.country,
        trace_ri_attempt=runner._log_ri_scrape_attempt,
    )
    ctx.log(
        "end",
        "n1",
        "governance_ingestion_first_pass",
        formal=ingestion.governance_data_quality.formal_completeness,
    )

    a1 = policy.build_n1_assessment(ingestion, bool(entity.cnpj), runner.search_orch is not None)
    act1, rationale1, rk1 = policy.decide_n1_fre_extended(
        a1, ingestion, bool(entity.cnpj), ctx, runner.breakers
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
            ri_url=effective_ri_url,
            fre_extended_probe=True,
            company_mode=entity.company_mode.value,
            search_orchestrator=runner.search_orch,
            website_hint=website_hint,
            country=input_payload.country,
            trace_ri_attempt=runner._log_ri_scrape_attempt,
        )
        if ingestion_retry.governance_data_quality.formal_completeness > ingestion.governance_data_quality.formal_completeness:
            ingestion = ingestion_retry
            runner._breaker_success("fre")
            ctx.log(
                "recovery",
                "n1",
                "fre_extended_improved",
                formal=ingestion.governance_data_quality.formal_completeness,
            )
        else:
            runner._breaker_failure("fre")
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
        governance_seed=seed,
        company_name=company_name,
        harvest=runner.harvest,
        search_orchestrator=runner.search_orch,
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
