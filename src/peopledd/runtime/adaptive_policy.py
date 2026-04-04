from __future__ import annotations

from typing import TYPE_CHECKING

from peopledd.models.contracts import GovernanceIngestion
from peopledd.runtime.adaptive_models import AdaptiveActionKind, PhaseAssessment
from peopledd.runtime.phase_assessment import (
    assess_after_n1_ingestion,
    assess_after_n2_n3_with_board_context,
    assess_after_n4_strategy,
)

if TYPE_CHECKING:
    from peopledd.models.contracts import PersonProfile, StrategyChallenges
    from peopledd.runtime.circuit_breaker import SourceCircuitBreaker
    from peopledd.runtime.context import RunContext


class DefaultAdaptivePolicy:
    """
    Rubric-driven policy: small action catalog, deterministic decisions.
    """

    def build_n1_assessment(
        self,
        ingestion: GovernanceIngestion,
        has_cnpj: bool,
        search_orchestrator_configured: bool,
    ) -> PhaseAssessment:
        return assess_after_n1_ingestion(
            ingestion,
            has_cnpj=has_cnpj,
            search_orchestrator_configured=search_orchestrator_configured,
        )

    def decide_n1_fre_extended(
        self,
        assessment: PhaseAssessment,
        ingestion: GovernanceIngestion,
        has_cnpj: bool,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        if not has_cnpj:
            return ("continue", "no_cnpj_skip_fre_extended", None)
        formal = ingestion.governance_data_quality.formal_completeness
        if formal >= 0.5:
            return ("continue", "formal_completeness_ok", None)
        if not ctx.recovery_allowed("n1_fre_extended"):
            return ("degrade_and_continue", "recovery_budget_blocks_n1_fre_extended", None)
        if not breakers["fre"].allow():
            return ("degrade_and_continue", "fre_circuit_open", None)
        if not any(g.kind == "formal_governance_weak" for g in assessment.gaps):
            return ("continue", "assessment_has_no_formal_weak_gap", None)
        return ("retry_n1_fre_extended", "formal_below_0_5_with_cnpj", "n1_fre_extended")

    def build_n2n3_assessment(
        self,
        people_profiles: list[PersonProfile],
        people_resolution: list[object],
        board_names: set[str],
    ) -> PhaseAssessment:
        return assess_after_n2_n3_with_board_context(
            people_profiles,
            people_resolution,
            board_names,
        )

    def decide_n2_person_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
        search_orchestrator_configured: bool,
        person_escalation_already_applied: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        if not search_orchestrator_configured:
            return ("continue", "search_orchestrator_not_configured", None)
        if person_escalation_already_applied:
            return ("continue", "person_search_escalation_already_applied", None)
        trigger_kinds = frozenset(
            {"people_low_evidence", "people_low_resolution", "people_ambiguous_matches"}
        )
        if not any(g.kind in trigger_kinds for g in assessment.gaps):
            return ("continue", "no_people_quality_gap", None)
        if not ctx.recovery_allowed("n2_person_search_escalate"):
            return ("degrade_and_continue", "recovery_budget_blocks_n2_escalation", None)
        if not breakers["harvest"].allow():
            return ("degrade_and_continue", "harvest_circuit_open", None)
        return (
            "rerun_n2n3_person_search_escalation",
            "people_gap_with_search_available",
            "n2_person_search_escalate",
        )

    def build_n4_assessment(self, strategy: StrategyChallenges) -> PhaseAssessment:
        return assess_after_n4_strategy(strategy)

    def decide_n4_widen_pages(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        if not any(g.kind == "strategy_empty" for g in assessment.gaps):
            return ("continue", "strategy_not_empty", None)
        if not ctx.recovery_allowed("n4_widen_pages"):
            return ("degrade_and_continue", "recovery_budget_blocks_n4_widen_pages", None)
        if not breakers["strategy_llm"].allow():
            return ("degrade_and_continue", "strategy_llm_circuit_open", None)
        return ("retry_n4_widen_pages", "strategy_empty_widen_crawl", "n4_widen_pages")

    def decide_n4_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
        widen_pages_was_attempted: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        if not any(g.kind == "strategy_empty" for g in assessment.gaps):
            return ("continue", "strategy_not_empty", None)
        if not widen_pages_was_attempted:
            return ("continue", "widen_pages_not_attempted_yet", None)
        if not ctx.recovery_allowed("n4_widen_search"):
            return ("degrade_and_continue", "recovery_budget_blocks_n4_widen_search", None)
        if not breakers["strategy_llm"].allow():
            return ("degrade_and_continue", "strategy_llm_circuit_open", None)
        return (
            "retry_n4_search_escalation",
            "strategy_still_empty_after_widen_pages",
            "n4_widen_search",
        )
