from __future__ import annotations

from typing import TYPE_CHECKING

from peopledd.models.contracts import GovernanceIngestion
from peopledd.runtime.adaptive_models import AdaptiveActionKind, PhaseAssessment
from peopledd.runtime.phase_assessment import (
    assess_after_n1_ingestion,
    assess_after_n2_n3_with_board_context,
    assess_after_n4_strategy,
)
from peopledd.runtime.recovery_planner import RecoveryPlanner

if TYPE_CHECKING:
    from peopledd.models.contracts import PersonProfile, StrategyChallenges
    from peopledd.runtime.circuit_breaker import SourceCircuitBreaker
    from peopledd.runtime.context import RunContext


class DefaultAdaptivePolicy:
    """
    Rubric-driven policy: small action catalog, deterministic decisions.
    """

    def __init__(self, planner: RecoveryPlanner | None = None) -> None:
        self._planner = planner or RecoveryPlanner()

    def build_n1_assessment(
        self,
        ingestion: GovernanceIngestion,
        has_cnpj: bool,
        search_orchestrator_configured: bool,
        has_ri_alternative: bool = False,
    ) -> PhaseAssessment:
        return assess_after_n1_ingestion(
            ingestion,
            has_cnpj=has_cnpj,
            search_orchestrator_configured=search_orchestrator_configured,
            has_ri_alternative=has_ri_alternative,
        )

    def decide_n1_ri_alternative(
        self,
        assessment: PhaseAssessment,
        has_ri_alternative: bool,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._planner.decide_n1_ri_alternative(assessment, has_ri_alternative, ctx, breakers)

    def decide_n1_fre_extended(
        self,
        assessment: PhaseAssessment,
        ingestion: GovernanceIngestion,
        has_cnpj: bool,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._planner.decide_n1_fre_extended(assessment, ingestion, has_cnpj, ctx, breakers)

    def build_n2n3_assessment(
        self,
        people_profiles: list[PersonProfile],
        people_resolution: list[object],
        board_names: set[str],
        exec_names: set[str] | None = None,
    ) -> PhaseAssessment:
        return assess_after_n2_n3_with_board_context(
            people_profiles,
            people_resolution,
            board_names,
            exec_names=exec_names,
        )

    def decide_n2_person_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
        search_orchestrator_configured: bool,
        person_escalation_already_applied: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._planner.decide_n2_person_search_escalation(
            assessment,
            ctx,
            breakers,
            search_orchestrator_configured,
            person_escalation_already_applied,
        )

    def build_n4_assessment(self, strategy: StrategyChallenges) -> PhaseAssessment:
        return assess_after_n4_strategy(strategy)

    def decide_n4_widen_pages(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._planner.decide_n4_widen_pages(assessment, ctx, breakers)

    def decide_n4_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: RunContext,
        breakers: dict[str, SourceCircuitBreaker],
        widen_pages_was_attempted: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._planner.decide_n4_search_escalation(
            assessment, ctx, breakers, widen_pages_was_attempted
        )
