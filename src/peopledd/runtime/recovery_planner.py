from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from peopledd.models.contracts import GovernanceIngestion
from peopledd.runtime.adaptive_models import AdaptiveActionKind, PhaseAssessment

_PEOPLE_GAP_KINDS = frozenset(
    {"people_low_evidence", "people_low_evidence_exec", "people_low_resolution", "people_ambiguous_matches"}
)

_STRATEGY_GAP_KINDS = frozenset({"strategy_empty", "strategy_thin"})

_RI_CURRENT_GAP_KINDS = frozenset(
    {"current_governance_weak", "ri_scrape_failed", "ri_low_content", "ri_anti_bot", "ri_timeout"}
)


@dataclass(frozen=True)
class RecoveryAction:
    """Single recoverable branch: preconditions must all hold (via `pre`)."""

    rationale: str
    kind: AdaptiveActionKind
    recovery_key: str | None
    cost: float
    expected_gain: float
    pre: Callable[..., bool]


# ── n1 FRE extended ──────────────────────────────────────────────────────────

def _n1_pre_no_cnpj(has_cnpj: bool, **_: Any) -> bool:
    return not has_cnpj


def _n1_pre_formal_ok(has_cnpj: bool, formal: float, **_: Any) -> bool:
    return has_cnpj and formal >= 0.5


def _n1_pre_degrade_recovery(has_cnpj: bool, formal: float, ctx: Any, **_: Any) -> bool:
    return has_cnpj and formal < 0.5 and not ctx.recovery_allowed("n1_fre_extended")


def _n1_pre_degrade_fre_circuit(has_cnpj: bool, formal: float, ctx: Any, breakers: dict, **_: Any) -> bool:
    return (
        has_cnpj
        and formal < 0.5
        and ctx.recovery_allowed("n1_fre_extended")
        and not breakers["fre"].allow()
    )


def _n1_pre_continue_no_formal_gap(
    has_cnpj: bool,
    formal: float,
    ctx: Any,
    breakers: dict,
    assessment: PhaseAssessment,
    **_: Any,
) -> bool:
    return (
        has_cnpj
        and formal < 0.5
        and ctx.recovery_allowed("n1_fre_extended")
        and breakers["fre"].allow()
        and not any(g.kind == "formal_governance_weak" for g in assessment.gaps)
    )


def _n1_pre_retry_fre(
    has_cnpj: bool,
    formal: float,
    ctx: Any,
    breakers: dict,
    assessment: PhaseAssessment,
    **_: Any,
) -> bool:
    return (
        has_cnpj
        and formal < 0.5
        and ctx.recovery_allowed("n1_fre_extended")
        and breakers["fre"].allow()
        and any(g.kind == "formal_governance_weak" for g in assessment.gaps)
    )


# ── n1 RI alternative (current governance weak) ──────────────────────────────

def _n1ri_pre_no_current_gap(assessment: PhaseAssessment, **_: Any) -> bool:
    return not any(g.kind in _RI_CURRENT_GAP_KINDS for g in assessment.gaps)


def _n1ri_pre_no_alternative(has_ri_alternative: bool, **_: Any) -> bool:
    return not has_ri_alternative


def _n1ri_pre_degrade_recovery(
    assessment: PhaseAssessment, ctx: Any, **_: Any
) -> bool:
    return (
        any(g.kind in _RI_CURRENT_GAP_KINDS for g in assessment.gaps)
        and not ctx.recovery_allowed("n1_ri_alternative")
    )


def _n1ri_pre_degrade_ri_circuit(
    assessment: PhaseAssessment, ctx: Any, breakers: dict, **_: Any
) -> bool:
    return (
        any(g.kind in _RI_CURRENT_GAP_KINDS for g in assessment.gaps)
        and ctx.recovery_allowed("n1_ri_alternative")
        and not breakers["ri"].allow()
    )


def _n1ri_pre_retry(
    assessment: PhaseAssessment, ctx: Any, breakers: dict, has_ri_alternative: bool, **_: Any
) -> bool:
    return (
        any(g.kind in _RI_CURRENT_GAP_KINDS for g in assessment.gaps)
        and has_ri_alternative
        and ctx.recovery_allowed("n1_ri_alternative")
        and breakers["ri"].allow()
    )


# ── n2 person search escalation ──────────────────────────────────────────────

def _n2_pre_llm_budget(ctx: Any, **_: Any) -> bool:
    return ctx.llm_calls_used >= ctx.max_llm_calls


def _n2_pre_no_orch(search_orchestrator_configured: bool, **_: Any) -> bool:
    return not search_orchestrator_configured


def _n2_pre_already_applied(person_escalation_already_applied: bool, **_: Any) -> bool:
    return person_escalation_already_applied


def _n2_pre_no_people_gap(assessment: PhaseAssessment, **_: Any) -> bool:
    return not any(g.kind in _PEOPLE_GAP_KINDS for g in assessment.gaps)


def _n2_pre_degrade_recovery(assessment: PhaseAssessment, ctx: Any, **_: Any) -> bool:
    return (
        any(g.kind in _PEOPLE_GAP_KINDS for g in assessment.gaps)
        and not ctx.recovery_allowed("n2_person_search_escalate")
    )


def _n2_pre_degrade_harvest(
    assessment: PhaseAssessment,
    ctx: Any,
    breakers: dict,
    search_orchestrator_configured: bool = False,
    **_: Any,
) -> bool:
    # Only degrade on Harvest failure when Exa is also unavailable (#2)
    return (
        any(g.kind in _PEOPLE_GAP_KINDS for g in assessment.gaps)
        and ctx.recovery_allowed("n2_person_search_escalate")
        and not breakers["harvest"].allow()
        and not search_orchestrator_configured
    )


def _n2_pre_rerun(
    search_orchestrator_configured: bool,
    person_escalation_already_applied: bool,
    assessment: PhaseAssessment,
    ctx: Any,
    **_: Any,
) -> bool:
    # Removed breakers["harvest"].allow() guard: escalation targets Exa, not Harvest (#2)
    return (
        search_orchestrator_configured
        and not person_escalation_already_applied
        and any(g.kind in _PEOPLE_GAP_KINDS for g in assessment.gaps)
        and ctx.recovery_allowed("n2_person_search_escalate")
    )


# ── n4 strategy ──────────────────────────────────────────────────────────────

def _n4_pre_llm_budget(ctx: Any, **_: Any) -> bool:
    return ctx.llm_calls_used >= ctx.max_llm_calls


def _n4_pre_strategy_not_empty(assessment: PhaseAssessment, **_: Any) -> bool:
    return not any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)


def _n4_pre_degrade_widen_recovery(assessment: PhaseAssessment, ctx: Any, **_: Any) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and not ctx.recovery_allowed("n4_widen_pages")
    )


def _n4_pre_degrade_widen_llm(assessment: PhaseAssessment, ctx: Any, breakers: dict, **_: Any) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and ctx.recovery_allowed("n4_widen_pages")
        and not breakers["strategy_llm"].allow()
    )


def _n4_pre_retry_widen(assessment: PhaseAssessment, ctx: Any, breakers: dict, **_: Any) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and ctx.recovery_allowed("n4_widen_pages")
        and breakers["strategy_llm"].allow()
    )


def _n4s_pre_widen_not_yet(assessment: PhaseAssessment, widen_pages_was_attempted: bool, **_: Any) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and not widen_pages_was_attempted
    )


def _n4s_pre_degrade_search_recovery(
    assessment: PhaseAssessment,
    widen_pages_was_attempted: bool,
    ctx: Any,
    **_: Any,
) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and widen_pages_was_attempted
        and not ctx.recovery_allowed("n4_widen_search")
    )


def _n4s_pre_degrade_search_llm(
    assessment: PhaseAssessment,
    widen_pages_was_attempted: bool,
    ctx: Any,
    breakers: dict,
    **_: Any,
) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and widen_pages_was_attempted
        and ctx.recovery_allowed("n4_widen_search")
        and not breakers["strategy_llm"].allow()
    )


def _n4s_pre_retry_search(
    assessment: PhaseAssessment,
    widen_pages_was_attempted: bool,
    ctx: Any,
    breakers: dict,
    **_: Any,
) -> bool:
    return (
        any(g.kind in _STRATEGY_GAP_KINDS for g in assessment.gaps)
        and widen_pages_was_attempted
        and ctx.recovery_allowed("n4_widen_search")
        and breakers["strategy_llm"].allow()
    )


def default_recovery_catalog() -> dict[str, list[RecoveryAction]]:
    """Ordered implicit priority via descending (expected_gain - cost) tie-breaker on expected_gain."""
    return {
        "n1_fre_extended": [
            RecoveryAction("no_cnpj_skip_fre_extended", "continue", None, 0.0, 100.0, _n1_pre_no_cnpj),
            RecoveryAction("formal_completeness_ok", "continue", None, 0.0, 99.0, _n1_pre_formal_ok),
            RecoveryAction(
                "recovery_budget_blocks_n1_fre_extended",
                "degrade_and_continue",
                None,
                0.0,
                98.0,
                _n1_pre_degrade_recovery,
            ),
            RecoveryAction(
                "fre_circuit_open", "degrade_and_continue", None, 0.0, 97.0, _n1_pre_degrade_fre_circuit
            ),
            RecoveryAction(
                "assessment_has_no_formal_weak_gap",
                "continue",
                None,
                0.0,
                96.0,
                _n1_pre_continue_no_formal_gap,
            ),
            RecoveryAction(
                "formal_below_0_5_with_cnpj",
                "retry_n1_fre_extended",
                "n1_fre_extended",
                0.1,
                95.0,
                _n1_pre_retry_fre,
            ),
        ],
        "n1_ri_alternative": [
            RecoveryAction("no_current_ri_gap", "continue", None, 0.0, 100.0, _n1ri_pre_no_current_gap),
            RecoveryAction("no_ri_alternative_url", "degrade_and_continue", None, 0.0, 99.0, _n1ri_pre_no_alternative),
            RecoveryAction(
                "recovery_budget_blocks_n1_ri_alternative",
                "degrade_and_continue",
                None,
                0.0,
                98.0,
                _n1ri_pre_degrade_recovery,
            ),
            RecoveryAction(
                "ri_circuit_open", "degrade_and_continue", None, 0.0, 97.0, _n1ri_pre_degrade_ri_circuit
            ),
            RecoveryAction(
                "current_weak_with_alternative_url",
                "retry_n1_ri_alternative",
                "n1_ri_alternative",
                0.1,
                96.0,
                _n1ri_pre_retry,
            ),
        ],
        "n2_person_search_escalation": [
            RecoveryAction(
                "llm_call_budget_exhausted_skip_n2_escalation",
                "degrade_and_continue",
                None,
                0.0,
                100.0,
                _n2_pre_llm_budget,
            ),
            RecoveryAction(
                "search_orchestrator_not_configured", "continue", None, 0.0, 99.0, _n2_pre_no_orch
            ),
            RecoveryAction(
                "person_search_escalation_already_applied",
                "continue",
                None,
                0.0,
                98.0,
                _n2_pre_already_applied,
            ),
            RecoveryAction("no_people_quality_gap", "continue", None, 0.0, 97.0, _n2_pre_no_people_gap),
            RecoveryAction(
                "recovery_budget_blocks_n2_escalation",
                "degrade_and_continue",
                None,
                0.0,
                96.0,
                _n2_pre_degrade_recovery,
            ),
            RecoveryAction(
                "harvest_circuit_open_no_exa_fallback",
                "degrade_and_continue",
                None,
                0.0,
                95.0,
                _n2_pre_degrade_harvest,
            ),
            RecoveryAction(
                "people_gap_with_search_available",
                "rerun_n2n3_person_search_escalation",
                "n2_person_search_escalate",
                0.2,
                94.0,
                _n2_pre_rerun,
            ),
        ],
        "n4_widen_pages": [
            RecoveryAction(
                "llm_call_budget_exhausted_skip_n4_widen",
                "degrade_and_continue",
                None,
                0.0,
                100.0,
                _n4_pre_llm_budget,
            ),
            RecoveryAction("strategy_not_empty", "continue", None, 0.0, 99.0, _n4_pre_strategy_not_empty),
            RecoveryAction(
                "recovery_budget_blocks_n4_widen_pages",
                "degrade_and_continue",
                None,
                0.0,
                98.0,
                _n4_pre_degrade_widen_recovery,
            ),
            RecoveryAction(
                "strategy_llm_circuit_open",
                "degrade_and_continue",
                None,
                0.0,
                97.0,
                _n4_pre_degrade_widen_llm,
            ),
            RecoveryAction(
                "strategy_empty_or_thin_widen_crawl",
                "retry_n4_widen_pages",
                "n4_widen_pages",
                0.15,
                96.0,
                _n4_pre_retry_widen,
            ),
        ],
        "n4_search_escalation": [
            RecoveryAction(
                "llm_call_budget_exhausted_skip_n4_search_escalation",
                "degrade_and_continue",
                None,
                0.0,
                100.0,
                _n4_pre_llm_budget,
            ),
            RecoveryAction("strategy_not_empty", "continue", None, 0.0, 99.0, _n4_pre_strategy_not_empty),
            RecoveryAction(
                "widen_pages_not_attempted_yet", "continue", None, 0.0, 98.0, _n4s_pre_widen_not_yet
            ),
            RecoveryAction(
                "recovery_budget_blocks_n4_widen_search",
                "degrade_and_continue",
                None,
                0.0,
                97.0,
                _n4s_pre_degrade_search_recovery,
            ),
            RecoveryAction(
                "strategy_llm_circuit_open",
                "degrade_and_continue",
                None,
                0.0,
                96.0,
                _n4s_pre_degrade_search_llm,
            ),
            RecoveryAction(
                "strategy_still_empty_or_thin_after_widen_pages",
                "retry_n4_search_escalation",
                "n4_widen_search",
                0.15,
                95.0,
                _n4s_pre_retry_search,
            ),
        ],
    }


class RecoveryPlanner:
    """Picks the best eligible recovery branch for a checkpoint."""

    def __init__(self, catalog: Mapping[str, Sequence[RecoveryAction]] | None = None) -> None:
        raw = default_recovery_catalog() if catalog is None else catalog
        self._catalog: dict[str, list[RecoveryAction]] = {k: list(v) for k, v in raw.items()}

    def _pick(self, phase: str, **kw: Any) -> tuple[AdaptiveActionKind, str, str | None]:
        actions = self._catalog.get(phase, [])
        eligible = [a for a in actions if a.pre(**kw)]
        if not eligible:
            return ("degrade_and_continue", f"recovery_planner_no_eligible_action:{phase}", None)
        best = max(eligible, key=lambda a: (a.expected_gain - a.cost, a.expected_gain))
        return (best.kind, best.rationale, best.recovery_key)

    def decide_n1_fre_extended(
        self,
        assessment: PhaseAssessment,
        ingestion: GovernanceIngestion,
        has_cnpj: bool,
        ctx: Any,
        breakers: dict[str, Any],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        formal = ingestion.governance_data_quality.formal_completeness
        return self._pick(
            "n1_fre_extended",
            assessment=assessment,
            ingestion=ingestion,
            has_cnpj=has_cnpj,
            ctx=ctx,
            breakers=breakers,
            formal=formal,
        )

    def decide_n1_ri_alternative(
        self,
        assessment: PhaseAssessment,
        has_ri_alternative: bool,
        ctx: Any,
        breakers: dict[str, Any],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._pick(
            "n1_ri_alternative",
            assessment=assessment,
            has_ri_alternative=has_ri_alternative,
            ctx=ctx,
            breakers=breakers,
        )

    def decide_n2_person_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: Any,
        breakers: dict[str, Any],
        search_orchestrator_configured: bool,
        person_escalation_already_applied: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._pick(
            "n2_person_search_escalation",
            assessment=assessment,
            ctx=ctx,
            breakers=breakers,
            search_orchestrator_configured=search_orchestrator_configured,
            person_escalation_already_applied=person_escalation_already_applied,
        )

    def decide_n4_widen_pages(
        self,
        assessment: PhaseAssessment,
        ctx: Any,
        breakers: dict[str, Any],
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._pick("n4_widen_pages", assessment=assessment, ctx=ctx, breakers=breakers)

    def decide_n4_search_escalation(
        self,
        assessment: PhaseAssessment,
        ctx: Any,
        breakers: dict[str, Any],
        widen_pages_was_attempted: bool,
    ) -> tuple[AdaptiveActionKind, str, str | None]:
        return self._pick(
            "n4_search_escalation",
            assessment=assessment,
            ctx=ctx,
            breakers=breakers,
            widen_pages_was_attempted=widen_pages_was_attempted,
        )
