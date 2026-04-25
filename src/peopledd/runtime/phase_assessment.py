from __future__ import annotations

from typing import Any

from peopledd.models.contracts import GovernanceIngestion, PersonProfile, StrategyChallenges
from peopledd.runtime.adaptive_models import AssessmentGap, AssessmentGapKind, PhaseAssessment

_RI_CURRENT_GAP_KINDS = frozenset({
    "current_governance_weak", "ri_scrape_failed", "ri_low_content", "ri_anti_bot", "ri_timeout",
})


def _ri_failure_to_gap_kind(mode: str) -> AssessmentGapKind:
    if mode == "anti_bot":
        return "ri_anti_bot"
    if mode == "timeout":
        return "ri_timeout"
    if mode == "low_content":
        return "ri_low_content"
    if mode == "budget_exhausted":
        return "llm_budget_exhausted"
    return "ri_scrape_failed"


def assess_after_n1_ingestion(
    ingestion: GovernanceIngestion,
    has_cnpj: bool,
    search_orchestrator_configured: bool,
    has_ri_alternative: bool = False,
) -> PhaseAssessment:
    gaps: list[AssessmentGap] = []
    formal = ingestion.governance_data_quality.formal_completeness
    current = ingestion.governance_data_quality.current_completeness
    if formal < 0.5 and has_cnpj:
        gaps.append(
            AssessmentGap(
                kind="formal_governance_weak",
                detail=f"formal_completeness={formal:.2f}",
            )
        )
    meta = ingestion.ingestion_metadata
    ri_mode = (meta.get("ri_primary_failure_mode") or "").strip() or None
    if current < 0.4:
        if ri_mode:
            gk = _ri_failure_to_gap_kind(ri_mode)
            gaps.append(
                AssessmentGap(
                    kind=gk,
                    detail=f"current_completeness={current:.2f};ri_primary_failure_mode={ri_mode}",
                )
            )
        else:
            gaps.append(
                AssessmentGap(
                    kind="current_governance_weak",
                    detail=f"current_completeness={current:.2f}",
                )
            )
    if not search_orchestrator_configured:
        gaps.append(AssessmentGap(kind="search_orchestrator_missing", detail="no exa/searxng"))
    return PhaseAssessment(
        checkpoint="n1_post_ingestion",
        metrics={
            "formal_completeness": formal,
            "current_completeness": current,
            "has_cnpj": has_cnpj,
            "has_ri_alternative": has_ri_alternative,
        },
        gaps=gaps,
    )


def assess_after_n2_n3_with_board_context(
    people_profiles: list[PersonProfile],
    people_resolution: list[Any],
    board_names: set[str],
    exec_names: set[str] | None = None,
) -> PhaseAssessment:
    """Uses observed_name alignment to board/exec for evidence/coverage signals."""
    from peopledd.models.common import ResolutionStatus

    gaps: list[AssessmentGap] = []
    ambiguous = sum(
        1
        for pr in people_resolution
        if pr.resolution_status == ResolutionStatus.AMBIGUOUS
    )
    if ambiguous >= 2:
        gaps.append(
            AssessmentGap(
                kind="people_ambiguous_matches",
                detail=f"ambiguous_count={ambiguous}",
            )
        )

    resolution_by_name = {pr.observed_name: pr for pr in people_resolution}
    profile_by_name = {pr.observed_name: pp for pr, pp in zip(people_resolution, people_profiles)}

    board_profiles: list[PersonProfile] = [
        profile_by_name[n] for n in board_names if n in profile_by_name
    ]

    if not board_profiles and board_names:
        gaps.append(AssessmentGap(kind="people_low_resolution", detail="no board profiles matched"))
        metrics: dict[str, Any] = {
            "profile_count": len(people_profiles),
            "board_profile_count": 0,
            "board_size": len(board_names),
        }
        if exec_names is not None:
            metrics["exec_size"] = len(exec_names)
        return PhaseAssessment(checkpoint="n2n3_post_profiles", metrics=metrics, gaps=gaps)

    densities = [float(p.profile_quality.evidence_density) for p in board_profiles]
    avg_evidence = sum(densities) / max(1, len(densities))
    useful = [float(p.profile_quality.useful_coverage_score) for p in board_profiles]
    avg_useful = sum(useful) / max(1, len(useful))

    if avg_evidence < 0.22 and len(board_profiles) >= 1:
        gaps.append(
            AssessmentGap(
                kind="people_low_evidence",
                detail=f"board_avg_evidence_density={avg_evidence:.3f}",
            )
        )
    if avg_useful < 0.4 and len(board_names) >= 3:
        gaps.append(
            AssessmentGap(
                kind="people_low_resolution",
                detail=f"board_avg_useful_coverage={avg_useful:.3f}",
            )
        )

    metrics = {
        "profile_count": len(people_profiles),
        "board_profile_count": len(board_profiles),
        "board_size": len(board_names),
        "board_avg_evidence_density": round(avg_evidence, 4),
        "board_avg_useful_coverage": round(avg_useful, 4),
        "ambiguous_resolution_count": ambiguous,
    }

    # Executive evidence assessment (#6)
    if exec_names:
        exec_profiles: list[PersonProfile] = [
            profile_by_name[n] for n in exec_names if n in profile_by_name
        ]
        if exec_profiles:
            exec_densities = [float(p.profile_quality.evidence_density) for p in exec_profiles]
            avg_exec_evidence = sum(exec_densities) / len(exec_densities)
            metrics["exec_profile_count"] = len(exec_profiles)
            metrics["exec_avg_evidence_density"] = round(avg_exec_evidence, 4)
            metrics["exec_size"] = len(exec_names)
            if avg_exec_evidence < 0.22 and len(exec_names) >= 2:
                gaps.append(
                    AssessmentGap(
                        kind="people_low_evidence_exec",
                        detail=f"exec_avg_evidence_density={avg_exec_evidence:.3f}",
                    )
                )
        else:
            metrics["exec_profile_count"] = 0
            metrics["exec_size"] = len(exec_names)

    return PhaseAssessment(checkpoint="n2n3_post_profiles", metrics=metrics, gaps=gaps)


def assess_after_n4_strategy(strategy: StrategyChallenges) -> PhaseAssessment:
    n_priorities = len(strategy.strategic_priorities)
    n_challenges = len(strategy.key_challenges)
    empty = not n_priorities and not n_challenges
    gaps: list[AssessmentGap] = []
    if empty:
        gaps.append(AssessmentGap(kind="strategy_empty", detail="no priorities or challenges"))
    elif n_priorities + n_challenges <= 2 and not strategy.recent_triggers:
        # Thin output: escalation can still enrich it (#3)
        gaps.append(
            AssessmentGap(
                kind="strategy_thin",
                detail=f"priorities={n_priorities},challenges={n_challenges},no_recent_triggers",
            )
        )
    return PhaseAssessment(
        checkpoint="n4_post_strategy",
        metrics={
            "priorities": n_priorities,
            "challenges": n_challenges,
            "recent_triggers": len(strategy.recent_triggers),
        },
        gaps=gaps,
    )
