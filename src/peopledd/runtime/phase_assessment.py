from __future__ import annotations

from typing import Any

from peopledd.models.contracts import GovernanceIngestion, PersonProfile, StrategyChallenges
from peopledd.runtime.adaptive_models import AssessmentGap, PhaseAssessment


def assess_after_n1_ingestion(
    ingestion: GovernanceIngestion,
    has_cnpj: bool,
    search_orchestrator_configured: bool,
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
    if current < 0.4:
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
        },
        gaps=gaps,
    )


def assess_after_n2_n3_with_board_context(
    people_profiles: list[PersonProfile],
    people_resolution: list[Any],
    board_names: set[str],
) -> PhaseAssessment:
    """Uses observed_name alignment to board for evidence/coverage signals."""
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

    board_profiles: list[PersonProfile] = []
    for pr, pp in zip(people_resolution, people_profiles):
        if pr.observed_name in board_names:
            board_profiles.append(pp)

    if not board_profiles and board_names:
        gaps.append(AssessmentGap(kind="people_low_resolution", detail="no board profiles matched"))
        return PhaseAssessment(
            checkpoint="n2n3_post_profiles",
            metrics={
                "profile_count": len(people_profiles),
                "board_profile_count": 0,
                "board_size": len(board_names),
            },
            gaps=gaps,
        )

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

    return PhaseAssessment(
        checkpoint="n2n3_post_profiles",
        metrics={
            "profile_count": len(people_profiles),
            "board_profile_count": len(board_profiles),
            "board_size": len(board_names),
            "board_avg_evidence_density": round(avg_evidence, 4),
            "board_avg_useful_coverage": round(avg_useful, 4),
            "ambiguous_resolution_count": ambiguous,
        },
        gaps=gaps,
    )


def assess_after_n4_strategy(strategy: StrategyChallenges) -> PhaseAssessment:
    empty = not strategy.strategic_priorities and not strategy.key_challenges
    gaps: list[AssessmentGap] = []
    if empty:
        gaps.append(AssessmentGap(kind="strategy_empty", detail="no priorities or challenges"))
    return PhaseAssessment(
        checkpoint="n4_post_strategy",
        metrics={
            "priorities": len(strategy.strategic_priorities),
            "challenges": len(strategy.key_challenges),
        },
        gaps=gaps,
    )
