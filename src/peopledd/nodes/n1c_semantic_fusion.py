from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from peopledd.models.contracts import (
    GovernanceFusionDecision,
    GovernanceSeed,
    GovernanceIngestion,
    GovernanceReconciliation,
    ProfileEvidenceNote,
    SemanticGovernanceFusion,
)
from peopledd.services.fusion_profile_evidence import (
    gather_profile_evidence,
    profile_notes_to_observations,
)
from peopledd.services.governance_fusion_judge import (
    build_unresolved_items,
    cluster_observations,
    fuse_observations,
    fusion_quality_from_decisions,
    merge_profile_observations,
)
from peopledd.services.governance_observation_builder import build_governance_observations

if TYPE_CHECKING:
    from peopledd.services.harvest_adapter import HarvestAdapter
    from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)


def _needs_profile_evidence(decisions: list[GovernanceFusionDecision]) -> bool:
    return any(d.decision_status == "ambiguous" for d in decisions) or any(
        d.confidence < 0.52 and d.decision_status == "resolved" for d in decisions
    )


def run(
    ingestion: GovernanceIngestion,
    reconciliation: GovernanceReconciliation,
    governance_seed: GovernanceSeed | None = None,
    company_name: str | None = None,
    harvest: HarvestAdapter | None = None,
    search_orchestrator: SearchOrchestrator | None = None,
    *,
    use_harvest: bool = True,
    prefer_llm: bool = True,
) -> SemanticGovernanceFusion:
    """
    Multi-source semantic governance fusion (n1c). Runs after n1b; does not replace reconciliation
    for downstream n2 by default. Populates FinalReport.semantic_governance_fusion.
    """
    observations = build_governance_observations(ingestion, governance_seed=governance_seed)
    candidates = cluster_observations(observations)
    decisions, resolved_snapshot, quality, llm_used = fuse_observations(
        observations,
        candidates,
        reconciliation,
        prefer_llm=prefer_llm,
        profile_rounds=0,
    )
    judge_passes = quality.judge_passes
    final_observations = observations
    final_candidates = candidates
    notes: list[ProfileEvidenceNote] = []

    if _needs_profile_evidence(decisions):
        names: list[str] = []
        for d in decisions:
            if d.decision_status == "ambiguous" or (
                d.decision_status == "resolved" and d.confidence < 0.52
            ):
                if d.canonical_name.strip():
                    names.append(d.canonical_name.strip())
        notes = gather_profile_evidence(
            names,
            company_name,
            harvest,
            search_orchestrator,
            use_harvest=use_harvest,
        )
        if notes:
            extra_obs = profile_notes_to_observations(notes)
            final_observations = merge_profile_observations(observations, extra_obs)
            final_candidates = cluster_observations(final_observations)
            decisions, resolved_snapshot, quality2, llm2 = fuse_observations(
                final_observations,
                final_candidates,
                reconciliation,
                prefer_llm=prefer_llm,
                profile_rounds=1,
            )
            judge_passes += quality2.judge_passes
            llm_used = llm_used or llm2
            quality = fusion_quality_from_decisions(
                decisions,
                len(final_observations),
                len(final_candidates),
                llm_used=llm_used,
                judge_passes=judge_passes,
                profile_evidence_rounds=1,
            )
            logger.info(
                "[n1c] Profile evidence round applied (%d notes), judge_passes=%s",
                len(notes),
                judge_passes,
            )

    unresolved = build_unresolved_items(decisions)
    return SemanticGovernanceFusion(
        observations=final_observations,
        candidates=final_candidates,
        fusion_decisions=decisions,
        resolved_snapshot=resolved_snapshot,
        fusion_quality=quality,
        unresolved_items=unresolved,
        profile_evidence_notes=notes,
    )
