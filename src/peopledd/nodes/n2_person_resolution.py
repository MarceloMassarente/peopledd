from __future__ import annotations

"""
n2_person_resolution — resolve board/exec members to LinkedIn profiles via Harvest.

For each person from the reconciled governance snapshot:
  1. search_by_name(name, company) via HarvestAdapter
  2. If empty and SearchOrchestrator has Exa, secondary resolution via Exa People Search (person_sourcing)
  3. Dedup: anonymized profiles and low-similarity homonyms are filtered in the adapter
  4. If 2+ candidates with high and similar scores → status=ambiguous
  5. Confidence: company match bumps when top result matches context; exa_people matches are capped lower
"""

import logging
import uuid
from typing import TYPE_CHECKING, Literal

from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import (
    GovernanceReconciliation,
    HarvestRecallMeta,
    MatchedProfile,
    PersonResolution,
)
from peopledd.runtime.adaptive_models import PersonSearchParams
from peopledd.services.harvest_adapter import HarvestAdapter, ProfileSearchOutcome

if TYPE_CHECKING:
    from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)


def run(
    reconciled: GovernanceReconciliation,
    harvest: HarvestAdapter,
    company_name: str | None = None,
    search_orchestrator: SearchOrchestrator | None = None,
    use_harvest: bool = True,
    person_search_params: PersonSearchParams | None = None,
    names_subset: set[str] | None = None,
    resolution_purpose: Literal["governance_member", "fusion_evidence"] = "governance_member",
) -> list[PersonResolution]:
    """
    Resolve each governance member to a LinkedIn profile.

    Args:
        reconciled: output of n1b with the reconciled governance snapshot.
        harvest: configured HarvestAdapter instance.
        company_name: canonical company name for context (improves Harvest search precision).
        search_orchestrator: optional SearchOrchestrator; secondary person resolution needs EXA_API_KEY.
        use_harvest: when False, skip Harvest search_by_name (secondary web sourcing may still run).
        person_search_params: optional tuning for secondary LinkedIn URL discovery.
        names_subset: when set, only resolve these observed names (for fusion-evidence passes).
        resolution_purpose: stored on each PersonResolution for downstream audit.
    """
    snapshot = reconciled.reconciled_governance_snapshot
    people = {m.person_name for m in snapshot.board_members}
    people.update({e.person_name for e in snapshot.executive_members})

    results: list[PersonResolution] = []
    pparams = person_search_params or PersonSearchParams.default()
    sorted_people = sorted(people)

    for attempt_index, name in enumerate(sorted_people):
        if not name or not name.strip():
            continue
        if names_subset is not None and name not in names_subset:
            continue

        candidates_from_sourcing = False
        harvest_recall: HarvestRecallMeta | None = None
        if use_harvest:
            try:
                outcome: ProfileSearchOutcome = harvest.search_by_name(
                    name=name,
                    company=company_name,
                )
                harvest_recall = outcome.recall.model_copy(deep=True)
                candidates = outcome.candidates
            except Exception as e:
                logger.error("[n2] Harvest search failed for '%s': %s", name, e)
                results.append(PersonResolution(
                    person_id=str(uuid.uuid4()),
                    observed_name=name,
                    resolution_status=ResolutionStatus.NOT_FOUND,
                    resolution_confidence=0.0,
                    harvest_recall=HarvestRecallMeta(resolution_attempted=True),
                    resolution_purpose=resolution_purpose,
                ))
                continue
        else:
            harvest_recall = HarvestRecallMeta(resolution_attempted=False)
            candidates = []

        if not candidates and search_orchestrator is not None:
            from peopledd.services import person_sourcing

            urls = person_sourcing.linkedin_profile_urls(
                search_orchestrator,
                name,
                company_name,
                person_params=pparams,
                attempt_index=attempt_index,
            )
            if urls:
                candidates = person_sourcing.harvest_style_results_from_urls(
                    urls[:5], name, company_name
                )
                candidates_from_sourcing = True
                if harvest_recall is not None:
                    harvest_recall = harvest_recall.model_copy(
                        update={"secondary_web_sourcing_used": True}
                    )
                logger.info(
                    "[n2] Secondary sourcing found %d LinkedIn URL(s) for '%s'",
                    len(candidates),
                    name,
                )

        if not candidates:
            results.append(PersonResolution(
                person_id=str(uuid.uuid4()),
                observed_name=name,
                resolution_status=ResolutionStatus.NOT_FOUND,
                resolution_confidence=0.2,
                harvest_recall=harvest_recall,
                resolution_purpose=resolution_purpose,
            ))
            logger.info("[n2] No profiles found for '%s'", name)
            continue

        top = candidates[0]

        # Ambiguity: 2+ candidates with high similarity close together
        ambiguous = (
            len(candidates) >= 2
            and candidates[1].name_similarity >= 0.7
            and abs(top.name_similarity - candidates[1].name_similarity) < 0.1
        )

        # Confidence scoring
        base_confidence = top.name_similarity
        if top.company_match:
            base_confidence = min(1.0, base_confidence + 0.12)
        if ambiguous:
            base_confidence = min(base_confidence, 0.65)
        if candidates_from_sourcing:
            base_confidence = min(base_confidence, 0.56)

        status = ResolutionStatus.AMBIGUOUS if ambiguous else ResolutionStatus.RESOLVED

        prov = "exa_people" if candidates_from_sourcing else "harvest"
        matched = [
            MatchedProfile(
                provider=prov,
                profile_id_or_url=c.linkedin_url,
                match_confidence=round(c.name_similarity + (0.1 if c.company_match else 0.0), 3),
            )
            for c in candidates[:3]
            if c.linkedin_url
        ]

        canonical_name = top.name if top.name else None

        results.append(PersonResolution(
            person_id=str(uuid.uuid4()),
            observed_name=name,
            canonical_name=canonical_name,
            resolution_status=status,
            resolution_confidence=round(base_confidence, 3),
            matched_profiles=matched,
            harvest_recall=harvest_recall,
            resolution_purpose=resolution_purpose,
        ))

        logger.info(
            f"[n2] '{name}' → {status.value} | "
            f"confidence={base_confidence:.2f} | "
            f"top='{top.name}' ({top.linkedin_url})"
        )

    return results

