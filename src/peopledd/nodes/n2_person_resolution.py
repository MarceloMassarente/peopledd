from __future__ import annotations

"""
n2_person_resolution — resolve board/exec members to LinkedIn profiles via Harvest.

For each person from the reconciled governance snapshot:
  1. search_by_name(name, company) via HarvestAdapter
  2. Dedup: anonymized profiles and low-similarity homonyms are filtered in the adapter
  3. If 2+ candidates with high and similar scores → status=ambiguous
  4. Confidence: CNPJ match (company) bumps to 0.85 if top result company matches
"""

import logging
import uuid

from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import (
    GovernanceReconciliation,
    MatchedProfile,
    PersonResolution,
)
from peopledd.services.harvest_adapter import HarvestAdapter

logger = logging.getLogger(__name__)


def run(
    reconciled: GovernanceReconciliation,
    harvest: HarvestAdapter,
    company_name: str | None = None,
) -> list[PersonResolution]:
    """
    Resolve each governance member to a LinkedIn profile.

    Args:
        reconciled: output of n1b with the reconciled governance snapshot.
        harvest: configured HarvestAdapter instance.
        company_name: canonical company name for context (improves Harvest search precision).
    """
    snapshot = reconciled.reconciled_governance_snapshot
    people = {m.person_name for m in snapshot.board_members}
    people.update({e.person_name for e in snapshot.executive_members})

    results: list[PersonResolution] = []

    for name in sorted(people):
        if not name or not name.strip():
            continue

        try:
            candidates = harvest.search_by_name(
                name=name,
                company=company_name,
            )
        except Exception as e:
            logger.error(f"[n2] Harvest search failed for '{name}': {e}")
            results.append(PersonResolution(
                person_id=str(uuid.uuid4()),
                observed_name=name,
                resolution_status=ResolutionStatus.NOT_FOUND,
                resolution_confidence=0.0,
            ))
            continue

        if not candidates:
            results.append(PersonResolution(
                person_id=str(uuid.uuid4()),
                observed_name=name,
                resolution_status=ResolutionStatus.NOT_FOUND,
                resolution_confidence=0.2,
            ))
            logger.info(f"[n2] No profiles found for '{name}'")
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

        status = ResolutionStatus.AMBIGUOUS if ambiguous else ResolutionStatus.RESOLVED

        matched = [
            MatchedProfile(
                provider="harvest",
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
        ))

        logger.info(
            f"[n2] '{name}' → {status.value} | "
            f"confidence={base_confidence:.2f} | "
            f"top='{top.name}' ({top.linkedin_url})"
        )

    return results

