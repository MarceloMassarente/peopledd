from __future__ import annotations

import uuid

from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import GovernanceReconciliation, MatchedProfile, PersonResolution
from peopledd.services.connectors import HarvestConnector


def run(reconciled: GovernanceReconciliation, harvest: HarvestConnector) -> list[PersonResolution]:
    people = {m.person_name for m in reconciled.reconciled_governance_snapshot.board_members}
    people.update({e.person_name for e in reconciled.reconciled_governance_snapshot.executive_members})

    results: list[PersonResolution] = []
    for name in sorted(people):
        response = harvest.resolve_person(name, context={})
        matched = [MatchedProfile(**m) for m in response.payload.get("matched_profiles", [])]
        results.append(
            PersonResolution(
                person_id=str(uuid.uuid4()),
                observed_name=name,
                canonical_name=response.payload.get("canonical_name"),
                resolution_status=ResolutionStatus.RESOLVED if response.ok else ResolutionStatus.PARTIAL,
                resolution_confidence=0.6 if response.ok else 0.3,
                matched_profiles=matched,
            )
        )
    return results
