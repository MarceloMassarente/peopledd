from __future__ import annotations

import uuid
from typing import Literal

from peopledd.models.common import SourceRef
from peopledd.models.contracts import (
    EvidenceSpan,
    GovernanceIngestion,
    GovernanceObservation,
    GovernanceSeed,
    GovernanceSnapshot,
)


def _current_track(
    ingestion: GovernanceIngestion,
) -> Literal["current_ri", "current_private_web"]:
    meta = ingestion.ingestion_metadata or {}
    if meta.get("private_web_discovery") == "1":
        return "current_private_web"
    return "current_ri"


def _obs_from_board(
    snapshot: GovernanceSnapshot,
    source_track: Literal["formal_fre", "current_ri", "current_private_web"],
    base_confidence: float,
) -> list[GovernanceObservation]:
    out: list[GovernanceObservation] = []
    for m in snapshot.board_members:
        oid = str(uuid.uuid4())
        span = None
        if m.source_refs:
            sr0 = m.source_refs[0]
            span = EvidenceSpan(url_or_ref=sr0.url_or_ref, snippet=m.role if hasattr(m, "role") else None)
        ref = (
            m.source_refs[0]
            if m.source_refs
            else SourceRef(source_type="governance_snapshot", label="board", url_or_ref="internal://board")
        )
        out.append(
            GovernanceObservation(
                observation_id=oid,
                observed_name=m.person_name,
                observed_role=m.role if m.role != "unknown" else None,
                organ="board",
                source_track=source_track,
                source_ref=ref,
                evidence_span=span,
                as_of_date=snapshot.as_of_date,
                source_confidence=base_confidence,
                raw_attributes={"independence_status": m.independence_status},
            )
        )
    return out


def _obs_from_executives(
    snapshot: GovernanceSnapshot,
    source_track: Literal["formal_fre", "current_ri", "current_private_web"],
    base_confidence: float,
) -> list[GovernanceObservation]:
    out: list[GovernanceObservation] = []
    for e in snapshot.executive_members:
        oid = str(uuid.uuid4())
        span = None
        if e.source_refs:
            span = EvidenceSpan(url_or_ref=e.source_refs[0].url_or_ref, snippet=e.formal_title)
        ref = (
            e.source_refs[0]
            if e.source_refs
            else SourceRef(source_type="governance_snapshot", label="executive", url_or_ref="internal://exec")
        )
        out.append(
            GovernanceObservation(
                observation_id=oid,
                observed_name=e.person_name,
                observed_role=e.formal_title,
                organ="executive",
                source_track=source_track,
                source_ref=ref,
                evidence_span=span,
                as_of_date=snapshot.as_of_date,
                source_confidence=base_confidence,
                raw_attributes={"normalized_role": e.normalized_role},
            )
        )
    return out


def _obs_from_committees(
    snapshot: GovernanceSnapshot,
    source_track: Literal["formal_fre", "current_ri", "current_private_web"],
    base_confidence: float,
) -> list[GovernanceObservation]:
    out: list[GovernanceObservation] = []
    for c in snapshot.committees:
        for mem in c.members:
            oid = str(uuid.uuid4())
            ref = (
                c.source_refs[0]
                if c.source_refs
                else SourceRef(
                    source_type="governance_snapshot",
                    label=c.committee_name,
                    url_or_ref="internal://committee",
                )
            )
            span = EvidenceSpan(url_or_ref=ref.url_or_ref, snippet=c.committee_name)
            out.append(
                GovernanceObservation(
                    observation_id=oid,
                    observed_name=mem.person_name,
                    observed_role=mem.position_in_committee,
                    organ="committee",
                    source_track=source_track,
                    source_ref=ref,
                    evidence_span=span,
                    as_of_date=snapshot.as_of_date,
                    source_confidence=base_confidence,
                    raw_attributes={
                        "committee_name": c.committee_name,
                        "committee_type": c.committee_type,
                    },
                )
            )
    return out


def _obs_from_fiscal(
    snapshot: GovernanceSnapshot,
    source_track: Literal["formal_fre", "current_ri", "current_private_web"],
    base_confidence: float,
) -> list[GovernanceObservation]:
    out: list[GovernanceObservation] = []
    for m in snapshot.fiscal_council:
        oid = str(uuid.uuid4())
        ref = (
            m.source_refs[0]
            if m.source_refs
            else SourceRef(source_type="governance_snapshot", label="fiscal", url_or_ref="internal://fiscal")
        )
        span = EvidenceSpan(url_or_ref=ref.url_or_ref, snippet=m.role if hasattr(m, "role") else None)
        out.append(
            GovernanceObservation(
                observation_id=oid,
                observed_name=m.person_name,
                observed_role=m.role if m.role != "unknown" else None,
                organ="fiscal_council",
                source_track=source_track,
                source_ref=ref,
                evidence_span=span,
                as_of_date=snapshot.as_of_date,
                source_confidence=base_confidence,
                raw_attributes={},
            )
        )
    return out


def _seed_source_ref(seed: GovernanceSeed):
    if seed.source_refs:
        return seed.source_refs[0]
    return SourceRef(source_type="sonar", label="seed", url_or_ref="internal://seed")


def build_governance_observations(
    ingestion: GovernanceIngestion,
    governance_seed: GovernanceSeed | None = None,
) -> list[GovernanceObservation]:
    """
    Flatten formal and current governance snapshots into atomic observations.
    Does not mutate ingestion. Optional sonar seed is treated as low-authority hypothesis.
    """
    observations: list[GovernanceObservation] = []
    formal = ingestion.formal_governance_snapshot
    current = ingestion.current_governance_snapshot
    current_track = _current_track(ingestion)

    observations.extend(_obs_from_board(formal, "formal_fre", 0.92))
    observations.extend(_obs_from_executives(formal, "formal_fre", 0.92))
    observations.extend(_obs_from_committees(formal, "formal_fre", 0.88))
    observations.extend(_obs_from_fiscal(formal, "formal_fre", 0.88))

    observations.extend(_obs_from_board(current, current_track, 0.78))
    observations.extend(_obs_from_executives(current, current_track, 0.78))
    observations.extend(_obs_from_committees(current, current_track, 0.72))
    observations.extend(_obs_from_fiscal(current, current_track, 0.72))

    if governance_seed is not None:
        seed_ref = _seed_source_ref(governance_seed)
        for m in governance_seed.board_members:
            observations.append(
                GovernanceObservation(
                    observation_id=str(uuid.uuid4()),
                    observed_name=m.person_name,
                    observed_role=m.role_or_title,
                    organ="board",
                    source_track="seed_sonar",
                    source_ref=seed_ref,
                    evidence_span=EvidenceSpan(
                        url_or_ref=m.evidence_url or seed_ref.url_or_ref,
                        snippet=m.role_or_title,
                    ),
                    as_of_date=governance_seed.generated_at,
                    source_confidence=min(0.7, max(0.2, governance_seed.confidence or 0.45)),
                    raw_attributes={"seed_provider": governance_seed.provider},
                )
            )
        for m in governance_seed.executive_members:
            observations.append(
                GovernanceObservation(
                    observation_id=str(uuid.uuid4()),
                    observed_name=m.person_name,
                    observed_role=m.role_or_title,
                    organ="executive",
                    source_track="seed_sonar",
                    source_ref=seed_ref,
                    evidence_span=EvidenceSpan(
                        url_or_ref=m.evidence_url or seed_ref.url_or_ref,
                        snippet=m.role_or_title,
                    ),
                    as_of_date=governance_seed.generated_at,
                    source_confidence=min(0.7, max(0.2, governance_seed.confidence or 0.45)),
                    raw_attributes={"seed_provider": governance_seed.provider},
                )
            )

    return observations
