from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

from peopledd.models.contracts import EvidenceSpan, GovernanceObservation, ProfileEvidenceNote
from peopledd.services.harvest_adapter import HarvestAdapter

if TYPE_CHECKING:
    from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)

_PROFILE_EVIDENCE_MAX_WORKERS = 4


def gather_profile_evidence(
    person_names: list[str],
    company_name: str | None,
    harvest: HarvestAdapter | None,
    search_orchestrator: SearchOrchestrator | None,
    *,
    use_harvest: bool = True,
    max_people: int = 8,
) -> list[ProfileEvidenceNote]:
    """
    Collect short public-profile lines to disambiguate names during semantic fusion.
    Uses Harvest when enabled; does not mutate pipeline people_resolution.
    """
    notes: list[ProfileEvidenceNote] = []
    if not person_names:
        return notes

    uniq: list[str] = []
    seen: set[str] = set()
    for n in person_names:
        k = n.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(n.strip())
        if len(uniq) >= max_people:
            break

    if use_harvest and harvest is not None:

        def _harvest_one(name: str) -> ProfileEvidenceNote | None:
            try:
                outcome = harvest.search_by_name(name=name, company=company_name or "")
            except Exception as e:
                logger.warning("[fusion_profile_evidence] Harvest search failed for %s: %s", name, e)
                return None
            if not outcome.candidates:
                return None
            top = outcome.candidates[0]
            headline = ""
            if top.linkedin_url:
                prof = harvest.get_profile(top.linkedin_url)
                if isinstance(prof, dict):
                    headline = str(prof.get("headline") or prof.get("title") or "")[:500]
            if not headline:
                headline = f"Top match: {top.name}"[:500]
            return ProfileEvidenceNote(
                person_name_observed=name,
                evidence_text=headline,
                source_url=top.linkedin_url,
                provider="harvest",
            )

        with ThreadPoolExecutor(max_workers=_PROFILE_EVIDENCE_MAX_WORKERS) as pool:
            futs = {pool.submit(_harvest_one, name): name for name in uniq}
            by_name: dict[str, ProfileEvidenceNote | None] = {n: None for n in uniq}
            for fut in as_completed(futs):
                nm = futs[fut]
                try:
                    by_name[nm] = fut.result()
                except Exception as e:
                    logger.warning("[fusion_profile_evidence] Harvest worker failed for %s: %s", nm, e)
            for name in uniq:
                note = by_name.get(name)
                if note is not None:
                    notes.append(note)
        return notes

    if search_orchestrator is not None:
        try:
            from peopledd.services import person_sourcing
            from peopledd.runtime.adaptive_models import PersonSearchParams
        except ImportError:
            return notes

        pparams = PersonSearchParams.default()

        def _exa_one(pair: tuple[int, str]) -> tuple[str, ProfileEvidenceNote | None]:
            i, name = pair
            try:
                urls = person_sourcing.linkedin_profile_urls(
                    search_orchestrator,
                    name,
                    company_name,
                    person_params=pparams,
                    attempt_index=i,
                )
            except Exception as e:
                logger.warning("[fusion_profile_evidence] Exa sourcing failed for %s: %s", name, e)
                return name, None
            if not urls:
                return name, None
            return name, ProfileEvidenceNote(
                person_name_observed=name,
                evidence_text="LinkedIn candidate URL discovered for disambiguation.",
                source_url=urls[0],
                provider="exa_people",
            )

        indexed = list(enumerate(uniq))
        with ThreadPoolExecutor(max_workers=_PROFILE_EVIDENCE_MAX_WORKERS) as pool:
            futs = {pool.submit(_exa_one, (i, n)): n for i, n in indexed}
            by_name: dict[str, ProfileEvidenceNote | None] = {n: None for n in uniq}
            for fut in as_completed(futs):
                nm = futs[fut]
                try:
                    _, note = fut.result()
                    by_name[nm] = note
                except Exception as e:
                    logger.warning("[fusion_profile_evidence] Exa worker failed for %s: %s", nm, e)
            for name in uniq:
                note = by_name.get(name)
                if note is not None:
                    notes.append(note)

    return notes


def profile_notes_to_observations(notes: list[ProfileEvidenceNote]) -> list[GovernanceObservation]:
    """Convert profile notes into synthetic GovernanceObservation objects."""
    import uuid

    from peopledd.models.common import SourceRef

    out: list[GovernanceObservation] = []
    for note in notes:
        oid = str(uuid.uuid4())
        url = note.source_url or "profile://unknown"
        out.append(
            GovernanceObservation(
                observation_id=oid,
                observed_name=note.person_name_observed,
                observed_role=None,
                organ="unknown",
                source_track="profile_evidence",
                source_ref=SourceRef(
                    source_type="profile_evidence",
                    label=note.provider,
                    url_or_ref=url,
                ),
                evidence_span=EvidenceSpan(url_or_ref=url, snippet=note.evidence_text[:2000]),
                source_confidence=0.55,
                raw_attributes={"provider": note.provider},
            )
        )
    return out
