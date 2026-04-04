from __future__ import annotations

"""
n3_profile_enrichment — enrich PersonResolution with full Harvest profile data.

For each resolved person:
  1. get_profile(linkedin_url) via HarvestAdapter (uses in-process cache, so no re-fetch)
  2. build_career_summary: current_roles, prior_roles, functional_experience, governance_signals
  3. compute_profile_quality: useful_coverage_score, evidence_density, recency_score, profile_confidence
  4. blind_spots: infer what's missing from the profile
"""

import logging

from peopledd.models.contracts import PersonProfile, PersonResolution, ProfileQuality
from peopledd.models.common import ResolutionStatus
from peopledd.services.harvest_adapter import HarvestAdapter

logger = logging.getLogger(__name__)


def run(
    resolved_people: list[PersonResolution],
    harvest: HarvestAdapter,
    use_harvest: bool = True,
) -> list[PersonProfile]:
    """
    Enrich each resolved person with their full Harvest LinkedIn profile.

    Degrades gracefully: if Harvest fetch fails, returns partial profile with quality signal.

    When use_harvest is False, get_profile is never called (metrics still computed on empty data).
    """
    profiles: list[PersonProfile] = []

    for person in resolved_people:
        # Only attempt fetch if we have a LinkedIn URL
        linkedin_url = None
        if person.matched_profiles:
            linkedin_url = person.matched_profiles[0].profile_id_or_url

        compact = None
        if (
            use_harvest
            and linkedin_url
            and person.resolution_status != ResolutionStatus.NOT_FOUND
        ):
            try:
                compact = harvest.get_profile(linkedin_url)
            except Exception as e:
                logger.warning(f"[n3] get_profile failed for '{person.observed_name}': {e}")

        # Build metrics
        quality_metrics = harvest.compute_profile_quality(compact)
        career = harvest.build_career_summary(compact)

        # Blind spots: enumerate what's missing
        blind_spots: list[str] = []
        if not compact:
            blind_spots.append("profile_not_found")
            if person.matched_profiles and person.matched_profiles[0].provider in (
                "exa_web",
                "exa_people",
            ):
                blind_spots.append("exa_url_only_no_harvest_profile")
        else:
            if not career.get("current_roles"):
                blind_spots.append("no_current_role")
            if not career.get("functional_experience"):
                blind_spots.append("unclear_functional_scope")
            if not (compact.get("education") or []):
                blind_spots.append("education_background")
            if quality_metrics["evidence_density"] < 0.3:
                blind_spots.append("low_experience_descriptions")
            if person.resolution_status == ResolutionStatus.AMBIGUOUS:
                blind_spots.append("ambiguous_profile_match")

        nominal_hit = bool(compact and compact.get("name"))

        logger.info(
            f"[n3] '{person.observed_name}': "
            f"coverage={quality_metrics['useful_coverage_score']:.2f} "
            f"density={quality_metrics['evidence_density']:.2f} "
            f"blind_spots={blind_spots}"
        )

        profiles.append(
            PersonProfile(
                person_id=person.person_id,
                career_summary=career,
                profile_quality=ProfileQuality(
                    nominal_hit=nominal_hit,
                    useful_coverage_score=quality_metrics["useful_coverage_score"],
                    evidence_density=quality_metrics["evidence_density"],
                    recency_score=quality_metrics["recency_score"],
                    profile_confidence=quality_metrics["profile_confidence"],
                ),
                blind_spots=blind_spots,
            )
        )

    return profiles

