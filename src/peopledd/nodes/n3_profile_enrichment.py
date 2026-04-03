from __future__ import annotations

from peopledd.models.contracts import PersonProfile, PersonResolution, ProfileQuality


def run(resolved_people: list[PersonResolution]) -> list[PersonProfile]:
    profiles: list[PersonProfile] = []
    for person in resolved_people:
        coverage = min(0.85, 0.45 + 0.1 * len(person.matched_profiles))
        profiles.append(
            PersonProfile(
                person_id=person.person_id,
                career_summary={
                    "current_roles": ["current executive/board role"],
                    "prior_roles": ["prior role unavailable in stub"],
                    "functional_experience": ["general_management"],
                    "industry_experience": ["unknown"],
                    "governance_signals": ["board_exposure"],
                },
                profile_quality=ProfileQuality(
                    nominal_hit=bool(person.matched_profiles),
                    useful_coverage_score=coverage,
                    evidence_density=0.55,
                    recency_score=0.5,
                    profile_confidence=0.6,
                ),
                blind_spots=["education_background", "multi_board_history"],
            )
        )
    return profiles
