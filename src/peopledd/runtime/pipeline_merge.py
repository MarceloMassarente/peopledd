from __future__ import annotations

from typing import Any

from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import (
    KeyChallenge,
    PersonProfile,
    PersonResolution,
    ProfileQuality,
    StrategicPriority,
    StrategyChallenges,
)

_RESOLUTION_RANK: dict[ResolutionStatus, int] = {
    ResolutionStatus.RESOLVED: 4,
    ResolutionStatus.PARTIAL: 3,
    ResolutionStatus.AMBIGUOUS: 2,
    ResolutionStatus.NOT_FOUND: 1,
}


def resolution_rank(pr: PersonResolution) -> int:
    return _RESOLUTION_RANK.get(pr.resolution_status, 0)


def strategy_is_empty(raw_sc: Any) -> bool:
    return not raw_sc.strategic_priorities and not raw_sc.key_challenges


def _priority_key(p: StrategicPriority) -> tuple[str, str]:
    return (p.priority.strip().lower(), p.time_horizon)


def _challenge_key(c: KeyChallenge) -> tuple[str, str]:
    return (c.challenge.strip().lower(), c.challenge_type)


def merge_strategy_challenges(base: StrategyChallenges, retry: StrategyChallenges) -> StrategyChallenges:
    """Union of priorities, challenges, and sonar briefs (retry wins same role)."""
    seen_p = {_priority_key(p) for p in base.strategic_priorities}
    merged_p = list(base.strategic_priorities)
    for p in retry.strategic_priorities:
        k = _priority_key(p)
        if k not in seen_p:
            seen_p.add(k)
            merged_p.append(p)

    seen_c = {_challenge_key(c) for c in base.key_challenges}
    merged_c = list(base.key_challenges)
    for c in retry.key_challenges:
        k = _challenge_key(c)
        if k not in seen_c:
            seen_c.add(k)
            merged_c.append(c)

    sonar_by_role: dict[str, Any] = {}
    for b in base.external_sonar_briefs:
        sonar_by_role[b.role] = b
    for b in retry.external_sonar_briefs:
        sonar_by_role[b.role] = b
    merged_sonar = list(sonar_by_role.values())

    merged_triggers = list(dict.fromkeys([*base.recent_triggers, *retry.recent_triggers]))
    merged_phase = {**base.company_phase_hypothesis, **retry.company_phase_hypothesis}
    return retry.model_copy(
        update={
            "strategic_priorities": merged_p,
            "key_challenges": merged_c,
            "external_sonar_briefs": merged_sonar,
            "recent_triggers": merged_triggers,
            "company_phase_hypothesis": merged_phase,
        }
    )


def merge_people_resolution(base: list[PersonResolution], retry: list[PersonResolution]) -> list[PersonResolution]:
    base_map = {pr.observed_name: pr for pr in base}
    for pr in retry:
        existing = base_map.get(pr.observed_name)
        if existing is None or resolution_rank(pr) > resolution_rank(existing):
            base_map[pr.observed_name] = pr
    return list(base_map.values())


def merge_people_phase_outputs(
    base_res: list[PersonResolution],
    retry_res: list[PersonResolution],
    base_prof: list[PersonProfile],
    retry_prof: list[PersonProfile],
) -> tuple[list[PersonResolution], list[PersonProfile]]:
    merged_res = merge_people_resolution(base_res, retry_res)
    merged_prof: dict[str, PersonProfile] = {}
    for p in base_prof:
        merged_prof[p.person_id] = p
    for p in retry_prof:
        existing = merged_prof.get(p.person_id)
        if existing is None or p.profile_quality.useful_coverage_score > existing.profile_quality.useful_coverage_score:
            merged_prof[p.person_id] = p
    ordered: list[PersonProfile] = []
    for r in merged_res:
        if r.person_id in merged_prof:
            ordered.append(merged_prof[r.person_id])
        else:
            ordered.append(
                PersonProfile(
                    person_id=r.person_id,
                    career_summary={},
                    profile_quality=ProfileQuality(),
                    blind_spots=["profile_not_found"],
                )
            )
    return merged_res, ordered


def aggregate_harvest_recall_totals(people_resolution: list[Any]) -> dict[str, int]:
    totals: dict[str, int] = {
        "raw_hits_profile_search_sum": 0,
        "after_filter_count_sum": 0,
        "anonymized_dropped_count_sum": 0,
        "people_with_profile_search_retry": 0,
        "people_with_secondary_web_sourcing": 0,
        "people_with_resolution_attempted": 0,
    }
    for pr in people_resolution:
        h = getattr(pr, "harvest_recall", None)
        if h is None:
            continue
        totals["raw_hits_profile_search_sum"] += int(h.raw_hits_profile_search)
        totals["after_filter_count_sum"] += int(h.after_filter_count)
        totals["anonymized_dropped_count_sum"] += int(h.anonymized_dropped_count)
        if h.profile_search_retry_used:
            totals["people_with_profile_search_retry"] += 1
        if h.secondary_web_sourcing_used:
            totals["people_with_secondary_web_sourcing"] += 1
        if h.resolution_attempted:
            totals["people_with_resolution_attempted"] += 1
    return totals
