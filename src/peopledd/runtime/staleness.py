from __future__ import annotations

from peopledd.models.common import ServiceLevel
from peopledd.models.contracts import GovernanceIngestion, PersonProfile


def compute_staleness_and_sl_dimensions(
    ingestion: GovernanceIngestion,
    people_profiles: list[PersonProfile],
    global_sl: ServiceLevel,
) -> tuple[dict[str, bool], dict[str, str]]:
    """
    Derive per-dimension staleness flags and coarse SL labels for reporting heatmap.

    Returns:
        staleness_by_dimension: e.g. formal, current, harvest, strategy_context
        sl_by_dimension: stringified ServiceLevel per dimension (aligned or weaker than global)
    """
    gq = ingestion.governance_data_quality
    formal_date = ingestion.formal_governance_snapshot.as_of_date
    current_date = ingestion.current_governance_snapshot.as_of_date

    stale_formal = gq.formal_completeness < 0.5 or (gq.freshness_score < 0.5 and bool(formal_date))
    stale_current = gq.current_completeness < 0.5 or not current_date

    if people_profiles:
        avg_evidence = sum(p.profile_quality.evidence_density for p in people_profiles) / len(people_profiles)
        stale_harvest = avg_evidence < 0.25
    else:
        stale_harvest = True

    staleness = {
        "formal": stale_formal,
        "current": stale_current,
        "harvest": stale_harvest,
    }

    def dim_sl(stale: bool, weak_data: bool) -> str:
        if weak_data:
            return ServiceLevel.SL5.value
        if stale:
            if global_sl.value in ("SL1", "SL2"):
                return ServiceLevel.SL3.value
            return global_sl.value
        return global_sl.value

    sl_map = {
        "formal": dim_sl(stale_formal, gq.formal_completeness < 0.35),
        "current": dim_sl(stale_current, gq.current_completeness < 0.35),
        "harvest": dim_sl(stale_harvest, stale_harvest and not people_profiles),
        "strategy_context": global_sl.value,
    }

    return staleness, sl_map
