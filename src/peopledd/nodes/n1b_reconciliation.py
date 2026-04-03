from __future__ import annotations

from peopledd.models.contracts import ConflictItem, GovernanceIngestion, GovernanceReconciliation


def run(ingestion: GovernanceIngestion) -> GovernanceReconciliation:
    conflicts: list[ConflictItem] = []

    formal_titles = {x.person_name: x.formal_title for x in ingestion.formal_governance_snapshot.executive_members}
    for current_exec in ingestion.current_governance_snapshot.executive_members:
        formal_title = formal_titles.get(current_exec.person_name)
        if formal_title and formal_title != current_exec.formal_title:
            conflicts.append(
                ConflictItem(
                    conflict_type="title_mismatch",
                    person_name=current_exec.person_name,
                    formal_value=formal_title,
                    current_value=current_exec.formal_title,
                    resolution_rule_applied="prefer_current_for_freshness_with_flag",
                    confidence=0.6,
                )
            )

    if not ingestion.formal_governance_snapshot.board_members:
        status = "current_only"
    elif not ingestion.current_governance_snapshot.board_members:
        status = "formal_only"
    elif conflicts:
        status = "minor_conflicts"
    else:
        status = "clean"

    reconciled = ingestion.formal_governance_snapshot.model_copy(deep=True)
    if ingestion.current_governance_snapshot.executive_members:
        reconciled.executive_members = ingestion.current_governance_snapshot.executive_members

    return GovernanceReconciliation(
        reconciliation_status=status,
        conflict_items=conflicts,
        reconciled_governance_snapshot=reconciled,
        reporting_basis={
            "formal_basis_date": ingestion.formal_governance_snapshot.as_of_date,
            "current_basis_date": ingestion.current_governance_snapshot.as_of_date,
            "preferred_view_for_reporting": "reconciled",
        },
    )
