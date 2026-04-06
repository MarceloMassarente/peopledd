from __future__ import annotations

from peopledd.models.contracts import BoardMember, Committee, ExecutiveMember, GovernanceSnapshot
from peopledd.utils.text import normalize_company_name


def formal_track_completeness(snapshot: GovernanceSnapshot) -> float:
    """Same weighting as n1 formal completeness (FRE track)."""
    score = 0.0
    if snapshot.board_members:
        score += 0.5
    if snapshot.executive_members:
        score += 0.35
    if snapshot.committees:
        score += 0.15
    return round(score, 2)


def current_track_completeness(snapshot: GovernanceSnapshot) -> float:
    """Same weighting as n1 current completeness (RI / private web track)."""
    score = 0.0
    if snapshot.board_members:
        score += 0.55
    if snapshot.executive_members:
        score += 0.30
    if snapshot.committees:
        score += 0.15
    return round(score, 2)


def _name_key(name: str) -> str:
    return normalize_company_name(name or "").strip().lower()


def merge_governance_snapshots(base: GovernanceSnapshot, extra: GovernanceSnapshot) -> GovernanceSnapshot:
    """
    Union board, executives, and committees by normalized person/committee identity.
    """
    board_by: dict[str, BoardMember] = {}
    for m in base.board_members:
        board_by[_name_key(m.person_name)] = m
    for m in extra.board_members:
        k = _name_key(m.person_name)
        if k not in board_by:
            board_by[k] = m

    exec_by: dict[str, ExecutiveMember] = {}
    for e in base.executive_members:
        exec_by[_name_key(e.person_name)] = e
    for e in extra.executive_members:
        k = _name_key(e.person_name)
        if k not in exec_by:
            exec_by[k] = e

    committee_keys: dict[str, Committee] = {}
    for c in base.committees:
        key = normalize_company_name(c.committee_name or "").strip().lower()
        if key:
            committee_keys[key] = c
    for c in extra.committees:
        key = normalize_company_name(c.committee_name or "").strip().lower()
        if key and key not in committee_keys:
            committee_keys[key] = c

    fiscal_by: dict[str, BoardMember] = {}
    for f in base.fiscal_council:
        fiscal_by[_name_key(f.person_name)] = f
    for f in extra.fiscal_council:
        k = _name_key(f.person_name)
        if k not in fiscal_by:
            fiscal_by[k] = f

    as_of = extra.as_of_date or base.as_of_date

    return GovernanceSnapshot(
        as_of_date=as_of,
        board_members=list(board_by.values()),
        executive_members=list(exec_by.values()),
        committees=list(committee_keys.values()),
        fiscal_council=list(fiscal_by.values()),
    )
