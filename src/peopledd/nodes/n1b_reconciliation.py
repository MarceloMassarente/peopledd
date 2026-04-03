from __future__ import annotations

"""
n1b_reconciliation — Reconciles formal (FRE) vs current (RI) governance snapshots.

Conflict types:
  missing_person      — person in one track not found in the other
  title_mismatch      — same person, different title in FRE vs RI
  organ_mismatch      — person appears in a different organ (board ↔ exec)
  term_mismatch       — date divergence > 90 days
  independence_mismatch — independence flag differs between tracks

Fuzzy name matching: ported _shingles() + _jaccard() from deepsearch deduplicator.
Threshold: Jaccard(trigrams) >= 0.6 = same person.
"""

import logging
from datetime import datetime
from typing import Set, Tuple

from peopledd.models.contracts import (
    BoardMember,
    ConflictItem,
    ExecutiveMember,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy name matching (ported from deepsearch deduplicator.py)
# ─────────────────────────────────────────────────────────────────────────────

_STOPWORDS = {"de", "da", "do", "dos", "das", "e", "a", "o"}

FUZZY_MATCH_THRESHOLD = 0.6  # Jaccard trigram threshold for same-person match


def _shingles(s: str, n: int = 3) -> Set[Tuple[str, ...]]:
    """Generate n-gram shingles from a name string."""
    tokens = [t for t in s.lower().split() if t not in _STOPWORDS]
    return set(tuple(tokens[i: i + n]) for i in range(max(0, len(tokens) - n + 1)))


def _jaccard(a: Set, b: Set) -> float:
    """Jaccard similarity between two sets."""
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union > 0 else 0.0


def _fuzzy_match(name_a: str, name_b: str) -> float:
    """Return Jaccard similarity between two person names using trigram shingles."""
    sh_a = _shingles(name_a)
    sh_b = _shingles(name_b)

    # Fallback: also check unigrams if trigrams give 0 (very short names)
    if not sh_a or not sh_b:
        tokens_a = set(name_a.lower().split()) - _STOPWORDS
        tokens_b = set(name_b.lower().split()) - _STOPWORDS
        return _jaccard(tokens_a, tokens_b)

    return _jaccard(sh_a, sh_b)


def _find_fuzzy(name: str, candidates: list[str]) -> str | None:
    """Find the best fuzzy match for name in candidates. Returns matched name or None."""
    best_score = 0.0
    best_match = None
    for c in candidates:
        score = _fuzzy_match(name, c)
        if score > best_score:
            best_score = score
            best_match = c
    if best_score >= FUZZY_MATCH_THRESHOLD:
        return best_match
    return None


def _date_delta_days(d1: str | None, d2: str | None) -> int | None:
    """Return absolute day difference between two date strings (YYYY-MM-DD). None if unparseable."""
    if not d1 or not d2:
        return None
    try:
        dt1 = datetime.strptime(d1[:10], "%Y-%m-%d")
        dt2 = datetime.strptime(d2[:10], "%Y-%m-%d")
        return abs((dt1 - dt2).days)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main reconciliation
# ─────────────────────────────────────────────────────────────────────────────

def run(ingestion: GovernanceIngestion) -> GovernanceReconciliation:
    formal = ingestion.formal_governance_snapshot
    current = ingestion.current_governance_snapshot

    conflicts: list[ConflictItem] = []

    # Canonical name lists
    formal_board_names = [m.person_name for m in formal.board_members]
    formal_exec_names = [m.person_name for m in formal.executive_members]
    current_board_names = [m.person_name for m in current.board_members]
    current_exec_names = [m.person_name for m in current.executive_members]

    all_formal_names = formal_board_names + formal_exec_names
    all_current_names = current_board_names + current_exec_names

    # ── 1. missing_person ─────────────────────────────────────────────────────

    for name in all_formal_names:
        match = _find_fuzzy(name, all_current_names)
        if not match:
            conflicts.append(ConflictItem(
                conflict_type="missing_person",
                person_name=name,
                formal_value="present",
                current_value="absent",
                resolution_rule_applied="trust_current_for_recency",
                confidence=0.7,
            ))

    for name in all_current_names:
        match = _find_fuzzy(name, all_formal_names)
        if not match:
            conflicts.append(ConflictItem(
                conflict_type="missing_person",
                person_name=name,
                formal_value="absent",
                current_value="present",
                resolution_rule_applied="flag_as_new_appointment",
                confidence=0.65,
            ))

    # ── 2. title_mismatch ─────────────────────────────────────────────────────

    formal_exec_map = {m.person_name: m for m in formal.executive_members}
    for current_exec in current.executive_members:
        matched_name = _find_fuzzy(current_exec.person_name, list(formal_exec_map.keys()))
        if matched_name:
            formal_exec = formal_exec_map[matched_name]
            if (
                formal_exec.formal_title
                and current_exec.formal_title
                and formal_exec.formal_title.lower().strip() != current_exec.formal_title.lower().strip()
            ):
                conflicts.append(ConflictItem(
                    conflict_type="title_mismatch",
                    person_name=current_exec.person_name,
                    formal_value=formal_exec.formal_title,
                    current_value=current_exec.formal_title,
                    resolution_rule_applied="prefer_current_for_freshness_with_flag",
                    confidence=0.6,
                ))

    # ── 3. organ_mismatch (major) ─────────────────────────────────────────────

    # Person in formal board but in current exec (or vice versa)
    for name in formal_board_names:
        if _find_fuzzy(name, current_exec_names):
            conflicts.append(ConflictItem(
                conflict_type="organ_mismatch",
                person_name=name,
                formal_value="board",
                current_value="executive",
                resolution_rule_applied="escalate_to_major_conflict",
                confidence=0.8,
            ))

    for name in formal_exec_names:
        if _find_fuzzy(name, current_board_names):
            conflicts.append(ConflictItem(
                conflict_type="organ_mismatch",
                person_name=name,
                formal_value="executive",
                current_value="board",
                resolution_rule_applied="escalate_to_major_conflict",
                confidence=0.8,
            ))

    # ── 4. term_mismatch ─────────────────────────────────────────────────────

    formal_board_map = {m.person_name: m for m in formal.board_members}
    for current_member in current.board_members:
        matched_name = _find_fuzzy(current_member.person_name, list(formal_board_map.keys()))
        if matched_name:
            formal_member = formal_board_map[matched_name]
            delta = _date_delta_days(formal_member.term_start, current_member.term_start)
            if delta is not None and delta > 90:
                conflicts.append(ConflictItem(
                    conflict_type="term_mismatch",
                    person_name=current_member.person_name,
                    formal_value=formal_member.term_start,
                    current_value=current_member.term_start,
                    resolution_rule_applied="prefer_formal_for_registry_dates",
                    confidence=0.55,
                ))

    # ── 5. independence_mismatch ──────────────────────────────────────────────

    for current_member in current.board_members:
        matched_name = _find_fuzzy(current_member.person_name, list(formal_board_map.keys()))
        if matched_name:
            formal_member = formal_board_map[matched_name]
            fi = formal_member.independence_status
            ci = current_member.independence_status
            if fi != "unknown" and ci != "unknown" and fi != ci:
                conflicts.append(ConflictItem(
                    conflict_type="independence_mismatch",
                    person_name=current_member.person_name,
                    formal_value=fi,
                    current_value=ci,
                    resolution_rule_applied="prefer_formal_as_regulatory_source",
                    confidence=0.75,
                ))

    # ── Status determination ──────────────────────────────────────────────────

    has_organ_mismatch = any(c.conflict_type == "organ_mismatch" for c in conflicts)

    if not formal.board_members and not formal.executive_members:
        status = "current_only"
    elif not current.board_members and not current.executive_members:
        status = "formal_only"
    elif has_organ_mismatch:
        status = "major_conflicts"
    elif conflicts:
        status = "minor_conflicts"
    else:
        status = "clean"

    # ── Reconciled snapshot ───────────────────────────────────────────────────
    # Strategy: start from formal as canonical baseline;
    # overlay current executive members (fresher); keep formal board unless current_only.

    if status == "current_only":
        reconciled = current.model_copy(deep=True)
    else:
        reconciled = formal.model_copy(deep=True)
        # Prefer current executives when they exist (more up-to-date roles/titles)
        if current.executive_members:
            reconciled.executive_members = current.executive_members
        # Merge committees: formal as base, add any committee from current not in formal
        formal_committee_names = {c.committee_name.lower() for c in formal.committees}
        for committee in current.committees:
            if committee.committee_name.lower() not in formal_committee_names:
                reconciled.committees.append(committee)

    logger.info(
        f"[n1b] Reconciliation: status={status}, "
        f"conflicts={len(conflicts)} "
        f"(organ={sum(1 for c in conflicts if c.conflict_type == 'organ_mismatch')},"
        f" missing={sum(1 for c in conflicts if c.conflict_type == 'missing_person')})"
    )

    return GovernanceReconciliation(
        reconciliation_status=status,
        conflict_items=conflicts,
        reconciled_governance_snapshot=reconciled,
        reporting_basis={
            "formal_basis_date": formal.as_of_date,
            "current_basis_date": current.as_of_date,
            "preferred_view_for_reporting": "reconciled",
        },
    )

