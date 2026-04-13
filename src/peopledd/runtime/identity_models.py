from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from peopledd.models.contracts import GovernanceSnapshot


@dataclass
class PersonIdentityCandidate:
    """Progressive person resolution context (Harvest / Exa profile rows as duck-typed objects)."""

    observed_name: str
    target_company: str | None
    expected_organ: str
    positive_signals: list[str] = field(default_factory=list)
    negative_signals: list[str] = field(default_factory=list)
    candidates: list[Any] = field(default_factory=list)
    resolution_round: int = 1


def expected_organ_for_person(snapshot: GovernanceSnapshot, person_name: str) -> str:
    for m in snapshot.board_members:
        if m.person_name == person_name:
            return "board"
    for e in snapshot.executive_members:
        if e.person_name == person_name:
            return "executive"
    return "unknown"


def _scores_close(top: Any, second: Any) -> bool:
    ts = float(getattr(top, "name_similarity", 0.0))
    ss = float(getattr(second, "name_similarity", 0.0))
    return ss >= 0.7 and abs(ts - ss) < 0.1


def _headline_matches_organ(headline: str | None, organ: str) -> bool:
    h = (headline or "").lower()
    if organ == "board":
        return any(
            k in h
            for k in (
                "board",
                "conselho",
                "chair",
                "administra",
                "director",
                "membro",
            )
        )
    if organ == "executive":
        return any(
            k in h
            for k in (
                "ceo",
                "cfo",
                "coo",
                "cto",
                "chief",
                "diretor",
                "vp",
                "president",
            )
        )
    return False


def resolve_profile_candidates(
    observed_name: str,
    target_company: str | None,
    expected_organ: str,
    candidates: Sequence[Any],
) -> tuple[list[Any], int, list[str]]:
    """
    Multi-round disambiguation: name (pre-sorted list) -> company_match -> headline/organ hints.
    Returns (ordered candidates to treat as primary list, resolution_round, negative_signals).
    """
    negatives: list[str] = []
    pool = list(candidates)
    if not pool:
        return [], 0, negatives

    rnd = 1
    if target_company:
        tc = target_company.strip().lower()
        for c in pool:
            cc = (getattr(c, "current_company", None) or "").strip().lower()
            if (
                cc
                and tc[:8] not in cc
                and not bool(getattr(c, "company_match", False))
                and float(getattr(c, "name_similarity", 0.0)) >= 0.65
            ):
                negatives.append("company_mismatch")

    if len(pool) == 1:
        return pool, rnd, negatives

    top, second = pool[0], pool[1]
    ambiguous = len(pool) >= 2 and _scores_close(top, second)

    if ambiguous and target_company:
        rnd = 2
        matched = [c for c in pool if bool(getattr(c, "company_match", False))]
        if len(matched) == 1:
            return matched, rnd, negatives
        if matched:
            pool = matched

    if len(pool) >= 2 and _scores_close(pool[0], pool[1]):
        rnd = max(rnd, 3)
        if expected_organ in ("board", "executive"):
            hits = [c for c in pool if _headline_matches_organ(getattr(c, "headline", None), expected_organ)]
            if len(hits) == 1:
                return hits, rnd, negatives
        negatives.append("linkedin_homonym_ambiguous")
        return pool, rnd, negatives

    return [pool[0]], rnd, negatives
