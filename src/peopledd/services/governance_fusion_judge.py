from __future__ import annotations

import asyncio
import json
import logging
import unicodedata
import uuid
from typing import Any

from peopledd.models.common import SourceRef
from peopledd.models.contracts import (
    BoardMember,
    ExecutiveMember,
    GovernanceCandidate,
    GovernanceFusionDecision,
    GovernanceFusionQuality,
    GovernanceObservation,
    GovernanceReconciliation,
    GovernanceSnapshot,
    FusionUnresolvedItem,
    SemanticGovernanceFusion,
)
from peopledd.vendor.search import _llm_json

logger = logging.getLogger(__name__)

_FUZZY_THRESHOLD = 0.58

_STOPWORDS = frozenset({"de", "da", "do", "dos", "das", "e", "a", "o"})


def _ascii_fold(s: str) -> str:
    nfd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfd if not unicodedata.combining(c))


def _name_tokens(s: str) -> set[str]:
    a = _ascii_fold(s).lower()
    return {t for t in a.split() if t and t not in _STOPWORDS}


def _fuzzy_name_score(a: str, b: str) -> float:
    ta, tb = _name_tokens(a), _name_tokens(b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def _find(parent: list[int], i: int) -> int:
    while parent[i] != i:
        parent[i] = parent[parent[i]]
        i = parent[i]
    return i


def _union(parent: list[int], i: int, j: int) -> None:
    ri, rj = _find(parent, i), _find(parent, j)
    if ri != rj:
        parent[ri] = rj


def cluster_observations(observations: list[GovernanceObservation]) -> list[GovernanceCandidate]:
    """Transitive clustering by fuzzy name match (cross-organ)."""
    n = len(observations)
    if n == 0:
        return []
    parent = list(range(n))
    for i in range(n):
        for j in range(i + 1, n):
            s = _fuzzy_name_score(observations[i].observed_name, observations[j].observed_name)
            if s >= _FUZZY_THRESHOLD:
                _union(parent, i, j)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        r = _find(parent, i)
        groups.setdefault(r, []).append(i)
    candidates: list[GovernanceCandidate] = []
    for idxs in groups.values():
        obs_ids = [observations[i].observation_id for i in idxs]
        names = [observations[i].observed_name for i in idxs]
        key_tokens = sorted(_name_tokens(names[0]))[:3]
        blocking_key = "_".join(key_tokens) if key_tokens else "anon"
        candidates.append(
            GovernanceCandidate(
                candidate_id=f"cand_{uuid.uuid4().hex[:10]}",
                observation_ids=obs_ids,
                blocking_key=blocking_key,
            )
        )
    return candidates


def _track_rank(track: str) -> int:
    order = {
        "formal_fre": 4,
        "current_ri": 3,
        "current_private_web": 2,
        "profile_evidence": 1,
        "other": 0,
    }
    return order.get(track, 0)


def rule_based_fusion(
    observations: list[GovernanceObservation],
    candidates: list[GovernanceCandidate],
) -> list[GovernanceFusionDecision]:
    """Deterministic fusion when LLM is unavailable."""
    by_id = {o.observation_id: o for o in observations}
    decisions: list[GovernanceFusionDecision] = []
    for cand in candidates:
        obs_list = [by_id[i] for i in cand.observation_ids if i in by_id]
        if not obs_list:
            continue
        sorted_obs = sorted(
            obs_list,
            key=lambda o: (_track_rank(o.source_track), o.source_confidence),
            reverse=True,
        )
        best = sorted_obs[0]
        support = [o.observation_id for o in sorted_obs]
        discarded: list[str] = []
        if len(sorted_obs) > 1:
            discarded = [o.observation_id for o in sorted_obs[1:]]
        organ: str = "unknown"
        for o in sorted_obs:
            if o.organ != "unknown":
                organ = o.organ
                break
        if organ == "unknown":
            organ = best.organ if best.organ != "unknown" else "unknown"
        status = "resolved" if organ != "unknown" else "partial"
        conf = min(0.95, 0.5 + 0.08 * len(support))
        if len(sorted_obs) > 1 and len({o.organ for o in sorted_obs if o.organ != "unknown"}) > 1:
            status = "ambiguous"
            conf = min(conf, 0.55)
        if len(sorted_obs) == 1:
            if best.source_track == "formal_fre":
                rationale_code = "dominant_formal"
            elif best.source_track in ("current_ri", "current_private_web"):
                rationale_code = "dominant_current"
            else:
                rationale_code = "rule_fallback"
        else:
            rationale_code = "merged_sources"
        decisions.append(
            GovernanceFusionDecision(
                decision_id=f"dec_{uuid.uuid4().hex[:10]}",
                candidate_id=cand.candidate_id,
                canonical_name=best.observed_name,
                normalized_role=best.observed_role,
                organ=organ,  # type: ignore[arg-type]
                decision_status=status,  # type: ignore[arg-type]
                confidence=round(conf, 3),
                supporting_observation_ids=support,
                discarded_observation_ids=discarded,
                decision_rationale_code=rationale_code,  # type: ignore[arg-type]
                decision_rationale_detail="Deterministic merge by source track and name cluster.",
            )
        )
    return decisions


_FUSION_JUDGE_SYSTEM = """Voce e um juiz de fusao de governanca corporativa.
Dado grupos de observacoes (nomes/cargos/orgaos de fontes formais FRE, RI, web, perfil),
decida UMA linha consolidada por grupo.
Regras:
- Prefira fonte formal_fre para datas e independencia quando aplicavel.
- RI e web sao melhores para cargos atuais.
- Se houver conflito forte de orgao (board vs executive) sem evidencia clara, marque ambiguous.
- Nunca invente pessoas: use apenas observation_ids fornecidos.
Responda apenas JSON no schema solicitado."""

_FUSION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "decisions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "candidate_id": {"type": "string"},
                    "canonical_name": {"type": "string"},
                    "normalized_role": {"type": "string"},
                    "organ": {
                        "type": "string",
                        "enum": ["board", "executive", "committee", "fiscal_council", "unknown"],
                    },
                    "decision_status": {
                        "type": "string",
                        "enum": ["resolved", "partial", "ambiguous", "rejected"],
                    },
                    "confidence": {"type": "number"},
                    "supporting_observation_ids": {"type": "array", "items": {"type": "string"}},
                    "discarded_observation_ids": {"type": "array", "items": {"type": "string"}},
                    "decision_rationale_code": {
                        "type": "string",
                        "enum": [
                            "dominant_formal",
                            "dominant_current",
                            "merged_sources",
                            "insufficient_evidence",
                            "llm_judge",
                            "rule_fallback",
                        ],
                    },
                    "decision_rationale_detail": {"type": "string"},
                },
                "required": [
                    "candidate_id",
                    "canonical_name",
                    "normalized_role",
                    "organ",
                    "decision_status",
                    "confidence",
                    "supporting_observation_ids",
                    "discarded_observation_ids",
                    "decision_rationale_code",
                    "decision_rationale_detail",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["decisions"],
    "additionalProperties": False,
}


def _pack_user_payload(
    candidates: list[GovernanceCandidate],
    observations: list[GovernanceObservation],
) -> str:
    by_id = {o.observation_id: o for o in observations}
    rows: list[dict[str, Any]] = []
    for c in candidates:
        obs_rows = []
        for oid in c.observation_ids:
            o = by_id.get(oid)
            if not o:
                continue
            obs_rows.append(
                {
                    "observation_id": o.observation_id,
                    "observed_name": o.observed_name,
                    "observed_role": o.observed_role or "",
                    "organ": o.organ,
                    "source_track": o.source_track,
                    "source_confidence": o.source_confidence,
                    "snippet": (o.evidence_span.snippet if o.evidence_span else "") or "",
                }
            )
        rows.append({"candidate_id": c.candidate_id, "observations": obs_rows})
    return json.dumps({"candidates": rows}, ensure_ascii=False)


def llm_judge_fusion(
    observations: list[GovernanceObservation],
    candidates: list[GovernanceCandidate],
) -> list[GovernanceFusionDecision] | None:
    if not candidates:
        return []
    user = _pack_user_payload(candidates, observations)
    try:
        raw = asyncio.run(
            _llm_json(
                system=_FUSION_JUDGE_SYSTEM,
                user=user,
                model="gpt-5.4-mini",
                schema=_FUSION_SCHEMA,
                timeout=60.0,
                budget_step="governance_fusion_judge",
            )
        )
    except RuntimeError:
        logger.warning("[governance_fusion_judge] asyncio.run unavailable; skipping LLM judge")
        raw = None
    if not raw or "decisions" not in raw:
        return None
    out: list[GovernanceFusionDecision] = []
    for row in raw["decisions"]:
        try:
            out.append(
                GovernanceFusionDecision(
                    decision_id=f"dec_{uuid.uuid4().hex[:10]}",
                    candidate_id=str(row["candidate_id"]),
                    canonical_name=str(row["canonical_name"]),
                    normalized_role=str(row.get("normalized_role") or "") or None,
                    organ=row["organ"],
                    decision_status=row["decision_status"],
                    confidence=float(row.get("confidence", 0.6)),
                    supporting_observation_ids=list(row.get("supporting_observation_ids") or []),
                    discarded_observation_ids=list(row.get("discarded_observation_ids") or []),
                    decision_rationale_code=row["decision_rationale_code"],
                    decision_rationale_detail=str(row.get("decision_rationale_detail") or "") or None,
                )
            )
        except Exception as e:
            logger.warning("[governance_fusion_judge] skip bad LLM row: %s", e)
    expected = {c.candidate_id for c in candidates}
    got = {d.candidate_id for d in out}
    if expected != got or len(out) != len(candidates):
        return None
    return out


def build_resolved_snapshot(
    decisions: list[GovernanceFusionDecision],
    observations: list[GovernanceObservation],
    reconciled: GovernanceReconciliation,
) -> GovernanceSnapshot:
    base = reconciled.reconciled_governance_snapshot.model_copy(deep=True)
    board: list[BoardMember] = []
    execs: list[ExecutiveMember] = []
    obs_by_id = {o.observation_id: o for o in observations}
    for d in decisions:
        if d.decision_status in ("rejected",):
            continue
        src_refs: list[SourceRef] = []
        for oid in d.supporting_observation_ids:
            o = obs_by_id.get(oid)
            if o:
                src_refs.append(o.source_ref)
        if d.organ == "board" and d.decision_status in ("resolved", "partial", "ambiguous"):
            board.append(
                BoardMember(
                    person_name=d.canonical_name,
                    source_refs=src_refs,
                )
            )
        elif d.organ == "executive" and d.decision_status in ("resolved", "partial", "ambiguous"):
            execs.append(
                ExecutiveMember(
                    person_name=d.canonical_name,
                    formal_title=d.normalized_role or "Diretor",
                    normalized_role="other",
                )
            )
    if board:
        base.board_members = board
    if execs:
        base.executive_members = execs
    return base


def build_unresolved_items(decisions: list[GovernanceFusionDecision]) -> list[FusionUnresolvedItem]:
    items: list[FusionUnresolvedItem] = []
    for d in decisions:
        if d.decision_status == "ambiguous":
            items.append(
                FusionUnresolvedItem(
                    item_id=f"un_{uuid.uuid4().hex[:8]}",
                    kind="name_collision",
                    detail=f"Ambiguous fusion for {d.canonical_name}",
                    related_observation_ids=d.supporting_observation_ids + d.discarded_observation_ids,
                )
            )
        elif d.decision_status == "rejected":
            items.append(
                FusionUnresolvedItem(
                    item_id=f"un_{uuid.uuid4().hex[:8]}",
                    kind="rejected_low_trust",
                    detail=d.decision_rationale_detail or "rejected",
                    related_observation_ids=d.supporting_observation_ids,
                )
            )
        elif d.decision_status == "resolved" and d.confidence < 0.48:
            items.append(
                FusionUnresolvedItem(
                    item_id=f"un_{uuid.uuid4().hex[:8]}",
                    kind="insufficient_evidence",
                    detail="Low confidence resolved decision",
                    related_observation_ids=d.supporting_observation_ids,
                )
            )
    return items


def fusion_quality_from_decisions(
    decisions: list[GovernanceFusionDecision],
    obs_count: int,
    cand_count: int,
    *,
    llm_used: bool,
    judge_passes: int,
    profile_rounds: int,
) -> GovernanceFusionQuality:
    if any(d.decision_status == "ambiguous" for d in decisions):
        overall = "major_gaps"
    elif any(d.confidence < 0.55 for d in decisions) or any(
        d.decision_status == "partial" for d in decisions
    ):
        overall = "minor_gaps"
    else:
        overall = "clean"
    return GovernanceFusionQuality(
        observation_count=obs_count,
        candidate_count=cand_count,
        llm_judge_used=llm_used,
        judge_passes=judge_passes,
        profile_evidence_rounds=profile_rounds,
        overall_status=overall,
    )


def fuse_observations(
    observations: list[GovernanceObservation],
    candidates: list[GovernanceCandidate],
    reconciliation: GovernanceReconciliation,
    *,
    prefer_llm: bool = True,
    profile_rounds: int = 0,
) -> tuple[list[GovernanceFusionDecision], GovernanceSnapshot, GovernanceFusionQuality, bool]:
    llm_used = False
    judge_passes = 0
    decisions: list[GovernanceFusionDecision] = []
    if prefer_llm:
        llm_decisions = llm_judge_fusion(observations, candidates)
        if llm_decisions is not None:
            decisions = llm_decisions
            llm_used = True
            judge_passes = 1
    if not decisions:
        decisions = rule_based_fusion(observations, candidates)
        judge_passes = 1
    snap = build_resolved_snapshot(decisions, observations, reconciliation)
    quality = fusion_quality_from_decisions(
        decisions,
        len(observations),
        len(candidates),
        llm_used=llm_used,
        judge_passes=judge_passes,
        profile_rounds=profile_rounds,
    )
    return decisions, snap, quality, llm_used


def run_semantic_fusion(
    observations: list[GovernanceObservation],
    reconciliation: GovernanceReconciliation,
    *,
    prefer_llm: bool = True,
) -> tuple[
    list[GovernanceCandidate],
    list[GovernanceFusionDecision],
    GovernanceSnapshot,
    GovernanceFusionQuality,
    bool,
]:
    candidates = cluster_observations(observations)
    decisions, snap, quality, llm_used = fuse_observations(
        observations, candidates, reconciliation, prefer_llm=prefer_llm, profile_rounds=0
    )
    return candidates, decisions, snap, quality, llm_used


def merge_profile_observations(
    base: list[GovernanceObservation],
    extra: list[GovernanceObservation],
) -> list[GovernanceObservation]:
    return base + extra
