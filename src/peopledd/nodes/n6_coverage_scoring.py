from __future__ import annotations

"""
n6_coverage_scoring — maps PersonProfile career data to capability coverage.

Key improvements over stub:
  - Per-dimension scoring uses functional_experience from real LinkedIn profiles
  - Evidence-weighted: profiles with higher profile_confidence get more weight
  - Single-point-of-failure: detected when only 1 person covers a critical dimension
  - Governance signals used to boost governanca_risco_compliance coverage
"""

import logging
from statistics import mean

from peopledd.models.contracts import CoverageItem, CoverageScoring, PersonProfile, RequiredCapabilityModel

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Functional experience → capability dimension mapping
# ─────────────────────────────────────────────────────────────────────────────

_FUNC_TO_DIMENSION: dict[str, list[str]] = {
    "financial": ["capital_allocation", "financas_performance"],
    "cfo": ["capital_allocation", "financas_performance"],
    "technology": ["transformacao_tecnologia"],
    "cto": ["transformacao_tecnologia"],
    "cio": ["transformacao_tecnologia"],
    "operations": ["execucao_operacional", "eficiencia_operacional"],
    "coo": ["execucao_operacional"],
    "people_hr": ["lideranca_pessoas"],
    "chro": ["lideranca_pessoas"],
    "legal_compliance": ["governanca_risco_compliance"],
    "legal": ["governanca_risco_compliance"],
    "commercial_marketing": ["desenvolvimento_negocio"],
    "cmo": ["desenvolvimento_negocio"],
    "strategy": ["gestao_crescimento_inorganico", "desenvolvimento_negocio"],
    "general_management": ["execucao_operacional", "governanca_risco_compliance"],
    "ceo": ["execucao_operacional", "governanca_risco_compliance"],
}

_GOV_SIGNALS_TO_DIMENSION: dict[str, list[str]] = {
    "board_experience": ["governanca_risco_compliance"],
    "audit_committee": ["governanca_risco_compliance", "capital_allocation"],
    "executive_track": ["execucao_operacional"],
}


def _organ_confidence(profiles: list[PersonProfile]) -> float:
    if not profiles:
        return 0.0
    return mean(p.profile_quality.profile_confidence for p in profiles)


def _size_normalizer(n: int) -> float:
    if n <= 0:
        return 0.6
    if n <= 5:
        return 1.0
    return max(0.75, 1 - ((n - 5) * 0.03))


def _build_dimension_coverage(
    profiles: list[PersonProfile],
) -> dict[str, dict]:
    """
    Build per-dimension coverage from actual profile data.

    Returns: {dimension: {"persons": int, "weighted_score": float, "confidence": float}}
    """
    dim_data: dict[str, list[float]] = {}

    for profile in profiles:
        profile_conf = profile.profile_quality.profile_confidence
        functional = profile.career_summary.get("functional_experience", [])
        governance_signals = profile.career_summary.get("governance_signals", [])

        # Map functional experience to dimensions
        covered_dims: set[str] = set()
        for func in functional:
            for dim in _FUNC_TO_DIMENSION.get(func, []):
                covered_dims.add(dim)

        # Map governance signals to dimensions
        for signal in governance_signals:
            for dim in _GOV_SIGNALS_TO_DIMENSION.get(signal, []):
                covered_dims.add(dim)

        # Apply profile confidence as weight
        for dim in covered_dims:
            if dim not in dim_data:
                dim_data[dim] = []
            dim_data[dim].append(profile_conf)

    result: dict[str, dict] = {}
    for dim, confidences in dim_data.items():
        n_persons = len(confidences)
        # Weighted score: highest confidence + diminishing returns for additional coverage
        sorted_conf = sorted(confidences, reverse=True)
        weighted = sorted_conf[0]
        for i, c in enumerate(sorted_conf[1:], 1):
            weighted += c * (0.5 ** i)  # diminishing returns
        weighted = min(5.0, weighted * 5.0)  # scale to [0, 5]

        result[dim] = {
            "persons": n_persons,
            "weighted_score": round(weighted, 2),
            "mean_confidence": round(mean(confidences), 3),
        }

    return result


def run(
    required: RequiredCapabilityModel,
    profiles: list[PersonProfile],
    board_size: int,
    executive_size: int,
) -> CoverageScoring:
    board_conf = _organ_confidence(profiles)
    dim_coverage = _build_dimension_coverage(profiles)
    size_norm = _size_normalizer(board_size)

    # ── Board coverage ─────────────────────────────────────────────────────────
    board_items: list[CoverageItem] = []
    for cap in required.board_required_capabilities:
        dim = cap.dimension
        dim_data = dim_coverage.get(dim)

        if dim_data:
            # Evidence-backed score
            raw_score = dim_data["weighted_score"]
            n_persons = dim_data["persons"]
            dim_conf = dim_data["mean_confidence"]
            rationale = f"{n_persons} pessoas cobrindo dimensão '{dim}' (conf. média={dim_conf:.2f})"
            spof = n_persons == 1 and cap.required_level >= 4
        else:
            # No evidence — fall back to proxy from organ confidence
            raw_score = min(5.0, 2.0 + board_conf * 2)
            n_persons = 0
            rationale = f"Sem evidência direta; proxy por conf. orgão={board_conf:.2f}"
            spof = board_size <= 2

        # Apply size normalization and organ confidence  
        normalized = raw_score * size_norm
        confidence_adjusted = normalized * max(0.35, board_conf)
        ratio = min(1.0, confidence_adjusted / max(cap.required_level, 1))
        gap = "low" if ratio >= 0.80 else "medium" if ratio >= 0.55 else "high"

        board_items.append(CoverageItem(
            dimension=dim,
            required_level=cap.required_level,
            observed_level=round(normalized, 2),
            confidence_adjusted_level=round(confidence_adjusted, 2),
            coverage_ratio=round(ratio, 2),
            gap_severity=gap,
            single_point_of_failure=spof,
            rationale=rationale,
        ))

    # ── Executive coverage ─────────────────────────────────────────────────────
    exec_items: list[CoverageItem] = []
    exec_conf = _organ_confidence(profiles)  # use same pool for now
    exec_dim_cov = _build_dimension_coverage(profiles)
    exec_size_norm = _size_normalizer(executive_size)

    for cap in required.executive_required_capabilities:
        dim = cap.dimension
        dim_data = exec_dim_cov.get(dim)

        if dim_data:
            raw_score = dim_data["weighted_score"]
            n_persons = dim_data["persons"]
            dim_conf = dim_data["mean_confidence"]
            rationale = f"{n_persons} exec. cobrindo '{dim}' (conf.={dim_conf:.2f})"
            spof = n_persons == 1 and cap.required_level >= 4
        else:
            raw_score = min(5.0, 2.0 + exec_conf * 2)
            rationale = f"Sem evidência direta; proxy conf.={exec_conf:.2f}"
            spof = executive_size <= 1

        normalized = raw_score * exec_size_norm
        confidence_adjusted = normalized * max(0.35, exec_conf)
        ratio = min(1.0, confidence_adjusted / max(cap.required_level, 1))
        gap = "low" if ratio >= 0.80 else "medium" if ratio >= 0.55 else "high"

        exec_items.append(CoverageItem(
            dimension=dim,
            required_level=cap.required_level,
            observed_level=round(normalized, 2),
            confidence_adjusted_level=round(confidence_adjusted, 2),
            coverage_ratio=round(ratio, 2),
            gap_severity=gap,
            single_point_of_failure=spof,
            rationale=rationale,
        ))

    # ── Flags ──────────────────────────────────────────────────────────────────
    flags: list[str] = []
    if board_size <= 2:
        flags.append("single_point_of_failure")
    if board_conf < 0.55:
        flags.append("low_confidence_dimension")
    if any(item.gap_severity == "high" for item in board_items):
        flags.append("critical_gap_detected")
    if any(item.single_point_of_failure for item in board_items + exec_items):
        flags.append("spof_in_critical_dimension")

    logger.info(
        f"[n6] Coverage: {len(board_items)} board dims, {len(exec_items)} exec dims, "
        f"flags={flags}, board_conf={board_conf:.2f}"
    )

    return CoverageScoring(
        board_coverage=board_items,
        executive_coverage=exec_items,
        organ_level_flags=flags,
    )

