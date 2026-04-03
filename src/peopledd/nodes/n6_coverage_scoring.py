from __future__ import annotations

from statistics import mean

from peopledd.models.contracts import CoverageItem, CoverageScoring, PersonProfile, RequiredCapabilityModel


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


def run(required: RequiredCapabilityModel, profiles: list[PersonProfile], board_size: int, executive_size: int) -> CoverageScoring:
    board_conf = _organ_confidence(profiles)

    board_items: list[CoverageItem] = []
    for cap in required.board_required_capabilities:
        top_score = min(5.0, 2.5 + board_conf * 2)
        mean_top_k = min(5.0, top_score - 0.5)
        redundancy = 0.5 if board_size < 3 else 0.8
        diversity = 0.6
        raw = 0.35 * top_score + 0.20 * mean_top_k + 0.15 * redundancy + 0.10 * diversity + 0.20 * board_conf
        normalized = raw * _size_normalizer(board_size)
        confidence_adjusted = normalized * max(0.4, board_conf)
        ratio = min(1.0, confidence_adjusted / max(cap.required_level, 1))
        gap = "low" if ratio >= 0.8 else "medium" if ratio >= 0.55 else "high"
        board_items.append(
            CoverageItem(
                dimension=cap.dimension,
                required_level=cap.required_level,
                observed_level=round(normalized, 2),
                confidence_adjusted_level=round(confidence_adjusted, 2),
                coverage_ratio=round(ratio, 2),
                gap_severity=gap,
                single_point_of_failure=board_size <= 2,
                rationale="Cobertura agregada com normalização por tamanho do órgão",
            )
        )

    flags = []
    if board_size <= 2:
        flags.append("single_point_of_failure")
    if board_conf < 0.55:
        flags.append("low_confidence_dimension")

    return CoverageScoring(
        board_coverage=board_items,
        executive_coverage=[],
        organ_level_flags=flags,
    )
