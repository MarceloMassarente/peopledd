from __future__ import annotations

from peopledd.models.contracts import CoverageScoring, ImprovementHypothesis, StrategyChallenges


def run(coverage: CoverageScoring, strategy: StrategyChallenges, analytical_confidence: float) -> list[ImprovementHypothesis]:
    hypotheses: list[ImprovementHypothesis] = []

    for idx, item in enumerate(coverage.board_coverage, start=1):
        if item.gap_severity == "high":
            conf = min(0.75, analytical_confidence)
            hypotheses.append(
                ImprovementHypothesis(
                    hypothesis_id=f"H{idx}",
                    category="recrutamento",
                    title=f"Reforçar competência em {item.dimension}",
                    problem_statement=f"Gap alto identificado em {item.dimension}",
                    evidence_basis=[item.rationale],
                    proposed_action="Abrir trilha de busca para conselheiro independente com experiência comprovada",
                    expected_benefit="Redução do risco de concentração e maior robustez decisória",
                    urgency="high",
                    confidence=conf,
                    non_triviality_score=0.7,
                )
            )

    if analytical_confidence < 0.7 and hypotheses:
        return hypotheses[:1]
    return hypotheses
