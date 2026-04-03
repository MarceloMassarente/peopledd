from __future__ import annotations

from peopledd.models.contracts import KeyChallenge, StrategicPriority, StrategyChallenges
from peopledd.models.common import SourceRef


def run(company_name: str) -> StrategyChallenges:
    ref = SourceRef(source_type="ri", label="Documento RI", url_or_ref="ri://strategy/stub")
    return StrategyChallenges(
        strategic_priorities=[
            StrategicPriority(
                priority=f"Eficiência operacional e crescimento sustentável em {company_name}",
                time_horizon="medium",
                confidence=0.58,
                source_refs=[ref],
            )
        ],
        key_challenges=[
            KeyChallenge(
                challenge="Balancear disciplina de capital com agenda de transformação",
                challenge_type="financial",
                severity="medium",
                confidence=0.55,
                source_refs=[ref],
            )
        ],
        recent_triggers=["Sem eventos recentes confiáveis no modo stub"],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.52},
    )
