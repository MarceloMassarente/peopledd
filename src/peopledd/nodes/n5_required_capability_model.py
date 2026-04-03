from __future__ import annotations

from peopledd.models.contracts import RequiredCapability, RequiredCapabilityModel, StrategyChallenges
from peopledd.services.sector_baseline import get_baseline


def _normalize_weights(rows: list[dict]) -> list[dict]:
    total = sum(r["importance_weight"] for r in rows) or 1.0
    return [{**r, "importance_weight": r["importance_weight"] / total} for r in rows]


def run(sector: str, strategy: StrategyChallenges) -> RequiredCapabilityModel:
    baseline = _normalize_weights(get_baseline(sector) or [
        {"dimension": "governanca_risco_compliance", "required_level": 4, "importance_weight": 0.34},
        {"dimension": "capital_allocation", "required_level": 4, "importance_weight": 0.33},
        {"dimension": "transformacao_tecnologia", "required_level": 3, "importance_weight": 0.33},
    ])

    board = [
        RequiredCapability(
            dimension=item["dimension"],
            required_level=item["required_level"],
            importance_weight=item["importance_weight"],
            origin="sector_baseline",
            rationale="Baseline setorial do SPEC v1.1",
        )
        for item in baseline
    ]

    executive = [
        RequiredCapability(
            dimension="execucao_operacional",
            required_level=4,
            importance_weight=0.5,
            origin="strategy_overlay",
            rationale="Necessário para entrega de prioridades estratégicas",
        ),
        RequiredCapability(
            dimension="financas_performance",
            required_level=4,
            importance_weight=0.5,
            origin="challenge_overlay",
            rationale="Necessário para desafios financeiros observados",
        ),
    ]

    return RequiredCapabilityModel(
        board_required_capabilities=board,
        executive_required_capabilities=executive,
    )
