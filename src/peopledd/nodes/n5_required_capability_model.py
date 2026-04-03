from __future__ import annotations

"""
n5_required_capability_model — builds required capability model
from sector baseline + real strategy overlay (from n4).

Strategy-to-capability mapping:
  - financial challenge → amplify capital_allocation + financas_performance
  - technology/digital priority → amplify transformacao_tecnologia
  - regulatory challenge → amplify governanca_risco_compliance
  - people challenge → add lideranca_pessoas to executive requirements
  - M&A/expansion → add gestao_crescimento_inorganico
"""

import logging
from typing import Literal

from peopledd.models.contracts import RequiredCapability, RequiredCapabilityModel, StrategyChallenges
from peopledd.services.sector_baseline import get_baseline

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Strategy-to-capability overlay maps
# ─────────────────────────────────────────────────────────────────────────────

_CHALLENGE_OVERLAYS: dict[str, dict] = {
    "financial": {
        "dimension": "capital_allocation",
        "required_level": 4,
        "importance_weight": 0.30,
        "origin": "challenge_overlay",
        "rationale": "Desafio financeiro requer capacidade de alocação de capital",
    },
    "technology": {
        "dimension": "transformacao_tecnologia",
        "required_level": 4,
        "importance_weight": 0.25,
        "origin": "challenge_overlay",
        "rationale": "Desafio tecnológico requer capacidade de transformação digital",
    },
    "regulatory": {
        "dimension": "governanca_risco_compliance",
        "required_level": 5,
        "importance_weight": 0.30,
        "origin": "challenge_overlay",
        "rationale": "Desafio regulatório requer alta capacidade de GRC",
    },
    "governance": {
        "dimension": "governanca_risco_compliance",
        "required_level": 5,
        "importance_weight": 0.35,
        "origin": "challenge_overlay",
        "rationale": "Desafio de governança requer capacidade de GRC elevada",
    },
    "people": {
        "dimension": "lideranca_pessoas",
        "required_level": 4,
        "importance_weight": 0.20,
        "origin": "challenge_overlay",
        "rationale": "Desafio de pessoas requer liderança e gestão de talentos",
    },
    "operational": {
        "dimension": "execucao_operacional",
        "required_level": 4,
        "importance_weight": 0.25,
        "origin": "challenge_overlay",
        "rationale": "Desafio operacional requer excelência em execução",
    },
    "market": {
        "dimension": "desenvolvimento_negocio",
        "required_level": 4,
        "importance_weight": 0.20,
        "origin": "challenge_overlay",
        "rationale": "Desafio de mercado requer capacidade de desenvolvimento de negócio",
    },
}

_PRIORITY_OVERLAYS: dict[str, dict] = {
    "M&A": {
        "dimension": "gestao_crescimento_inorganico",
        "required_level": 4,
        "importance_weight": 0.20,
        "origin": "strategy_overlay",
        "rationale": "Prioridade de M&A requer capacidade de gestão de aquisições",
    },
    "digital": {
        "dimension": "transformacao_tecnologia",
        "required_level": 4,
        "importance_weight": 0.22,
        "origin": "strategy_overlay",
        "rationale": "Transformação digital é prioridade estratégica",
    },
    "expansão": {
        "dimension": "desenvolvimento_negocio",
        "required_level": 4,
        "importance_weight": 0.18,
        "origin": "strategy_overlay",
        "rationale": "Expansão geográfica requer capacidade de desenvolvimento de negócio",
    },
    "sustentabilidade": {
        "dimension": "esg_sustentabilidade",
        "required_level": 3,
        "importance_weight": 0.12,
        "origin": "strategy_overlay",
        "rationale": "Estratégia ESG requer capacidade de sustentabilidade",
    },
    "custos": {
        "dimension": "eficiencia_operacional",
        "required_level": 4,
        "importance_weight": 0.18,
        "origin": "strategy_overlay",
        "rationale": "Foco em eficiência/custos requer capacidade operacional forte",
    },
}


def _normalize_weights(rows: list[dict]) -> list[dict]:
    total = sum(r["importance_weight"] for r in rows) or 1.0
    return [{**r, "importance_weight": round(r["importance_weight"] / total, 4)} for r in rows]


def _merge_overlays(
    base: list[dict], overlays: list[dict]
) -> list[dict]:
    """Merge overlay items into base, bumping required_level if dimension already exists."""
    dim_index: dict[str, int] = {r["dimension"]: i for i, r in enumerate(base)}
    result = list(base)
    for ov in overlays:
        dim = ov["dimension"]
        if dim in dim_index:
            idx = dim_index[dim]
            existing = result[idx]
            # Bump required_level to max; bump weight by overlay weight * 0.5
            result[idx] = {
                **existing,
                "required_level": max(existing["required_level"], ov["required_level"]),
                "importance_weight": existing["importance_weight"] + ov["importance_weight"] * 0.5,
            }
        else:
            result.append(ov)
            dim_index[dim] = len(result) - 1
    return result


def run(sector: str, strategy: StrategyChallenges) -> RequiredCapabilityModel:
    # ── 1. Sector baseline ─────────────────────────────────────────────────────
    baseline_raw = get_baseline(sector) or [
        {"dimension": "governanca_risco_compliance", "required_level": 4, "importance_weight": 0.34},
        {"dimension": "capital_allocation", "required_level": 4, "importance_weight": 0.33},
        {"dimension": "transformacao_tecnologia", "required_level": 3, "importance_weight": 0.33},
    ]

    # ── 2. Compute overlays from actual strategy ───────────────────────────────
    challenge_overlays_to_apply: list[dict] = []
    for challenge in strategy.key_challenges:
        ct = challenge.challenge_type
        if ct in _CHALLENGE_OVERLAYS and challenge.confidence >= 0.5:
            ov = dict(_CHALLENGE_OVERLAYS[ct])
            # Scale importance by challenge severity
            severity_scale = {"high": 1.3, "medium": 1.0, "low": 0.7}
            ov["importance_weight"] *= severity_scale.get(challenge.severity, 1.0)
            challenge_overlays_to_apply.append(ov)

    priority_overlays_to_apply: list[dict] = []
    for priority in strategy.strategic_priorities:
        ptext_lower = priority.priority.lower()
        for keyword, ov_template in _PRIORITY_OVERLAYS.items():
            if keyword in ptext_lower and priority.confidence >= 0.5:
                ov = dict(ov_template)
                ov["importance_weight"] *= min(1.5, 0.8 + priority.confidence)
                priority_overlays_to_apply.append(ov)
                break

    # ── 3. Merge and normalize board capabilities ──────────────────────────────
    board_merged = _merge_overlays(baseline_raw, priority_overlays_to_apply + challenge_overlays_to_apply)
    board_normalized = _normalize_weights(board_merged)

    board = [
        RequiredCapability(
            dimension=item["dimension"],
            required_level=item["required_level"],
            importance_weight=item["importance_weight"],
            origin=item.get("origin", "sector_baseline"),  # type: ignore
            rationale=item.get("rationale", "Baseline setorial SPEC v1.1"),
        )
        for item in board_normalized
    ]

    # ── 4. Executive capabilities — operationally driven ─────────────────────

    # Base executive always needs: execucao_operacional + financas_performance
    exec_base = [
        {
            "dimension": "execucao_operacional",
            "required_level": 4,
            "importance_weight": 0.40,
            "origin": "strategy_overlay",
            "rationale": "Entrega de prioridades estratégicas requer execução operacional",
        },
        {
            "dimension": "financas_performance",
            "required_level": 4,
            "importance_weight": 0.35,
            "origin": "challenge_overlay",
            "rationale": "Desafios financeiros observados requerem gestão de performance",
        },
    ]

    # Add exec overlays from high-severity challenges
    exec_overlays = []
    for challenge in strategy.key_challenges:
        if challenge.severity == "high" and challenge.confidence >= 0.6:
            ct = challenge.challenge_type
            if ct == "people":
                exec_overlays.append({
                    "dimension": "lideranca_pessoas",
                    "required_level": 4,
                    "importance_weight": 0.25,
                    "origin": "challenge_overlay",
                    "rationale": "Desafio crítico de pessoas exige liderança organizacional",
                })
            elif ct == "technology":
                exec_overlays.append({
                    "dimension": "transformacao_tecnologia",
                    "required_level": 4,
                    "importance_weight": 0.20,
                    "origin": "challenge_overlay",
                    "rationale": "Desafio tecnológico crítico requer liderança tech",
                })

    exec_merged = _merge_overlays(exec_base, exec_overlays)
    exec_normalized = _normalize_weights(exec_merged)

    executive = [
        RequiredCapability(
            dimension=item["dimension"],
            required_level=item["required_level"],
            importance_weight=item["importance_weight"],
            origin=item.get("origin", "strategy_overlay"),  # type: ignore
            rationale=item.get("rationale", ""),
        )
        for item in exec_normalized
    ]

    logger.info(
        f"[n5] Built capability model: "
        f"{len(board)} board dims, {len(executive)} exec dims "
        f"(sector={sector}, challenges={len(strategy.key_challenges)})"
    )

    return RequiredCapabilityModel(
        board_required_capabilities=board,
        executive_required_capabilities=executive,
    )

