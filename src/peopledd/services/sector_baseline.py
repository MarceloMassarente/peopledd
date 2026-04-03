from __future__ import annotations

SECTOR_BASELINES: dict[str, list[dict]] = {
    "mineracao": [
        {"dimension": "seguranca_operacional", "required_level": 5, "importance_weight": 0.18},
        {"dimension": "grandes_ativos", "required_level": 4, "importance_weight": 0.16},
        {"dimension": "regulacao_licenciamento", "required_level": 4, "importance_weight": 0.14},
        {"dimension": "capital_allocation", "required_level": 4, "importance_weight": 0.17},
        {"dimension": "relacoes_institucionais", "required_level": 4, "importance_weight": 0.17},
        {"dimension": "supply_logistica", "required_level": 3, "importance_weight": 0.18},
    ],
    "bancos": [
        {"dimension": "risco_credito", "required_level": 5, "importance_weight": 0.2},
        {"dimension": "capital_liquidez", "required_level": 5, "importance_weight": 0.2},
        {"dimension": "compliance_regulatorio", "required_level": 5, "importance_weight": 0.2},
        {"dimension": "tecnologia_dados", "required_level": 4, "importance_weight": 0.2},
        {"dimension": "crescimento_cliente", "required_level": 4, "importance_weight": 0.2},
    ],
}


def get_baseline(sector: str) -> list[dict]:
    return SECTOR_BASELINES.get(sector, [])
