from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConnectorResult:
    ok: bool
    payload: dict
    degradation: str | None = None


class CVMConnector:
    def lookup_company(self, name: str) -> ConnectorResult:
        # Stub com retorno determinístico para desenvolvimento local.
        payload = {
            "resolved_name": name,
            "legal_name": name,
            "cod_cvm": None,
            "cnpj": None,
            "tickers": [],
            "listed": False,
        }
        return ConnectorResult(ok=True, payload=payload)


class RIConnector:
    def resolve_ri_url(self, company_name: str) -> ConnectorResult:
        guessed = f"https://ri.{company_name.lower().replace(' ', '')}.com.br"
        return ConnectorResult(ok=True, payload={"ri_url": guessed})


class HarvestConnector:
    def resolve_person(self, name: str, context: dict) -> ConnectorResult:
        if not name:
            return ConnectorResult(ok=False, payload={}, degradation="person_name_missing")
        payload = {
            "canonical_name": name,
            "matched_profiles": [{"provider": "public_web", "profile_id_or_url": "stub", "match_confidence": 0.55}],
        }
        return ConnectorResult(ok=True, payload=payload)
