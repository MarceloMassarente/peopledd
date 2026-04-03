from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ConnectorResult:
    ok: bool
    payload: dict
    degradation: str | None = None


import asyncio
from peopledd.services.cvm_client import CVMClient

class CVMConnector:
    def __init__(self):
        self._client = CVMClient()

    def lookup_company(self, name: str, cnpj_hint: str | None = None, ticker_hint: str | None = None) -> ConnectorResult:
        query = cnpj_hint if cnpj_hint else name
        
        candidates = asyncio.run(self._client.search_company(query))
        
        if not candidates:
            return ConnectorResult(ok=False, payload={}, degradation="cvm_not_found")
            
        if len(candidates) == 1:
            c = candidates[0]
            payload = {
                "resolved_name": c.nome_razao_social,
                "legal_name": c.nome_razao_social,
                "cod_cvm": c.cod_cvm,
                "cnpj": c.cnpj,
                "site_ri": c.site_ri,
                "tickers": c.tickers,
                "listed": c.tipo == "CIA ABERTA",
                "ambiguous": False
            }
            return ConnectorResult(ok=True, payload=payload)
            
        # Ambiguous case
        payload = {
            "ambiguous": True,
            "candidates": [
                {
                    "legal_name": c.nome_razao_social,
                    "cnpj": c.cnpj,
                    "cod_cvm": c.cod_cvm
                }
                for c in candidates
            ]
        }
        return ConnectorResult(ok=True, payload=payload)


class RIConnector:
    """
    Resolves the Investor Relations (RI) URL for a company.

    Strategy (in order):
      1. Exa Company Search (category="company") — finds the actual RI/website
      2. Heuristic domain guess (ri.<slug>.com.br) as last resort
    """

    def __init__(self, exa_api_key: str | None = None):
        import os
        self._exa_key = exa_api_key or os.environ.get("EXA_API_KEY", "")

    def resolve_ri_url(self, company_name: str, sector: str | None = None) -> ConnectorResult:
        """
        Resolve the RI URL for a Brazilian company.
        Uses Exa Company Search when EXA_API_KEY is set; falls back to heuristic.
        """
        import asyncio
        return asyncio.run(self._resolve_async(company_name, sector))

    async def _resolve_async(self, company_name: str, sector: str | None) -> ConnectorResult:
        if self._exa_key:
            try:
                from peopledd.vendor.search import ExaProvider
                exa = ExaProvider(api_key=self._exa_key)
                profile = await exa.company_lookup_async(
                    company_name, hq_country="Brazil", sector=sector
                )
                if profile:
                    ri_url = profile.ri_url or profile.website
                    if ri_url:
                        return ConnectorResult(
                            ok=True,
                            payload={
                                "ri_url": ri_url,
                                "website": profile.website,
                                "description": profile.description,
                                "exa_score": profile.exa_score,
                                "resolution_method": "exa_company_search",
                            },
                        )
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"[RIConnector] Exa lookup failed: {e}")

        # Heuristic fallback
        slug = company_name.lower().replace(" ", "").replace("s.a.", "").replace("s/a", "").strip(".")
        guessed = f"https://ri.{slug}.com.br"
        return ConnectorResult(
            ok=True,
            payload={
                "ri_url": guessed,
                "resolution_method": "heuristic",
            },
            degradation="ri_heuristic_fallback",
        )


class HarvestConnector:
    def resolve_person(self, name: str, context: dict) -> ConnectorResult:
        if not name:
            return ConnectorResult(ok=False, payload={}, degradation="person_name_missing")
        payload = {
            "canonical_name": name,
            "matched_profiles": [{"provider": "public_web", "profile_id_or_url": "stub", "match_confidence": 0.55}],
        }
        return ConnectorResult(ok=True, payload=payload)
