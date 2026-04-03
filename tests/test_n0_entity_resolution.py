import pytest
from unittest.mock import MagicMock
from peopledd.models.contracts import InputPayload
from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.services.connectors import CVMConnector, RIConnector, ConnectorResult
from peopledd.nodes.n0_entity_resolution import run


def test_n0_resolves_correctly():
    # Setup mocks
    cvm_mock = MagicMock(spec=CVMConnector)
    ri_mock = MagicMock(spec=RIConnector)

    cvm_mock.lookup_company.return_value = ConnectorResult(
        ok=True,
        payload={
            "resolved_name": "Itaú Unibanco Holding S.A.",
            "legal_name": "Itaú Unibanco Holding S.A.",
            "cod_cvm": "19348",
            "cnpj": "60.872.504/0001-23",
            "tickers": ["ITUB4", "ITUB3"],
            "site_ri": "https://ri.itau.com.br",
            "listed": True,
            "ambiguous": False
        }
    )

    ri_mock.resolve_ri_url.return_value = ConnectorResult(
        ok=True, payload={"ri_url": "https://ri.itau.com.br"}
    )

    # Input payload with high confidence hint
    payload = InputPayload(company_name="Itau Unibanco", cnpj_hint="60872504000123")
    
    result = run(payload, cvm_mock, ri_mock)
    
    assert result.resolution_status == ResolutionStatus.RESOLVED
    assert result.resolution_confidence == 0.95
    assert result.company_mode == CompanyMode.LISTED_BR
    assert result.cnpj == "60.872.504/0001-23"


def test_n0_handles_ambiguous():
    cvm_mock = MagicMock(spec=CVMConnector)
    ri_mock = MagicMock(spec=RIConnector)

    cvm_mock.lookup_company.return_value = ConnectorResult(
        ok=True,
        payload={
            "ambiguous": True,
            "candidates": [
                {"legal_name": "Empresa A S.A.", "cnpj": "000", "cod_cvm": "1"},
                {"legal_name": "Empresa A Participações", "cnpj": "111", "cod_cvm": "2"}
            ]
        }
    )

    ri_mock.resolve_ri_url.return_value = ConnectorResult(ok=True, payload={"ri_url": "ri://none"})

    payload = InputPayload(company_name="Empresa A")
    result = run(payload, cvm_mock, ri_mock)

    assert result.resolution_status == ResolutionStatus.AMBIGUOUS
    assert result.candidate_entities is not None
    assert len(result.candidate_entities) == 2


def test_n0_not_found():
    cvm_mock = MagicMock(spec=CVMConnector)
    ri_mock = MagicMock(spec=RIConnector)

    cvm_mock.lookup_company.return_value = ConnectorResult(
        ok=False, payload={}, degradation="cvm_not_found"
    )
    ri_mock.resolve_ri_url.return_value = ConnectorResult(ok=True, payload={"ri_url": "ri://none"})

    payload = InputPayload(company_name="Startup Secreta")
    result = run(payload, cvm_mock, ri_mock)

    assert result.resolution_status == ResolutionStatus.NOT_FOUND
    assert result.company_mode == CompanyMode.PRIVATE_OR_UNRESOLVED
