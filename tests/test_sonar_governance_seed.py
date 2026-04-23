from __future__ import annotations

from peopledd.services import sonar_governance_seed


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, *args, **kwargs):
        return _FakeResponse(self._payload)


def test_fetch_governance_seed_parses_json(monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key")
    payload = {
        "choices": [
            {
                "message": {
                    "content": """
{
  "ri_url": "https://ri.example.com",
  "board_members": [{"person_name":"Ana", "role_or_title":"Conselheira", "evidence_url":"https://ri.example.com/ca"}],
  "executive_members": [{"person_name":"Bruno", "role_or_title":"CEO", "evidence_url":"https://ri.example.com/diretoria"}],
  "confidence": 0.74
}
"""
                }
            }
        ],
        "citations": ["https://ri.example.com/governanca"],
    }

    monkeypatch.setattr(
        sonar_governance_seed.httpx,
        "Client",
        lambda **kwargs: _FakeClient(payload),
    )
    monkeypatch.setattr(sonar_governance_seed, "try_consume_llm_call", lambda step: True)

    seed = sonar_governance_seed.fetch_governance_seed("Sabesp")
    assert seed is not None
    assert seed.ri_url_candidate == "https://ri.example.com"
    assert len(seed.board_members) == 1
    assert len(seed.executive_members) == 1
    assert seed.confidence == 0.74


def test_fetch_governance_seed_returns_none_when_empty(monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key")
    payload = {"choices": [{"message": {"content": "{}"}}], "citations": []}
    monkeypatch.setattr(
        sonar_governance_seed.httpx,
        "Client",
        lambda **kwargs: _FakeClient(payload),
    )
    monkeypatch.setattr(sonar_governance_seed, "try_consume_llm_call", lambda step: True)

    seed = sonar_governance_seed.fetch_governance_seed("Acme")
    assert seed is None
