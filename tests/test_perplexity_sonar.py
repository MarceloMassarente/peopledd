from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from peopledd.services import perplexity_sonar


@pytest.fixture(autouse=True)
def clear_perplexity_env(monkeypatch):
    monkeypatch.delenv("PERPLEXITY_API_KEY", raising=False)
    monkeypatch.delenv("PERPLEXITY_SONAR_DISABLE", raising=False)


def test_fetch_returns_empty_without_key():
    out = asyncio.run(perplexity_sonar.fetch_sonar_briefs_pair("Acme SA", sector="Bancos", country="BR"))
    assert out == []


def test_fetch_two_roles_via_call_sonar(monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key")

    async def fake_call(
        user_prompt: str,
        *,
        budget_step: str,
        model: str,
        search_recency_filter: str | None,
    ):
        if "18 meses" in user_prompt:
            return ("Fato recente um.", ("https://example.com/news",))
        return ("Risco setorial típico.", ("https://example.com/sector",))

    with patch.object(perplexity_sonar, "_call_sonar", side_effect=fake_call):
        out = asyncio.run(perplexity_sonar.fetch_sonar_briefs_pair("Acme SA", sector="Varejo", country="BR"))

    assert len(out) == 2
    roles = {o.role for o in out}
    assert roles == {"recent_company_facts", "sector_governance_context"}
    by_role = {o.role: o for o in out}
    assert "Fato" in by_role["recent_company_facts"].body
    assert by_role["recent_company_facts"].citation_urls == ("https://example.com/news",)
    assert "Risco" in by_role["sector_governance_context"].body


def test_perplexity_sonar_enabled_respects_disable(monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "x")
    assert perplexity_sonar.perplexity_sonar_enabled() is True
    monkeypatch.setenv("PERPLEXITY_SONAR_DISABLE", "1")
    assert perplexity_sonar.perplexity_sonar_enabled() is False
