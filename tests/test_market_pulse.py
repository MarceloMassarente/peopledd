from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

from peopledd.models.contracts import CanonicalEntity, StrategyChallenges
from peopledd.services.harvest_adapter import HarvestAdapter
from peopledd.services.market_pulse_retriever import (
    _build_queries,
    _dedup_search_results,
    retrieve_market_pulse_async,
    run_sync,
)
from peopledd.vendor.search import SearchResult


def test_build_queries_standard_vs_deep():
    ent = "Empresa X"
    q_std = _build_queries(ent, [], "standard")
    assert len(q_std) == 2
    q_deep = _build_queries(ent, ["SUZB3"], "deep")
    assert len(q_deep) == 4
    assert any("SUZB3" in q for q in q_deep)


def test_build_queries_deep_without_ticker():
    q_deep = _build_queries("Y", [], "deep")
    assert len(q_deep) == 4
    assert any("M&A" in x or "joint venture" in x for x in q_deep)


def test_dedup_search_results_same_path():
    a = SearchResult(url="https://a.com/x", title="t1", snippet="s", source="exa")
    b = SearchResult(url="https://a.com/x", title="t2", snippet="s2", source="searxng")
    out = _dedup_search_results([a, b])
    assert len(out) == 1


def test_run_sync_no_api_keys():
    orch = MagicMock()
    orch.exa.api_key = ""
    orch.searxng.base_url = ""
    entity = CanonicalEntity(entity_id="e1", input_company_name="Co")
    pulse = run_sync(
        orch,
        company_name="Co",
        entity=entity,
        strategy=StrategyChallenges(),
        analysis_depth="standard",
    )
    assert pulse.skipped_reason == "no_api_keys"
    assert pulse.claims == []


def test_retrieve_market_pulse_async_llm_ok():
    orch = MagicMock()
    orch.exa.api_key = "exa-key"
    orch.searxng.base_url = ""

    hit = SearchResult(
        url="https://news.example/a",
        title="Lucro sobe",
        snippet="Empresa reportou resultado.",
        source="exa",
        published_date="2025-01-01",
    )

    async def fake_exa(q, num_results=10, category="auto", include_domains=None):
        return [hit]

    orch.exa.search_async = fake_exa

    fake_payload = {
        "claims": [
            {
                "statement": "Midia menciona resultado da empresa.",
                "topic": "earnings",
                "sentiment": "positive",
                "confidence": 0.7,
                "source_urls": ["https://news.example/a"],
                "alignment_with_ri": "unknown",
            }
        ]
    }

    class FakeChoice:
        message = MagicMock(content=json.dumps(fake_payload))

    class FakeResp:
        choices = [FakeChoice()]

    fake_client = MagicMock()
    fake_client.chat.completions.create = AsyncMock(return_value=FakeResp())

    entity = CanonicalEntity(entity_id="e1", input_company_name="Co", resolved_name="Co SA")
    strategy = StrategyChallenges()

    with patch(
        "peopledd.services.market_pulse_retriever.try_consume_llm_call",
        return_value=True,
    ):
        with patch(
            "peopledd.services.market_pulse_retriever._get_openai_client",
            return_value=fake_client,
        ):
            pulse = asyncio.run(
                retrieve_market_pulse_async(
                    orch,
                    company_name="Co",
                    entity=entity,
                    strategy=strategy,
                    analysis_depth="standard",
                )
            )

    assert pulse.skipped_reason is None
    assert len(pulse.claims) == 1
    assert pulse.claims[0].statement.startswith("Midia")
    assert pulse.claims[0].source_urls == ["https://news.example/a"]
    assert len(pulse.source_hits) >= 1


def test_retrieve_budget_skips_llm():
    orch = MagicMock()
    orch.exa.api_key = "k"
    orch.searxng.base_url = ""

    async def fake_exa(*_a, **_kw):
        return [
            SearchResult(
                url="https://x.com/1",
                title="t",
                snippet="s",
                source="exa",
            )
        ]

    orch.exa.search_async = fake_exa

    entity = CanonicalEntity(entity_id="e1", input_company_name="Co")

    with patch(
        "peopledd.services.market_pulse_retriever.try_consume_llm_call",
        return_value=False,
    ):
        with patch("peopledd.services.market_pulse_retriever.record_llm_route"):
            pulse = asyncio.run(
                retrieve_market_pulse_async(
                    orch,
                    company_name="Co",
                    entity=entity,
                    strategy=StrategyChallenges(),
                    analysis_depth="standard",
                )
            )

    assert pulse.skipped_reason == "budget_exhausted"
    assert pulse.claims == []
    assert pulse.source_hits


def test_harvest_industry_experience_from_company_name():
    adapter = HarvestAdapter(api_key="")
    compact = {
        "experience": [
            {
                "position": "Diretor Financeiro",
                "company": "Banco do Brasil",
                "is_current": True,
            },
            {
                "position": "Analyst",
                "company": "Industria de Celulose Exemplo SA",
                "is_current": False,
            },
        ]
    }
    summary = adapter.build_career_summary(compact)
    tags = summary["industry_experience"]
    assert "financial_services" in tags
    assert "pulp_paper" in tags
