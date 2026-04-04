from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from peopledd.services import person_sourcing
from peopledd.vendor.search import SearchOrchestrator, SearchResult


def test_linkedin_profile_urls_async_exa():
    orch = SearchOrchestrator(searxng_url=None, exa_api_key="fake-key-for-init")

    async def fake_search(*_a, **_kw):
        return [
            SearchResult(
                url="https://www.linkedin.com/in/jane-doe-exec",
                title="Jane Doe",
                snippet="",
                source="exa",
            )
        ]

    async def fake_company(*_a, **_kw):
        return []

    orch.exa.search_people_linkedin_async = fake_search  # type: ignore[method-assign]
    orch.exa.search_company_rich_async = fake_company  # type: ignore[method-assign]

    urls = asyncio.run(
        person_sourcing.linkedin_profile_urls_async(orch, "Jane Doe", "Acme Corp")
    )
    assert len(urls) == 1
    assert "linkedin.com/in/jane-doe-exec" in urls[0]


def test_linkedin_profile_urls_llm_picks_one_when_two_candidates():
    orch = SearchOrchestrator(searxng_url=None, exa_api_key="fake-key-for-init")

    async def fake_search(*_a, **_kw):
        return [
            SearchResult(url="https://www.linkedin.com/in/wrong-person", title="Other", snippet="x", source="exa"),
            SearchResult(
                url="https://www.linkedin.com/in/jane-doe-exec",
                title="Jane Doe",
                snippet="CFO Acme",
                source="exa",
            ),
        ]

    async def fake_company(*_a, **_kw):
        return [
            SearchResult(
                url="https://ri.example.com/gov",
                title="Governance",
                snippet="Jane Doe CFO",
                source="exa",
            )
        ]

    orch.exa.search_people_linkedin_async = fake_search  # type: ignore[method-assign]
    orch.exa.search_company_rich_async = fake_company  # type: ignore[method-assign]

    async def fake_llm(*_a, **_kw):
        return {
            "linkedin_url": "https://www.linkedin.com/in/jane-doe-exec",
            "confidence": 0.92,
            "reason": "Name and role align with company context",
        }

    with patch("peopledd.vendor.search._llm_json", new=AsyncMock(side_effect=fake_llm)):
        urls = asyncio.run(
            person_sourcing.linkedin_profile_urls_async(orch, "Jane Doe", "Acme Corp")
        )
    assert len(urls) == 1
    assert "jane-doe-exec" in urls[0].lower()


def test_linkedin_profile_urls_none_orchestrator():
    assert person_sourcing.linkedin_profile_urls(None, "A", "B") == []


def test_harvest_style_results_from_urls_builds_profile_search_results():
    urls = ["https://www.linkedin.com/in/test-user"]
    results = person_sourcing.harvest_style_results_from_urls(urls, "Test User", "Co")
    assert len(results) == 1
    assert results[0].linkedin_url == "https://www.linkedin.com/in/test-user"
    assert results[0].name_similarity >= 0.99
