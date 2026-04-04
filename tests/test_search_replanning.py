from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from peopledd.runtime.adaptive_models import FindUrlsParams
from peopledd.vendor.search import FindUrlsOutcome, PlannerOutput, SearchOrchestrator


def test_find_urls_async_respects_max_searx_queries() -> None:
    orch = SearchOrchestrator(searxng_url="http://fake", exa_api_key=None)
    orch.searxng.search_async = AsyncMock(return_value=[])
    orch.planner.plan_async = AsyncMock(
        return_value=PlannerOutput(
            web_queries=["q1", "q2", "q3"],
            exa_query="",
            preferred_domains=[],
            avoid_terms=[],
        )
    )
    fp = FindUrlsParams(max_searx_queries=3, searx_num_results=5, exa_num_results=10)

    async def _run() -> FindUrlsOutcome:
        return await orch.find_urls_async(
            "Acme",
            ri_url="https://ri.acme.com",
            sector=None,
            find_params=fp,
        )

    out = asyncio.run(_run())
    assert isinstance(out, FindUrlsOutcome)
    assert out.searxng_queries_used == 3
    assert orch.searxng.search_async.await_count == 3


def test_find_urls_sync_returns_url_list_only() -> None:
    orch = SearchOrchestrator(searxng_url=None, exa_api_key=None)

    async def _fake(*args: object, **kwargs: object) -> FindUrlsOutcome:
        return FindUrlsOutcome(urls=["https://a.com"], searxng_queries_used=0)

    orch.find_urls_async = _fake  # type: ignore[method-assign]
    urls = orch.find_urls("Acme", ri_url="https://ri.a.com")
    assert urls == ["https://a.com"]
