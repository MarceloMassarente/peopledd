from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from peopledd.vendor.search import ExaProvider, _exa_people_result_snippet


def test_search_people_linkedin_async_uses_people_category_and_dedupes():
    exa = ExaProvider(api_key="test-key")

    responses = [
        {
            "results": [
                {
                    "url": "https://www.linkedin.com/in/alice-1",
                    "title": "Alice",
                    "text": "CEO",
                    "score": 0.9,
                },
                {
                    "url": "https://noise.example.com/p",
                    "title": "x",
                    "text": "",
                    "score": 0.99,
                },
            ]
        },
        {
            "results": [
                {
                    "url": "https://www.linkedin.com/in/alice-1/",
                    "title": "Alice B",
                    "text": "board",
                    "score": 0.95,
                },
            ]
        },
    ]
    resp_iter = iter(responses)

    async def fake_post(_url, json=None, **_kw):
        assert json is not None
        assert json.get("category") == "people"
        assert json.get("type") == "auto"
        assert "includeDomains" not in json
        assert "excludeDomains" not in json
        assert "useAutoprompt" not in json
        c = json.get("contents") or {}
        assert "highlights" in c
        assert c["highlights"].get("maxCharacters") == 4000
        assert "text" not in c
        data = next(resp_iter)
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json = MagicMock(return_value=data)
        return r

    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(side_effect=fake_post)

    async def run():
        with patch("peopledd.vendor.search.httpx.AsyncClient", return_value=mock_client):
            return await exa.search_people_linkedin_async(
                ["Alice TestCorp", "TestCorp Alice"],
                num_results_per_query=5,
                max_concurrent=2,
            )

    out = asyncio.run(run())
    assert len(out) == 1
    assert "alice-1" in out[0].url.lower()
    assert out[0].score == 0.95


def test_exa_people_result_snippet_merges_highlights_and_text():
    s = _exa_people_result_snippet(
        {
            "highlights": ["VP at Acme", "Board member"],
            "text": "Full bio paragraph.",
        }
    )
    assert "VP at Acme" in s
    assert "Board member" in s
    assert "Full bio" in s
