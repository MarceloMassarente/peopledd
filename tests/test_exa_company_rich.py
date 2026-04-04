from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from peopledd.vendor.search import ExaProvider


def test_search_company_rich_async_matches_exa_py_style_payload():
    exa = ExaProvider(api_key="test-key")

    async def fake_post(_url, json=None, **_kw):
        assert json is not None
        assert json.get("category") == "company"
        assert json.get("type") == "auto"
        assert json.get("numResults") == 10
        assert json.get("outputSchema") == {"type": "text"}
        c = json.get("contents") or {}
        assert c == {"text": {"maxCharacters": 20000}}
        assert "highlights" not in c
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json = MagicMock(
            return_value={
                "results": [
                    {
                        "url": "https://ri.example.com/gov",
                        "title": "RI",
                        "text": "Conselho: João Silva",
                        "highlights": ["João Silva conselheiro"],
                        "score": 0.8,
                    }
                ]
            }
        )
        return r

    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(side_effect=fake_post)

    async def run():
        with patch("peopledd.vendor.search.httpx.AsyncClient", return_value=mock_client):
            return await exa.search_company_rich_async(
                "conselheiros de administração JSL",
                num_results=10,
            )

    out = asyncio.run(run())
    assert len(out) == 1
    assert "João" in out[0].snippet or "João" in out[0].title


def test_search_company_rich_async_retries_on_400_for_text_true():
    exa = ExaProvider(api_key="test-key")
    calls: list[dict] = []
    attempt = 0

    async def fake_post(_url, json=None, **_kw):
        nonlocal attempt
        attempt += 1
        calls.append(json)
        if attempt == 1:
            req = httpx.Request("POST", "https://api.exa.ai/search")
            resp = httpx.Response(400, request=req)
            raise httpx.HTTPStatusError("bad request", request=req, response=resp)
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json = MagicMock(
            return_value={"results": [{"url": "https://x.com", "title": "t", "text": "body", "score": 0.5}]}
        )
        return r

    mock_client = MagicMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(side_effect=fake_post)

    async def run():
        with patch("peopledd.vendor.search.httpx.AsyncClient", return_value=mock_client):
            return await exa.search_company_rich_async("Acme Corp governance", num_results=5)

    out = asyncio.run(run())
    assert len(out) == 1
    assert len(calls) == 2
    assert calls[1]["contents"]["text"] == {"maxCharacters": 10000}
