from __future__ import annotations

from unittest.mock import AsyncMock, patch

from peopledd.services.harvest_adapter import HarvestAdapter


def test_harvest_adapter_pipeline_cache_roundtrip_search(tmp_path):
    db = tmp_path / "pipeline.sqlite"
    adapter = HarvestAdapter(api_key="k", pipeline_cache_db_path=str(db))

    fake_payload = {
        "elements": [
            {
                "linkedinUrl": "https://www.linkedin.com/in/u-one",
                "firstName": "U",
                "lastName": "One",
                "headline": "CFO",
                "location": {},
                "currentPositions": [{"companyName": "Co"}],
            }
        ]
    }

    mock_get = AsyncMock(return_value=fake_payload)
    with patch("peopledd.services.harvest_adapter._harvest_get", new=mock_get):
        r1 = adapter.search_by_name("U One", company="Co")
        r2 = adapter.search_by_name("U One", company="Co")

    assert len(r1.candidates) >= 1
    assert len(r2.candidates) >= 1
    assert mock_get.await_count == 1
    assert db.exists()
