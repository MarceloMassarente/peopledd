from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

from peopledd.models.contracts import BoardMember, GovernanceSnapshot
from peopledd.runtime.source_attempt import (
    SourceAttemptResult,
    classify_scrape_exception,
    primary_ri_failure_mode,
)
from peopledd.services.ri_scraper import RIScraper
from peopledd.vendor.scraper import ScrapeResult


def test_primary_ri_failure_mode_prefers_llm_extract() -> None:
    attempts = [
        SourceAttemptResult(True, None, "https://x", 200, "httpx", 10.0),
        SourceAttemptResult(False, "budget_exhausted", "https://x", 200, "llm_extract", 5.0),
    ]
    assert primary_ri_failure_mode(attempts) == "budget_exhausted"


def test_primary_ri_failure_mode_first_network() -> None:
    attempts = [
        SourceAttemptResult(False, "not_found", "https://x", 0, "httpx", 1.0),
    ]
    assert primary_ri_failure_mode(attempts) == "not_found"


def test_attempts_json_roundtrip() -> None:
    a = SourceAttemptResult(
        success=False,
        failure_mode="low_content",
        source_url="https://ri.example",
        content_words=5,
        strategy_used="httpx",
        latency_ms=12.5,
        governance_found=False,
        error_detail="x",
    )
    s = SourceAttemptResult.attempts_json([a])
    row = json.loads(s)[0]
    assert row["failure_mode"] == "low_content"
    assert row["success"] is False


def test_classify_timeout() -> None:
    assert classify_scrape_exception(TimeoutError()) == "timeout"
    assert classify_scrape_exception(asyncio.TimeoutError()) == "timeout"


def test_scrape_board_returns_tuple_with_mocked_chain() -> None:
    """RIScraper returns (snapshot, attempts) without live HTTP."""
    rich = "# " + "word " * 120
    scrape_ok = ScrapeResult(
        success=True,
        content=rich,
        url="https://ri.test/",
        strategy="httpx",
        status_code=200,
        raw_html="<html><body>governança</body></html>",
    )

    snap = GovernanceSnapshot(
        board_members=[BoardMember(person_name="Alice", source_refs=[])],
    )

    extract_attempt = SourceAttemptResult(
        success=True,
        failure_mode=None,
        source_url="https://ri.test/",
        content_words=len(rich.split()),
        strategy_used="llm_extract",
        latency_ms=1.0,
        governance_found=True,
    )

    with patch("peopledd.services.ri_scraper.MultiStrategyScraper") as MS:
        inst = MagicMock()
        inst.scrape_url = AsyncMock(return_value=scrape_ok)
        MS.return_value = inst
        scraper = RIScraper()

        async def fake_extract(_content: str, _company: str, _url: str):
            return snap, extract_attempt

        scraper._extract_governance_with_attempt = fake_extract  # type: ignore[method-assign]

        out_snap, attempts = asyncio.run(scraper.scrape_board_async("https://ri.test/", "Co"))

    assert len(out_snap.board_members) == 1
    assert attempts[-1].strategy_used == "llm_extract"
    assert attempts[-1].success is True
