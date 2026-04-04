from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime

from peopledd.models.contracts import (
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
)
from peopledd.models.common import SourceRef
from peopledd.services.cvm_client import CVMClient
from peopledd.services.fre_parser import FREParser
from peopledd.services.ri_scraper import RIScraper

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _current_year() -> int:
    return datetime.now().year


def _pick_best_fre_year(available_years: list[int]) -> int | None:
    """Choose the most recent FRE year available (<= current year)."""
    now = _current_year()
    valid = [y for y in available_years if y <= now]
    return max(valid) if valid else None


def _formal_completeness(snapshot: GovernanceSnapshot) -> float:
    score = 0.0
    if snapshot.board_members:
        score += 0.5
    if snapshot.executive_members:
        score += 0.35
    if snapshot.committees:
        score += 0.15
    return round(score, 2)


def _current_completeness(snapshot: GovernanceSnapshot) -> float:
    score = 0.0
    if snapshot.board_members:
        score += 0.55
    if snapshot.executive_members:
        score += 0.30
    if snapshot.committees:
        score += 0.15
    return round(score, 2)


def _freshness_score(as_of_date: str | None) -> float:
    if not as_of_date:
        return 0.0
    from datetime import timedelta
    try:
        dt = datetime.strptime(as_of_date[:10], "%Y-%m-%d")
        delta = datetime.now() - dt
        if delta.days < 180:
            return 1.0
        if delta.days < 540:
            return 0.5
        return 0.2
    except ValueError:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Main node
# ─────────────────────────────────────────────────────────────────────────────

def run(
    company_name: str,
    cnpj: str | None = None,
    ri_url: str | None = None,
    fre_extended_probe: bool = False,
) -> GovernanceIngestion:
    """
    Dual-track governance ingestion.

    Track A (formal): CVM FRE structured data → FREParser
    Track B (current): RI page scrape → ScrapeOrchestratorV5 → LLM extraction

    Degrades gracefully: if one track fails, uses the other.
    """
    formal, formal_meta = _ingest_formal(cnpj)
    current = _ingest_current(ri_url, company_name)

    quality = GovernanceDataQuality(
        formal_completeness=_formal_completeness(formal),
        current_completeness=_current_completeness(current),
        freshness_score=max(
            _freshness_score(formal.as_of_date),
            _freshness_score(current.as_of_date),
        ),
    )

    ingestion_metadata: dict[str, str | None] = {**formal_meta}
    if ri_url:
        ingestion_metadata["ri_scrape_url"] = ri_url

    return GovernanceIngestion(
        formal_governance_snapshot=formal,
        current_governance_snapshot=current,
        governance_data_quality=quality,
        ingestion_metadata=ingestion_metadata,
    )


def _ingest_formal(cnpj: str | None) -> tuple[GovernanceSnapshot, dict[str, str | None]]:
    """Track A: download + parse CVM FRE ZIP. Returns snapshot + metadata for n8 provenance."""
    if not cnpj:
        logger.warning("[n1] No CNPJ available — skipping formal track")
        return GovernanceSnapshot(), {}

    client = CVMClient()

    # Probe last 3 years (current may not be released yet); optional extended backfill for recovery policy
    cy = _current_year()
    years_to_try = [cy - 1, cy, cy - 2]
    if fre_extended_probe:
        years_to_try = years_to_try + [cy - 3, cy - 4, cy - 5]

    for year in years_to_try:
        try:
            meta = asyncio.run(client.get_fre_metadata(cnpj, year))
            if not meta:
                continue

            zip_bytes = asyncio.run(client.download_fre_zip(meta.url_zip))
            if not zip_bytes:
                continue

            parser = FREParser(cnpj=cnpj, source_url=meta.url_zip)
            snapshot = parser.parse(zip_bytes)

            if snapshot.board_members or snapshot.executive_members:
                logger.info(f"[n1] FRE track A: {len(snapshot.board_members)} board, "
                            f"{len(snapshot.executive_members)} exec from {year}")
                return snapshot, {
                    "fre_source_url": meta.url_zip,
                    "fre_year": str(year),
                }

        except Exception as e:
            logger.warning(f"[n1] FRE {year} failed: {e}")
            continue

    logger.warning("[n1] All FRE years exhausted — formal track empty")
    return GovernanceSnapshot(), {}


def _ingest_current(ri_url: str | None, company_name: str) -> GovernanceSnapshot:
    """Track B: scrape RI page and extract governance via LLM."""
    if not ri_url:
        logger.warning("[n1] No RI URL available — skipping current track")
        return GovernanceSnapshot()

    try:
        scraper = RIScraper(
            browserless_endpoint=os.environ.get("BROWSERLESS_ENDPOINT"),
            browserless_token=os.environ.get("BROWSERLESS_TOKEN"),
        )
        snapshot = scraper.scrape_board(ri_url, company_name)
        logger.info(f"[n1] RI track B: {len(snapshot.board_members)} board, "
                    f"{len(snapshot.executive_members)} exec from {ri_url}")
        return snapshot
    except Exception as e:
        logger.error(f"[n1] RI track B failed: {e}")
        return GovernanceSnapshot()

