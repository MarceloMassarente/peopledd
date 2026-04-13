from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from peopledd.models.contracts import (
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
)
from peopledd.services.cvm_client import CVMClient
from peopledd.services.fre_parser import FREParser
from peopledd.services.governance_completeness import (
    current_track_completeness,
    formal_track_completeness,
    merge_governance_snapshots,
)
from peopledd.services.private_governance_discovery import (
    discover_governance,
    eligible_for_private_web_discovery,
)
from peopledd.runtime.pipeline_context import get_attached_run_context
from peopledd.runtime.source_attempt import (
    SourceAttemptResult,
    classify_scrape_exception,
    primary_ri_failure_mode,
)
from peopledd.runtime.source_memory import CompanyMemory, company_key_for
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
    *,
    company_mode: str | None = None,
    search_orchestrator: Any | None = None,
    website_hint: str | None = None,
    country: str = "BR",
    enable_private_web_discovery: bool = True,
    trace_ri_attempt: Callable[[SourceAttemptResult], None] | None = None,
) -> GovernanceIngestion:
    """
    Dual-track governance ingestion.

    Track A (formal): CVM FRE structured data → FREParser
    Track B (current): RI page scrape → LLM extraction
    Track B fallback: when board/exec are still empty, Exa company + web LLM + Exa people
    (`private_governance_discovery`) fills current_governance_snapshot.

    Degrades gracefully: if one track fails, uses the other.

    company_mode is reserved for future policy (e.g. stricter gating); ingestion uses empty current + Exa availability.
    """
    logger.debug("[n1] company_mode=%s enable_private_web=%s", company_mode, enable_private_web_discovery)
    formal, formal_meta = _ingest_formal(cnpj)
    current, ri_track_meta = _ingest_current(
        ri_url, company_name, country=country, trace_ri=trace_ri_attempt
    )

    used_private = False
    private_meta: dict[str, str | None] = {}
    prior_current = current
    if eligible_for_private_web_discovery(
        current_snapshot=current,
        search_orchestrator=search_orchestrator,
        enabled=enable_private_web_discovery,
        company_mode=company_mode,
        formal_snapshot=formal,
    ):
        snap, private_meta = discover_governance(
            search_orchestrator,
            company_name,
            country=country,
            website_hint=website_hint,
        )
        if snap.board_members or snap.executive_members or snap.committees:
            had_prior_people = bool(
                prior_current.board_members or prior_current.executive_members
            )
            if had_prior_people:
                current = merge_governance_snapshots(prior_current, snap)
            else:
                current = snap
            used_private = True
            logger.info(
                "[n1] Private web discovery filled current track (board=%d exec=%d committees=%d merge=%s)",
                len(current.board_members),
                len(current.executive_members),
                len(current.committees),
                had_prior_people,
            )

    quality = GovernanceDataQuality(
        formal_completeness=formal_track_completeness(formal),
        current_completeness=current_track_completeness(current),
        freshness_score=max(
            _freshness_score(formal.as_of_date),
            _freshness_score(current.as_of_date),
        ),
    )

    ingestion_metadata: dict[str, str | None] = {**formal_meta, **ri_track_meta}
    if ri_url:
        ingestion_metadata["ri_scrape_url"] = ri_url
    if used_private:
        ingestion_metadata["private_web_discovery"] = "1"
        ingestion_metadata["private_web_anchor_website"] = private_meta.get("anchor_website")
        ingestion_metadata["private_web_reason"] = private_meta.get("reason")
        ingestion_metadata["private_web_source_count"] = private_meta.get("source_count")

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


def _ri_preferred_surfaces(company_name: str, country: str) -> list[str] | None:
    ctx = get_attached_run_context()
    if ctx is None or ctx.source_memory is None:
        return None
    mem = ctx.source_memory.load(company_key_for(company_name, country))
    if mem is None or not mem.useful_ri_surfaces:
        return None
    return list(mem.useful_ri_surfaces)


def _persist_ri_source_memory(
    company_name: str,
    country: str,
    attempts: list[SourceAttemptResult],
    snapshot: GovernanceSnapshot,
) -> None:
    ctx = get_attached_run_context()
    if ctx is None or ctx.source_memory is None:
        return
    key = company_key_for(company_name, country)
    store = ctx.source_memory
    mem = store.load(key) or CompanyMemory(company_key=key)
    for a in attempts:
        if not a.success and a.strategy_used:
            s = a.strategy_used
            if s not in mem.failed_ri_strategies:
                mem.failed_ri_strategies.append(s)
    if snapshot.board_members or snapshot.executive_members or snapshot.committees:
        for a in reversed(attempts):
            if a.strategy_used == "llm_extract" and a.success and a.source_url:
                u = a.source_url.strip()
                if u:
                    rest = [x for x in mem.useful_ri_surfaces if x != u]
                    mem.useful_ri_surfaces = [u] + rest
                break
    mem.last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    store.save(key, mem)


def _ingest_current(
    ri_url: str | None,
    company_name: str,
    *,
    country: str = "BR",
    trace_ri: Callable[[SourceAttemptResult], None] | None = None,
) -> tuple[GovernanceSnapshot, dict[str, str | None]]:
    """Track B: scrape RI page and extract governance via LLM. Returns snapshot + metadata keys."""
    if not ri_url:
        logger.warning("[n1] No RI URL available — skipping current track")
        return GovernanceSnapshot(), {}

    try:
        scraper = RIScraper(
            browserless_endpoint=os.environ.get("BROWSERLESS_ENDPOINT"),
            browserless_token=os.environ.get("BROWSERLESS_TOKEN"),
        )
        preferred = _ri_preferred_surfaces(company_name, country)
        snapshot, attempts = scraper.scrape_board(
            ri_url, company_name, preferred_urls=preferred
        )
        meta: dict[str, str | None] = {
            "ri_attempts_json": SourceAttemptResult.attempts_json(attempts),
        }
        pm = primary_ri_failure_mode(attempts)
        if pm:
            meta["ri_primary_failure_mode"] = pm
        if trace_ri:
            for a in attempts:
                trace_ri(a)
        _persist_ri_source_memory(company_name, country, attempts, snapshot)
        logger.info(
            f"[n1] RI track B: {len(snapshot.board_members)} board, "
            f"{len(snapshot.executive_members)} exec from {ri_url}"
        )
        return snapshot, meta
    except Exception as e:
        logger.error(f"[n1] RI track B failed: {e}")
        attempts = [
            SourceAttemptResult(
                success=False,
                failure_mode=classify_scrape_exception(e),
                source_url=ri_url,
                content_words=0,
                strategy_used=None,
                latency_ms=0.0,
                error_detail=str(e)[:500],
            )
        ]
        meta = {
            "ri_attempts_json": SourceAttemptResult.attempts_json(attempts),
            "ri_primary_failure_mode": primary_ri_failure_mode(attempts),
        }
        if trace_ri:
            for a in attempts:
                trace_ri(a)
        _persist_ri_source_memory(company_name, country, attempts, GovernanceSnapshot())
        return GovernanceSnapshot(), meta

