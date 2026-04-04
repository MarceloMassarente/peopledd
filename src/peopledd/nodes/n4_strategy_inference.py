from __future__ import annotations

"""
n4_strategy_inference — extracts strategic context from RI pages.

Uses StrategyRetriever (ScrapeOrchestratorV5 + gpt-5.4) to build StrategyChallenges.
Degrades gracefully to empty challenges if RI URL is unavailable.
"""

import logging
from typing import TYPE_CHECKING

from peopledd.models.contracts import ExternalSonarBrief, KeyChallenge, StrategicPriority, StrategyChallenges
from peopledd.models.common import SourceRef
from peopledd.runtime.adaptive_models import FindUrlsParams
from peopledd.services.strategy_retriever import StrategyRetriever

if TYPE_CHECKING:
    from peopledd.vendor.search import SearchOrchestrator

logger = logging.getLogger(__name__)


def run(
    company_name: str,
    ri_url: str | None = None,
    sector: str | None = None,
    country: str = "BR",
    retriever: StrategyRetriever | None = None,
    strategy_max_pages: int | None = None,
    search_orchestrator: SearchOrchestrator | None = None,
    find_urls_params: FindUrlsParams | None = None,
    strategy_search_attempt_index: int = 0,
    find_urls_escalation_level: int = 0,
) -> StrategyChallenges:
    """
    Infer strategic priorities and key challenges for a company.

    Args:
        company_name: Resolved company name.
        ri_url: RI base URL to scrape strategy pages from.
        sector: CVM sector code or description for context.
        country: País para queries Perplexity Sonar (quando habilitado).
        retriever: Optional injected StrategyRetriever (for testing).
        search_orchestrator: Shared SearchOrchestrator for adaptive URL discovery (optional).
        find_urls_params: Tuning for SearchOrchestrator.find_urls_async (escalation).
    """
    if retriever is None:
        kw: dict = {"llm_model": "gpt-5.4", "search_orchestrator": search_orchestrator}
        if strategy_max_pages is not None:
            kw["max_pages"] = max(1, min(12, strategy_max_pages))
        retriever = StrategyRetriever(**kw)
    else:
        if strategy_max_pages is not None:
            mp = max(1, min(12, strategy_max_pages))
            retriever.max_pages = mp
            retriever._search.max_results = mp
            retriever._search.selector.max_urls = mp

    raw = retriever.retrieve(
        company_name=company_name,
        ri_url=ri_url,
        sector=sector,
        country=country,
        find_urls_params=find_urls_params,
        strategy_search_attempt_index=strategy_search_attempt_index,
        find_urls_escalation_level=find_urls_escalation_level,
        skip_perplexity_sonar=strategy_search_attempt_index > 0,
    )

    # Fallback source ref
    src = SourceRef(
        source_type="ri",
        label="RI estratégia",
        url_or_ref=ri_url or f"ri://{company_name}/strategy",
    )

    # ── Strategic Priorities ──────────────────────────────────────────────────
    strategic_priorities: list[StrategicPriority] = []
    for item in raw.get("strategic_priorities", []):
        priority_text = item.get("priority", "").strip()
        if not priority_text:
            continue
        strategic_priorities.append(StrategicPriority(
            priority=priority_text,
            time_horizon=item.get("time_horizon", "medium"),  # type: ignore
            confidence=float(item.get("confidence", 0.6)),
            source_refs=[SourceRef(
                source_type="ri",
                label=item.get("evidence_snippet", "estratégia RI")[:80],
                url_or_ref=ri_url or src.url_or_ref,
            )],
        ))

    # ── Key Challenges ────────────────────────────────────────────────────────
    key_challenges: list[KeyChallenge] = []
    for item in raw.get("key_challenges", []):
        challenge_text = item.get("challenge", "").strip()
        if not challenge_text:
            continue
        key_challenges.append(KeyChallenge(
            challenge=challenge_text,
            challenge_type=item.get("challenge_type", "operational"),  # type: ignore
            severity=item.get("severity", "medium"),  # type: ignore
            confidence=float(item.get("confidence", 0.6)),
            source_refs=[SourceRef(
                source_type="ri",
                label=item.get("evidence_snippet", "desafio RI")[:80],
                url_or_ref=ri_url or src.url_or_ref,
            )],
        ))

    # ── Recent Triggers ───────────────────────────────────────────────────────
    recent_triggers = [
        t.strip() for t in raw.get("recent_triggers", []) if t and t.strip()
    ]

    # ── Company Phase Hypothesis ──────────────────────────────────────────────
    phase_raw = raw.get("company_phase_hypothesis", {})
    company_phase = {
        "phase": phase_raw.get("phase", "mixed"),
        "confidence": phase_raw.get("confidence", 0.4),
        "rationale": phase_raw.get("rationale", ""),
    }

    external_briefs: list[ExternalSonarBrief] = []
    for row in raw.get("external_sonar_briefs") or []:
        if not isinstance(row, dict):
            continue
        role = row.get("role")
        if role not in ("recent_company_facts", "sector_governance_context"):
            continue
        refs_raw = row.get("source_refs") or []
        refs: list[SourceRef] = []
        for sr in refs_raw:
            if not isinstance(sr, dict):
                continue
            u = (sr.get("url_or_ref") or "").strip()
            if not u:
                continue
            refs.append(
                SourceRef(
                    source_type=sr.get("source_type") or "perplexity_sonar_pro",
                    label=sr.get("label"),
                    url_or_ref=u,
                    date=sr.get("date"),
                )
            )
        external_briefs.append(
            ExternalSonarBrief(
                role=role,
                body=str(row.get("body") or ""),
                source_refs=refs,
            )
        )

    logger.info(
        f"[n4] {company_name}: "
        f"{len(strategic_priorities)} priorities, "
        f"{len(key_challenges)} challenges, "
        f"phase={company_phase['phase']} ({company_phase['confidence']:.2f}), "
        f"sonar_briefs={len(external_briefs)}"
    )

    return StrategyChallenges(
        strategic_priorities=strategic_priorities,
        key_challenges=key_challenges,
        recent_triggers=recent_triggers,
        company_phase_hypothesis=company_phase,
        external_sonar_briefs=external_briefs,
    )

