from __future__ import annotations

"""
n4_strategy_inference — extracts strategic context from RI pages.

Uses StrategyRetriever (ScrapeOrchestratorV5 + gpt-5.4) to build StrategyChallenges.
Degrades gracefully to empty challenges if RI URL is unavailable.
"""

import logging

from peopledd.models.contracts import KeyChallenge, StrategicPriority, StrategyChallenges
from peopledd.models.common import SourceRef
from peopledd.services.strategy_retriever import StrategyRetriever

logger = logging.getLogger(__name__)


def run(
    company_name: str,
    ri_url: str | None = None,
    sector: str | None = None,
    retriever: StrategyRetriever | None = None,
) -> StrategyChallenges:
    """
    Infer strategic priorities and key challenges for a company.

    Args:
        company_name: Resolved company name.
        ri_url: RI base URL to scrape strategy pages from.
        sector: CVM sector code or description for context.
        retriever: Optional injected StrategyRetriever (for testing).
    """
    if retriever is None:
        retriever = StrategyRetriever(llm_model="gpt-5.4")

    raw = retriever.retrieve(company_name=company_name, ri_url=ri_url, sector=sector)

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

    logger.info(
        f"[n4] {company_name}: "
        f"{len(strategic_priorities)} priorities, "
        f"{len(key_challenges)} challenges, "
        f"phase={company_phase['phase']} ({company_phase['confidence']:.2f})"
    )

    return StrategyChallenges(
        strategic_priorities=strategic_priorities,
        key_challenges=key_challenges,
        recent_triggers=recent_triggers,
        company_phase_hypothesis=company_phase,
    )

