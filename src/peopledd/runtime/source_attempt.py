from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

SourceFailureMode = Literal[
    "timeout",
    "anti_bot",
    "low_content",
    "pdf_only",
    "llm_extract_failed",
    "schema_mismatch",
    "network_error",
    "not_found",
    "parse_error",
    "budget_exhausted",
]


@dataclass(frozen=True)
class SourceAttemptResult:
    """Structured outcome of a single RI fetch or extraction step (intra-run only)."""

    success: bool
    failure_mode: SourceFailureMode | None
    source_url: str
    content_words: int = 0
    strategy_used: str | None = None
    latency_ms: float = 0.0
    as_of_date_hint: str | None = None
    governance_found: bool = False
    error_detail: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "failure_mode": self.failure_mode,
            "source_url": self.source_url,
            "content_words": self.content_words,
            "strategy_used": self.strategy_used,
            "latency_ms": round(self.latency_ms, 3),
            "as_of_date_hint": self.as_of_date_hint,
            "governance_found": self.governance_found,
            "error_detail": self.error_detail,
        }

    @staticmethod
    def attempts_json(attempts: list[SourceAttemptResult]) -> str:
        return json.dumps([a.to_json_dict() for a in attempts], ensure_ascii=False)


def primary_ri_failure_mode(attempts: list[SourceAttemptResult]) -> SourceFailureMode | None:
    """Prefer LLM/extract failure; else first unsuccessful attempt."""
    for a in attempts:
        if a.strategy_used == "llm_extract" and not a.success and a.failure_mode:
            return a.failure_mode
    for a in attempts:
        if not a.success and a.failure_mode:
            return a.failure_mode
    return None


def classify_scrape_exception(exc: BaseException) -> SourceFailureMode:
    import asyncio

    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "timeout"
    name = type(exc).__name__.lower()
    if "timeout" in name:
        return "timeout"
    return "network_error"


def classify_http_status(status_code: int) -> SourceFailureMode | None:
    if status_code in (401, 403, 429):
        return "anti_bot"
    if status_code == 404:
        return "not_found"
    return None
