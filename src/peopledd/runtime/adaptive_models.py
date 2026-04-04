from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

AssessmentGapKind = Literal[
    "formal_governance_weak",
    "current_governance_weak",
    "people_low_resolution",
    "people_low_evidence",
    "people_ambiguous_matches",
    "strategy_empty",
    "search_orchestrator_missing",
]

PhaseCheckpoint = Literal["n1_post_ingestion", "n2n3_post_profiles", "n4_post_strategy"]

AdaptiveActionKind = Literal[
    "continue",
    "retry_n1_fre_extended",
    "retry_n4_widen_pages",
    "retry_n4_search_escalation",
    "rerun_n2n3_person_search_escalation",
    "degrade_and_continue",
    "noop",
]

PersonLinkedInQueryStyle = Literal["default", "company_first", "name_title_company"]


class AssessmentGap(BaseModel):
    kind: AssessmentGapKind
    detail: str = ""


class PhaseAssessment(BaseModel):
    """Rubric output for a pipeline checkpoint (serializable)."""

    checkpoint: PhaseCheckpoint
    metrics: dict[str, Any] = Field(default_factory=dict)
    gaps: list[AssessmentGap] = Field(default_factory=list)


class AdaptiveDecisionRecord(BaseModel):
    """Single controller decision appended to telemetry and trace."""

    sequence: int
    checkpoint: PhaseCheckpoint
    action: AdaptiveActionKind
    rationale: str
    rule_based: bool = True
    recovery_key: str | None = None


class SearchAttemptRecord(BaseModel):
    purpose: Literal["strategy_find_urls", "person_exa_people"]
    attempt_index: int
    escalation_level: int = 0
    searxng_queries_used: int = 0
    exa_num_results_requested: int = 0
    exa_company_context_results_requested: int = 0
    url_count: int = 0
    empty_pool: bool = False
    topic_excerpt: str = ""
    error: str | None = None


class FindUrlsParams(BaseModel):
    """Parameters for SearchOrchestrator.find_urls_async beyond company/topic/ri."""

    max_searx_queries: int = 2
    searx_num_results: int = 10
    exa_num_results: int = 10
    topic_override: str | None = None

    model_config = {"frozen": True}

    @classmethod
    def default(cls) -> FindUrlsParams:
        return cls()

    @classmethod
    def escalated(cls) -> FindUrlsParams:
        return cls(
            max_searx_queries=3,
            searx_num_results=12,
            exa_num_results=15,
            topic_override=(
                "estratégia corporativa relatório anual RI CVM governança "
                "relações com investidores apresentação resultados"
            ),
        )


class PersonSearchParams(BaseModel):
    """Exa People Search (category=people) query tuning for n2 secondary resolution."""

    query_style: PersonLinkedInQueryStyle = "default"
    escalation_level: int = 0

    model_config = {"frozen": True}

    @classmethod
    def default(cls) -> PersonSearchParams:
        return cls()

    @classmethod
    def escalated(cls) -> PersonSearchParams:
        return cls(query_style="company_first", escalation_level=1)


class PipelineSearchPlanState:
    """Mutable per-run search knobs; not serialized on FinalReport."""

    def __init__(self) -> None:
        self.person_params: PersonSearchParams = PersonSearchParams.default()
        self.find_urls_params: FindUrlsParams = FindUrlsParams.default()
        self.strategy_max_pages: int | None = None

    def escalate_person_secondary(self) -> None:
        self.person_params = PersonSearchParams.escalated()

    def escalate_strategy_find_urls(self) -> None:
        self.find_urls_params = FindUrlsParams.escalated()
