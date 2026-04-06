from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from .common import CompanyMode, EntityRelationType, ResolutionStatus, ServiceLevel, SourceRef


class InputPayload(BaseModel):
    company_name: str
    country: str = "BR"
    company_type_hint: Literal["auto", "listed", "private"] = "auto"
    ticker_hint: str | None = None
    cnpj_hint: str | None = None
    analysis_depth: Literal["standard", "deep"] = "standard"
    use_harvest: bool = True
    use_apify: bool = True
    use_browserless: bool = True
    allow_manual_resolution: bool = False
    output_mode: Literal["report", "json", "both"] = "both"
    prefer_llm: bool = Field(
        default=True,
        description="When True, n1c semantic fusion may call the LLM judge; when False, rule-based fusion only.",
    )


class CanonicalEntity(BaseModel):
    entity_id: str
    input_company_name: str
    resolved_name: str | None = None
    legal_name: str | None = None
    company_mode: CompanyMode = CompanyMode.PRIVATE_OR_UNRESOLVED
    country: str = "BR"
    cnpj: str | None = None
    cod_cvm: str | None = None
    tickers: list[str] = Field(default_factory=list)
    ri_url: str | None = None
    exa_company_enrichment: dict[str, Any] | None = Field(
        default=None,
        description="Snapshot from Exa company search when RI URL was resolved via exa_company_search.",
    )
    entity_relation_type: EntityRelationType = EntityRelationType.UNKNOWN
    analysis_scope_entity: str | None = None
    resolution_confidence: float = 0.0
    resolution_status: ResolutionStatus = ResolutionStatus.NOT_FOUND
    resolution_evidence: list[SourceRef] = Field(default_factory=list)
    candidate_entities: list[str] = Field(default_factory=list)


class BoardMember(BaseModel):
    person_name: str
    role: Literal["chair", "vice-chair", "board-member", "unknown"] = "unknown"
    independence_status: Literal["independent", "non_independent", "unknown"] = "unknown"
    committee_links: list[str] = Field(default_factory=list)
    term_start: str | None = None
    term_end: str | None = None
    source_refs: list[SourceRef] = Field(default_factory=list)


class ExecutiveMember(BaseModel):
    person_name: str
    formal_title: str
    normalized_role: Literal[
        "ceo", "cfo", "coo", "chro", "cto", "cio", "cmo", "legal", "other"
    ] = "other"
    term_start: str | None = None
    source_refs: list[SourceRef] = Field(default_factory=list)


class CommitteeMember(BaseModel):
    person_name: str
    position_in_committee: Literal["chair", "member", "unknown"] = "unknown"


class Committee(BaseModel):
    committee_name: str
    committee_type: Literal["audit", "people", "finance", "strategy", "risk", "esg", "other"] = "other"
    members: list[CommitteeMember] = Field(default_factory=list)
    source_refs: list[SourceRef] = Field(default_factory=list)


class GovernanceSnapshot(BaseModel):
    as_of_date: str | None = None
    board_members: list[BoardMember] = Field(default_factory=list)
    executive_members: list[ExecutiveMember] = Field(default_factory=list)
    committees: list[Committee] = Field(default_factory=list)
    fiscal_council: list[BoardMember] = Field(default_factory=list)


class GovernanceDataQuality(BaseModel):
    formal_completeness: float = 0.0
    current_completeness: float = 0.0
    freshness_score: float = 0.0


class GovernanceIngestion(BaseModel):
    formal_governance_snapshot: GovernanceSnapshot = Field(default_factory=GovernanceSnapshot)
    current_governance_snapshot: GovernanceSnapshot = Field(default_factory=GovernanceSnapshot)
    governance_data_quality: GovernanceDataQuality = Field(default_factory=GovernanceDataQuality)
    ingestion_metadata: dict[str, str | None] = Field(
        default_factory=dict,
        description=(
            "Proveniência: fre_source_url, fre_year, ri_scrape_url; "
            "private_web_discovery=1, private_web_anchor_website, private_web_reason, "
            "private_web_source_count quando current veio de Exa web."
        ),
    )


class ConflictItem(BaseModel):
    conflict_type: Literal[
        "missing_person", "title_mismatch", "organ_mismatch", "term_mismatch", "independence_mismatch"
    ]
    person_name: str | None = None
    formal_value: str | None = None
    current_value: str | None = None
    resolution_rule_applied: str
    confidence: float = 0.0
    source_refs: list[SourceRef] = Field(default_factory=list)


class GovernanceReconciliation(BaseModel):
    reconciliation_status: Literal[
        "clean", "minor_conflicts", "major_conflicts", "current_only", "formal_only"
    ] = "clean"
    conflict_items: list[ConflictItem] = Field(default_factory=list)
    reconciled_governance_snapshot: GovernanceSnapshot = Field(default_factory=GovernanceSnapshot)
    reporting_basis: dict[str, str | None] = Field(
        default_factory=lambda: {
            "formal_basis_date": None,
            "current_basis_date": None,
            "preferred_view_for_reporting": "reconciled",
        }
    )


class MatchedProfile(BaseModel):
    provider: Literal["harvest", "public_web", "exa_web", "exa_people", "other"]
    profile_id_or_url: str
    match_confidence: float = 0.0


class HarvestRecallMeta(BaseModel):
    """Per-person Harvest profile-search recall (aligned with harvest_linkedin_v31 audit fields)."""

    raw_hits_profile_search: int = 0
    after_filter_count: int = 0
    anonymized_dropped_count: int = 0
    profile_search_retry_used: bool = False
    secondary_web_sourcing_used: bool = False
    resolution_attempted: bool = False


class PersonResolution(BaseModel):
    person_id: str
    observed_name: str
    canonical_name: str | None = None
    resolution_status: ResolutionStatus = ResolutionStatus.NOT_FOUND
    resolution_confidence: float = 0.0
    matched_profiles: list[MatchedProfile] = Field(default_factory=list)
    harvest_recall: HarvestRecallMeta | None = None
    resolution_purpose: Literal["governance_member", "fusion_evidence"] = "governance_member"


class ProfileQuality(BaseModel):
    nominal_hit: bool = False
    useful_coverage_score: float = 0.0
    evidence_density: float = 0.0
    recency_score: float = 0.0
    profile_confidence: float = 0.0


class PersonProfile(BaseModel):
    person_id: str
    career_summary: dict[str, list[str]] = Field(default_factory=dict)
    profile_quality: ProfileQuality = Field(default_factory=ProfileQuality)
    blind_spots: list[str] = Field(default_factory=list)


class StrategicPriority(BaseModel):
    priority: str
    time_horizon: Literal["short", "medium", "long"]
    confidence: float = 0.0
    source_refs: list[SourceRef] = Field(default_factory=list)


class KeyChallenge(BaseModel):
    challenge: str
    challenge_type: Literal[
        "financial", "operational", "market", "regulatory", "governance", "technology", "people"
    ]
    severity: Literal["low", "medium", "high"]
    confidence: float = 0.0
    source_refs: list[SourceRef] = Field(default_factory=list)


class ExternalSonarBrief(BaseModel):
    """Evidência sintetizada via Perplexity Sonar Pro (queries fixas no pipeline)."""

    role: Literal["recent_company_facts", "sector_governance_context"]
    body: str = ""
    source_refs: list[SourceRef] = Field(default_factory=list)


class StrategyChallenges(BaseModel):
    strategic_priorities: list[StrategicPriority] = Field(default_factory=list)
    key_challenges: list[KeyChallenge] = Field(default_factory=list)
    recent_triggers: list[str] = Field(default_factory=list)
    company_phase_hypothesis: dict[str, str | float] = Field(
        default_factory=lambda: {"phase": "mixed", "confidence": 0.0}
    )
    external_sonar_briefs: list[ExternalSonarBrief] = Field(default_factory=list)


class RequiredCapability(BaseModel):
    dimension: str
    required_level: int = 1
    importance_weight: float = 0.0
    origin: Literal["sector_baseline", "strategy_overlay", "challenge_overlay"]
    rationale: str


class RequiredCapabilityModel(BaseModel):
    board_required_capabilities: list[RequiredCapability] = Field(default_factory=list)
    executive_required_capabilities: list[RequiredCapability] = Field(default_factory=list)


class CoverageItem(BaseModel):
    dimension: str
    required_level: int = 0
    observed_level: float = 0.0
    confidence_adjusted_level: float = 0.0
    coverage_ratio: float = 0.0
    gap_severity: Literal["low", "medium", "high"] = "medium"
    single_point_of_failure: bool = False
    rationale: str = ""
    source_refs: list[SourceRef] = Field(default_factory=list)


class CoverageScoring(BaseModel):
    board_coverage: list[CoverageItem] = Field(default_factory=list)
    executive_coverage: list[CoverageItem] = Field(default_factory=list)
    organ_level_flags: list[str] = Field(default_factory=list)


class EvidenceSpan(BaseModel):
    """Grounding snippet for a governance observation (URL + optional extract)."""

    url_or_ref: str
    snippet: str | None = None
    chunk_id: str | None = None
    content_fingerprint: str | None = None


class GovernanceObservation(BaseModel):
    """Atomic person mention from one source (formal FRE, RI, private web, profile evidence)."""

    observation_id: str
    observed_name: str
    observed_role: str | None = None
    organ: Literal["board", "executive", "committee", "fiscal_council", "unknown"] = "unknown"
    source_track: Literal[
        "formal_fre", "current_ri", "current_private_web", "profile_evidence", "other"
    ] = "other"
    source_ref: SourceRef
    evidence_span: EvidenceSpan | None = None
    as_of_date: str | None = None
    source_confidence: float = 0.7
    raw_attributes: dict[str, Any] = Field(default_factory=dict)


class GovernanceCandidate(BaseModel):
    """Deterministic pre-cluster of observations likely referring to one person."""

    candidate_id: str
    observation_ids: list[str] = Field(default_factory=list)
    blocking_key: str | None = None


class GovernanceFusionDecision(BaseModel):
    """LLM or rule-based fusion outcome for one candidate cluster."""

    decision_id: str
    candidate_id: str
    canonical_name: str
    normalized_role: str | None = None
    organ: Literal["board", "executive", "committee", "fiscal_council", "unknown"] = "unknown"
    decision_status: Literal["resolved", "partial", "ambiguous", "rejected"] = "partial"
    confidence: float = 0.5
    supporting_observation_ids: list[str] = Field(default_factory=list)
    discarded_observation_ids: list[str] = Field(default_factory=list)
    decision_rationale_code: Literal[
        "dominant_formal",
        "dominant_current",
        "merged_sources",
        "insufficient_evidence",
        "llm_judge",
        "rule_fallback",
    ] = "rule_fallback"
    decision_rationale_detail: str | None = None


class FusionUnresolvedItem(BaseModel):
    item_id: str
    kind: Literal[
        "name_collision",
        "organ_conflict",
        "insufficient_evidence",
        "profile_evidence_pending",
        "rejected_low_trust",
    ] = "insufficient_evidence"
    detail: str = ""
    related_observation_ids: list[str] = Field(default_factory=list)


class ProfileEvidenceNote(BaseModel):
    """Public profile line used as auxiliary evidence for fusion (Harvest / Exa)."""

    person_name_observed: str
    evidence_text: str
    source_url: str | None = None
    provider: Literal["harvest", "exa_people", "other"] = "other"


class GovernanceFusionQuality(BaseModel):
    observation_count: int = 0
    candidate_count: int = 0
    llm_judge_used: bool = False
    judge_passes: int = 0
    profile_evidence_rounds: int = 0
    overall_status: Literal["clean", "minor_gaps", "major_gaps"] = "clean"


class SemanticGovernanceFusion(BaseModel):
    """
    Multi-source semantic fusion layer (parallel to legacy reconciliation).
    Downstream nodes may consume this when migrating; n2 still uses governance_reconciliation by default.
    """

    observations: list[GovernanceObservation] = Field(default_factory=list)
    candidates: list[GovernanceCandidate] = Field(default_factory=list)
    fusion_decisions: list[GovernanceFusionDecision] = Field(default_factory=list)
    resolved_snapshot: GovernanceSnapshot = Field(default_factory=GovernanceSnapshot)
    fusion_quality: GovernanceFusionQuality = Field(default_factory=GovernanceFusionQuality)
    unresolved_items: list[FusionUnresolvedItem] = Field(default_factory=list)
    profile_evidence_notes: list[ProfileEvidenceNote] = Field(default_factory=list)


class ImprovementHypothesis(BaseModel):
    hypothesis_id: str
    category: str
    title: str
    problem_statement: str
    evidence_basis: list[str] = Field(default_factory=list)
    evidence_claim_refs: list[str] = Field(default_factory=list)
    proposed_action: str
    expected_benefit: str
    urgency: Literal["low", "medium", "high"]
    confidence: float = 0.0
    non_triviality_score: float = 0.0
    missing_evidence: list[str] = Field(
        default_factory=list,
        description="Ledger: documented gaps before promotion to report.",
    )
    contradicting_claim_refs: list[str] = Field(
        default_factory=list,
        description="Ledger: claim ids that tension this hypothesis.",
    )
    ledger_status: Literal["promoted", "demoted", "unchanged"] = "unchanged"


class EvidenceDocument(BaseModel):
    doc_id: str
    source_type: str
    title: str
    date: str | None = None
    url_or_ref: str
    retrieval_timestamp: str


class EvidenceClaim(BaseModel):
    claim_id: str
    claim_text: str
    claim_type: Literal["fact", "inference", "score_input", "hypothesis_basis", "semantic_fusion"]
    source_refs: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    observation_ids: list[str] = Field(
        default_factory=list,
        description="Cross-refs to GovernanceObservation.observation_id when claim comes from n1c fusion.",
    )
    fusion_decision_id: str | None = Field(
        default=None,
        description="GovernanceFusionDecision.decision_id when applicable.",
    )


class EvidencePack(BaseModel):
    documents: list[EvidenceDocument] = Field(default_factory=list)
    claims: list[EvidenceClaim] = Field(default_factory=list)


class DegradationProfile(BaseModel):
    service_level: ServiceLevel
    degradations: list[str] = Field(default_factory=list)
    omitted_sections: list[str] = Field(default_factory=list)
    mandatory_disclaimers: list[str] = Field(default_factory=list)
    sl_by_dimension: dict[str, str] = Field(
        default_factory=dict,
        description="Per-dimension service level label (e.g. formal, current, harvest).",
    )
    staleness_by_dimension: dict[str, bool] = Field(
        default_factory=dict,
        description="True when dimension exceeds staleness / weak-data heuristics.",
    )


class ConfidencePolicy(BaseModel):
    data_completeness_score: float = 0.0
    evidence_quality_score: float = 0.0
    analytical_confidence_score: float = 0.0


class PipelineTelemetry(BaseModel):
    """Runtime trace summary attached to FinalReport for audit and tooling."""

    run_id: str = ""
    trace_events: list[dict[str, Any]] = Field(default_factory=list)
    recovery_counts: dict[str, int] = Field(default_factory=dict)
    circuit_states: dict[str, str] = Field(default_factory=dict)
    harvest_recall_totals: dict[str, int] = Field(
        default_factory=dict,
        description="Aggregates over people_resolution harvest_recall (e.g. raw_hits_sum).",
    )
    llm_calls_used: int = 0
    llm_budget_skips: list[str] = Field(
        default_factory=list,
        description="Steps skipped because max_llm_calls was reached.",
    )
    llm_routes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Per-channel record: used_llm vs rule/fallback and reason.",
    )
    adaptive_decisions: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Structured adaptive controller decisions (see peopledd.runtime.adaptive_models).",
    )
    search_attempts: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Search/discovery attempts for strategy URLs and person LinkedIn sourcing.",
    )


class FinalReport(BaseModel):
    input_payload: InputPayload
    entity_resolution: CanonicalEntity
    governance: GovernanceIngestion
    governance_reconciliation: GovernanceReconciliation
    semantic_governance_fusion: SemanticGovernanceFusion | None = None
    people_resolution: list[PersonResolution]
    people_profiles: list[PersonProfile]
    strategy_and_challenges: StrategyChallenges
    required_capability_model: RequiredCapabilityModel
    coverage_scoring: CoverageScoring
    improvement_hypotheses: list[ImprovementHypothesis]
    evidence_pack: EvidencePack
    degradation_profile: DegradationProfile
    confidence_policy: ConfidencePolicy
    pipeline_telemetry: PipelineTelemetry | None = None
