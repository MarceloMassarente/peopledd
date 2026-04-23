from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from peopledd.models.contracts import (
    CanonicalEntity,
    InputPayload,
    ConfidencePolicy,
    CoverageScoring,
    DegradationProfile,
    EvidencePack,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSeed,
    ImprovementHypothesis,
    MarketPulse,
    PersonProfile,
    PersonResolution,
    RequiredCapabilityModel,
    SemanticGovernanceFusion,
    StrategyChallenges,
)
from peopledd.runtime.adaptive_models import FindUrlsParams, PersonSearchParams, PipelineSearchPlanState

CheckpointPhase = Literal["post_people"]


def checkpoint_input_fingerprint(payload: InputPayload) -> str:
    """
    Stable hash of InputPayload fields that affect n0–n3 (identity and early pipeline).
    Excludes run_id and output_mode so resume can change artifact mode without invalidating checkpoint.
    """
    body = {
        "company_name": (payload.company_name or "").strip(),
        "country": (payload.country or "BR").strip().upper(),
        "company_type_hint": payload.company_type_hint,
        "ticker_hint": payload.ticker_hint,
        "cnpj_hint": payload.cnpj_hint,
        "use_harvest": payload.use_harvest,
        "prefer_llm": payload.prefer_llm,
        "use_apify": payload.use_apify,
        "use_browserless": payload.use_browserless,
        "allow_manual_resolution": payload.allow_manual_resolution,
        "analysis_depth": payload.analysis_depth,
    }
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def search_plan_to_dict(plan: PipelineSearchPlanState) -> dict[str, Any]:
    return {
        "person_params": plan.person_params.model_dump(mode="json"),
        "find_urls_params": plan.find_urls_params.model_dump(mode="json"),
        "strategy_max_pages": plan.strategy_max_pages,
    }


def search_plan_from_dict(data: dict[str, Any]) -> PipelineSearchPlanState:
    plan = PipelineSearchPlanState()
    plan.person_params = PersonSearchParams.model_validate(data["person_params"])
    plan.find_urls_params = FindUrlsParams.model_validate(data["find_urls_params"])
    plan.strategy_max_pages = data.get("strategy_max_pages")
    return plan


@dataclass
class PipelineState:
    """Mutable pipeline carry-over between phase methods (n0–n9)."""

    company_name: str = ""
    entity: CanonicalEntity | None = None
    ingestion: GovernanceIngestion | None = None
    reconciliation: GovernanceReconciliation | None = None
    semantic_fusion: SemanticGovernanceFusion | None = None
    governance_seed: GovernanceSeed | None = None
    people_resolution: list[PersonResolution] = field(default_factory=list)
    people_profiles: list[PersonProfile] = field(default_factory=list)
    strategy: StrategyChallenges | None = None
    market_pulse: MarketPulse | None = None
    capability_model: RequiredCapabilityModel | None = None
    coverage: CoverageScoring | None = None
    evidence: EvidencePack | None = None
    hypotheses: list[ImprovementHypothesis] = field(default_factory=list)
    degradation_profile: DegradationProfile | None = None
    confidence_policy: ConfidencePolicy | None = None

    def is_governance_complete(self) -> bool:
        return (
            self.entity is not None
            and self.ingestion is not None
            and self.reconciliation is not None
            and self.semantic_fusion is not None
        )

    def is_people_complete(self) -> bool:
        return self.is_governance_complete() and self.people_phase_completed

    people_phase_completed: bool = False

    def to_checkpoint_dict(self) -> dict[str, Any]:
        def dump_model(m: Any) -> dict[str, Any] | None:
            if m is None:
                return None
            return m.model_dump(mode="json")

        return {
            "checkpoint_version": 1,
            "company_name": self.company_name,
            "entity": dump_model(self.entity),
            "ingestion": dump_model(self.ingestion),
            "reconciliation": dump_model(self.reconciliation),
            "semantic_fusion": dump_model(self.semantic_fusion),
            "governance_seed": dump_model(self.governance_seed),
            "people_resolution": [p.model_dump(mode="json") for p in self.people_resolution],
            "people_profiles": [p.model_dump(mode="json") for p in self.people_profiles],
            "people_phase_completed": self.people_phase_completed,
        }

    @classmethod
    def from_checkpoint_dict(cls, data: dict[str, Any]) -> PipelineState:
        def load_entity(raw: dict[str, Any] | None) -> CanonicalEntity | None:
            return CanonicalEntity.model_validate(raw) if raw else None

        def load_ingestion(raw: dict[str, Any] | None) -> GovernanceIngestion | None:
            return GovernanceIngestion.model_validate(raw) if raw else None

        def load_recon(raw: dict[str, Any] | None) -> GovernanceReconciliation | None:
            return GovernanceReconciliation.model_validate(raw) if raw else None

        def load_fusion(raw: dict[str, Any] | None) -> SemanticGovernanceFusion | None:
            return SemanticGovernanceFusion.model_validate(raw) if raw else None

        def load_seed(raw: dict[str, Any] | None) -> GovernanceSeed | None:
            return GovernanceSeed.model_validate(raw) if raw else None

        return cls(
            company_name=str(data.get("company_name") or ""),
            entity=load_entity(data.get("entity")),
            ingestion=load_ingestion(data.get("ingestion")),
            reconciliation=load_recon(data.get("reconciliation")),
            semantic_fusion=load_fusion(data.get("semantic_fusion")),
            governance_seed=load_seed(data.get("governance_seed")),
            people_resolution=[
                PersonResolution.model_validate(x) for x in (data.get("people_resolution") or [])
            ],
            people_profiles=[PersonProfile.model_validate(x) for x in (data.get("people_profiles") or [])],
            people_phase_completed=bool(data.get("people_phase_completed")),
        )


CHECKPOINT_FILENAME = "checkpoint.json"


def write_checkpoint(
    base: Path,
    run_id: str,
    phase: CheckpointPhase,
    state: PipelineState,
    search_plan: PipelineSearchPlanState,
    *,
    input_fingerprint: str,
) -> None:
    payload = {
        "run_id": run_id,
        "phase": phase,
        "input_fingerprint": input_fingerprint,
        "state": state.to_checkpoint_dict(),
        "search_plan": search_plan_to_dict(search_plan),
    }
    base.mkdir(parents=True, exist_ok=True)
    (base / CHECKPOINT_FILENAME).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def read_checkpoint(
    base: Path,
) -> tuple[str, CheckpointPhase, PipelineState, PipelineSearchPlanState, str | None] | None:
    path = base / CHECKPOINT_FILENAME
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    rid = str(raw.get("run_id") or "")
    if raw.get("phase") != "post_people":
        return None
    inner = raw.get("state")
    if not isinstance(inner, dict) or inner.get("checkpoint_version") != 1:
        return None
    sp_raw = raw.get("search_plan")
    if not isinstance(sp_raw, dict):
        return None
    stored_fp = raw.get("input_fingerprint")
    if stored_fp is not None and not isinstance(stored_fp, str):
        return None
    try:
        st = PipelineState.from_checkpoint_dict(inner)
        sp = search_plan_from_dict(sp_raw)
    except Exception:
        return None
    return rid, "post_people", st, sp, stored_fp if isinstance(stored_fp, str) else None


def remove_checkpoint(base: Path) -> None:
    path = base / CHECKPOINT_FILENAME
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass
