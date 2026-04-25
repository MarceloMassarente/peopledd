from __future__ import annotations

import json
from pathlib import Path

from peopledd.models.common import ResolutionStatus
from peopledd.models.contracts import (
    BoardMember,
    CanonicalEntity,
    CompanyMode,
    EntityRelationType,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
    GovernanceDataQuality,
    InputPayload,
    MarketPulse,
    PersonProfile,
    PersonResolution,
    ProfileQuality,
    SemanticGovernanceFusion,
    StrategyChallenges,
)
from peopledd.runtime.adaptive_models import PipelineSearchPlanState
from peopledd.runtime.pipeline_state import (
    CHECKPOINT_FILENAME,
    PipelineState,
    checkpoint_input_fingerprint,
    read_checkpoint,
    remove_checkpoint,
    search_plan_from_dict,
    search_plan_to_dict,
    write_checkpoint,
)


def test_checkpoint_input_fingerprint_stable_and_differs_by_company() -> None:
    a = InputPayload(company_name="Acme")
    b = InputPayload(company_name="Beta")
    fa = checkpoint_input_fingerprint(a)
    fb = checkpoint_input_fingerprint(b)
    assert len(fa) == 64
    assert fa == checkpoint_input_fingerprint(InputPayload(company_name="Acme"))
    assert fa != fb


def test_output_mode_does_not_change_fingerprint() -> None:
    a = InputPayload(company_name="X", output_mode="both")
    b = InputPayload(company_name="X", output_mode="report")
    assert checkpoint_input_fingerprint(a) == checkpoint_input_fingerprint(b)


def test_search_plan_roundtrip_dict() -> None:
    plan = PipelineSearchPlanState()
    plan.strategy_max_pages = 5
    plan.escalate_person_secondary()
    d = search_plan_to_dict(plan)
    restored = search_plan_from_dict(d)
    assert restored.strategy_max_pages == 5
    assert restored.person_params.escalation_level == 1


def test_checkpoint_write_read_roundtrip(tmp_path: Path) -> None:
    entity = CanonicalEntity(
        entity_id="e1",
        input_company_name="Acme",
        resolved_name="Acme SA",
        company_mode=CompanyMode.LISTED_BR,
        entity_relation_type=EntityRelationType.UNKNOWN,
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(),
        current_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="A", source_refs=[])]
        ),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.1,
            current_completeness=0.5,
            freshness_score=0.5,
        ),
        ingestion_metadata={},
    )
    recon = GovernanceReconciliation(
        reconciled_governance_snapshot=ingestion.current_governance_snapshot,
    )
    fusion = SemanticGovernanceFusion(
        resolved_snapshot=ingestion.current_governance_snapshot,
    )
    pr = PersonResolution(
        person_id="p1",
        observed_name="A",
        resolution_status=ResolutionStatus.RESOLVED,
    )
    pp = PersonProfile(
        person_id="p1",
        profile_quality=ProfileQuality(useful_coverage_score=0.8),
    )
    state = PipelineState(
        company_name="Acme SA",
        entity=entity,
        ingestion=ingestion,
        reconciliation=recon,
        semantic_fusion=fusion,
        people_resolution=[pr],
        people_profiles=[pp],
        people_phase_completed=True,
    )
    sp = PipelineSearchPlanState()
    rid = "test-run-id"
    base = tmp_path / rid
    fp = checkpoint_input_fingerprint(InputPayload(company_name="Acme SA"))
    write_checkpoint(base, rid, "post_people", state, sp, input_fingerprint=fp)
    loaded = read_checkpoint(base)
    assert loaded is not None
    out_rid, phase, st2, sp2, out_fp = loaded
    assert out_rid == rid
    assert out_fp == fp
    assert phase == "post_people"
    assert st2.company_name == "Acme SA"
    assert st2.entity is not None and st2.entity.resolved_name == "Acme SA"
    assert len(st2.people_resolution) == 1
    assert st2.people_profiles[0].profile_quality.useful_coverage_score == 0.8
    assert sp2.strategy_max_pages is None
    remove_checkpoint(base)
    assert read_checkpoint(base) is None


def test_post_strategy_checkpoint_roundtrip(tmp_path: Path) -> None:
    entity = CanonicalEntity(
        entity_id="e1",
        input_company_name="Acme",
        resolved_name="Acme SA",
        company_mode=CompanyMode.LISTED_BR,
        entity_relation_type=EntityRelationType.UNKNOWN,
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(),
        current_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="A", source_refs=[])]
        ),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.1,
            current_completeness=0.5,
            freshness_score=0.5,
        ),
        ingestion_metadata={},
    )
    recon = GovernanceReconciliation(
        reconciled_governance_snapshot=ingestion.current_governance_snapshot,
    )
    fusion = SemanticGovernanceFusion(
        resolved_snapshot=ingestion.current_governance_snapshot,
    )
    pr = PersonResolution(
        person_id="p1",
        observed_name="A",
        resolution_status=ResolutionStatus.RESOLVED,
    )
    pp = PersonProfile(
        person_id="p1",
        profile_quality=ProfileQuality(useful_coverage_score=0.8),
    )
    strategy = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )
    state = PipelineState(
        company_name="Acme SA",
        entity=entity,
        ingestion=ingestion,
        reconciliation=recon,
        semantic_fusion=fusion,
        people_resolution=[pr],
        people_profiles=[pp],
        people_phase_completed=True,
        strategy=strategy,
        market_pulse=MarketPulse(),
    )
    sp = PipelineSearchPlanState()
    rid = "test-run-post-strategy"
    base = tmp_path / rid
    fp = checkpoint_input_fingerprint(InputPayload(company_name="Acme SA"))
    write_checkpoint(base, rid, "post_strategy", state, sp, input_fingerprint=fp)
    loaded = read_checkpoint(base)
    assert loaded is not None
    out_rid, phase, st2, sp2, out_fp = loaded
    assert out_rid == rid
    assert out_fp == fp
    assert phase == "post_strategy"
    assert st2.strategy is not None
    assert st2.market_pulse is not None
    remove_checkpoint(base)
    assert read_checkpoint(base) is None


def test_read_checkpoint_rejects_non_string_input_fingerprint(tmp_path: Path) -> None:
    base = tmp_path / "bad"
    base.mkdir()
    bad = {
        "run_id": "x",
        "phase": "post_people",
        "input_fingerprint": 12345,
        "state": {"checkpoint_version": 1, "company_name": "", "people_resolution": [], "people_profiles": []},
        "search_plan": {
            "person_params": {"query_style": "default", "escalation_level": 0},
            "find_urls_params": {
                "max_searx_queries": 2,
                "searx_num_results": 10,
                "serper_num_results": 10,
                "exa_num_results": 10,
                "topic_override": None,
            },
            "strategy_max_pages": None,
        },
    }
    (base / CHECKPOINT_FILENAME).write_text(json.dumps(bad), encoding="utf-8")
    assert read_checkpoint(base) is None
