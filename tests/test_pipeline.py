from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.models.contracts import (
    BoardMember,
    CanonicalEntity,
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
    HarvestRecallMeta,
    InputPayload,
    StrategyChallenges,
)
from peopledd.orchestrator import run_pipeline
from peopledd.services.harvest_adapter import ProfileSearchOutcome


def _empty_strategy() -> StrategyChallenges:
    return StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )


def test_pipeline_generates_report(tmp_path):
    entity = CanonicalEntity(
        entity_id="e2e-1",
        input_company_name="Empresa Exemplo",
        resolved_name="Empresa Exemplo SA",
        company_mode=CompanyMode.LISTED_BR,
        cnpj="00000000000191",
        ri_url="https://ri.example.com",
        resolution_status=ResolutionStatus.RESOLVED,
        resolution_confidence=0.9,
        analysis_scope_entity="Empresa Exemplo SA",
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="Director One")],
        ),
        current_governance_snapshot=GovernanceSnapshot(),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.85,
            current_completeness=0.5,
            freshness_score=0.8,
        ),
    )

    harvest_inst = MagicMock()
    harvest_inst.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[],
        recall=HarvestRecallMeta(resolution_attempted=True),
    )
    harvest_inst.get_profile.return_value = None
    harvest_inst.compute_profile_quality.return_value = {
        "useful_coverage_score": 0.0,
        "evidence_density": 0.0,
        "recency_score": 0.0,
        "profile_confidence": 0.1,
    }
    harvest_inst.build_career_summary.return_value = {
        "current_roles": [],
        "prior_roles": [],
        "functional_experience": [],
        "industry_experience": [],
        "governance_signals": [],
    }

    with (
        patch("peopledd.runtime.graph_runner.n0_entity_resolution.run", return_value=entity),
        patch("peopledd.runtime.graph_runner.n1_governance_ingestion.run", return_value=ingestion),
        patch("peopledd.runtime.graph_runner.HarvestAdapter", return_value=harvest_inst),
        patch("peopledd.runtime.graph_runner.n4_strategy_inference.run", return_value=_empty_strategy()),
    ):
        payload = InputPayload(company_name="Empresa Exemplo")
        report = run_pipeline(payload, output_dir=str(tmp_path))

    assert report.entity_resolution.input_company_name == "Empresa Exemplo"
    assert report.entity_resolution.resolution_status == ResolutionStatus.RESOLVED
    assert report.degradation_profile.service_level in {"SL1", "SL2", "SL3", "SL4", "SL5"}
    assert len(report.people_resolution) == 1
    assert report.people_resolution[0].observed_name == "Director One"
    assert report.people_resolution[0].resolution_status == ResolutionStatus.NOT_FOUND
    assert len(report.people_profiles) == 1
    assert report.strategy_and_challenges.company_phase_hypothesis["phase"] == "mixed"
    assert report.pipeline_telemetry is not None
    assert report.pipeline_telemetry.run_id
    assert report.pipeline_telemetry.adaptive_decisions
    assert any(
        d.get("checkpoint") == "n1_post_ingestion"
        for d in report.pipeline_telemetry.adaptive_decisions
    )
    run_dirs = list(Path(tmp_path).iterdir())
    assert run_dirs, "expected run subdirectory"
    trace_path = run_dirs[0] / "run_trace.json"
    assert trace_path.is_file()
    trace = json.loads(trace_path.read_text(encoding="utf-8"))
    assert any(e.get("node") == "n0" for e in trace)


def test_output_mode_json_skips_markdown(tmp_path):
    entity = CanonicalEntity(
        entity_id="e2e-json",
        input_company_name="Empresa Exemplo",
        resolved_name="Empresa Exemplo SA",
        company_mode=CompanyMode.LISTED_BR,
        cnpj="00000000000191",
        ri_url="https://ri.example.com",
        resolution_status=ResolutionStatus.RESOLVED,
        resolution_confidence=0.9,
        analysis_scope_entity="Empresa Exemplo SA",
    )
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=GovernanceSnapshot(
            board_members=[BoardMember(person_name="Director One")],
        ),
        current_governance_snapshot=GovernanceSnapshot(),
        governance_data_quality=GovernanceDataQuality(
            formal_completeness=0.85,
            current_completeness=0.5,
            freshness_score=0.8,
        ),
    )
    harvest_inst = MagicMock()
    harvest_inst.search_by_name.return_value = ProfileSearchOutcome(
        candidates=[],
        recall=HarvestRecallMeta(resolution_attempted=True),
    )
    harvest_inst.get_profile.return_value = None
    harvest_inst.compute_profile_quality.return_value = {
        "useful_coverage_score": 0.0,
        "evidence_density": 0.0,
        "recency_score": 0.0,
        "profile_confidence": 0.1,
    }
    harvest_inst.build_career_summary.return_value = {
        "current_roles": [],
        "prior_roles": [],
        "functional_experience": [],
        "industry_experience": [],
        "governance_signals": [],
    }
    with (
        patch("peopledd.runtime.graph_runner.n0_entity_resolution.run", return_value=entity),
        patch("peopledd.runtime.graph_runner.n1_governance_ingestion.run", return_value=ingestion),
        patch("peopledd.runtime.graph_runner.HarvestAdapter", return_value=harvest_inst),
        patch("peopledd.runtime.graph_runner.n4_strategy_inference.run", return_value=_empty_strategy()),
    ):
        payload = InputPayload(company_name="Empresa Exemplo", output_mode="json")
        run_pipeline(payload, output_dir=str(tmp_path))

    run_dirs = list(Path(tmp_path).iterdir())
    assert run_dirs
    base = run_dirs[0]
    assert (base / "run_trace.json").is_file()
    assert (base / "final_report.json").is_file()
    assert not (base / "final_report.md").exists()
