from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from peopledd.models.common import ResolutionStatus, ServiceLevel
from peopledd.models.contracts import (
    CanonicalEntity,
    ConfidencePolicy,
    CoverageScoring,
    DegradationProfile,
    EvidencePack,
    FinalReport,
    GovernanceIngestion,
    GovernanceReconciliation,
    GovernanceSnapshot,
    InputPayload,
    MarketPulse,
    PersonProfile,
    PersonResolution,
    PipelineTelemetry,
    RequiredCapabilityModel,
    StrategyChallenges,
)
from peopledd.runtime.artifact_policy import planned_artifact_filenames
from peopledd.runtime.run_metadata import build_run_summary, describe_run_payload, format_dry_run_plan
from peopledd.utils.io import OutputDirectoryError, validate_output_base_dir


def test_planned_artifacts_include_run_summary():
    for mode in ("report", "json", "both"):
        names = planned_artifact_filenames(mode)
        assert "run_summary.json" in names
        assert names[-1] == "run_summary.json"


def test_describe_run_payload_shape():
    payload = describe_run_payload()
    assert payload["describe_run_version"] == 1
    assert "n0_entity_resolution" in payload["pipeline_stages"]
    for mode in ("report", "json", "both"):
        assert mode in payload["artifacts_by_output_mode"]
        assert "run_summary.json" in payload["artifacts_by_output_mode"][mode]
    assert payload["input_payload_json_schema"]["title"] == "InputPayload"
    names = {e["name"] for e in payload["environment_variables"]}
    assert "OPENAI_API_KEY" in names
    assert "EXA_API_KEY" in names


def test_build_run_summary_fields(tmp_path):
    payload = InputPayload(company_name="X")
    entity = CanonicalEntity(
        entity_id="e1",
        input_company_name="X",
        resolved_name="X SA",
        resolution_status=ResolutionStatus.RESOLVED,
    )
    snap = GovernanceSnapshot()
    ingestion = GovernanceIngestion(
        formal_governance_snapshot=snap,
        current_governance_snapshot=snap,
    )
    reconciliation = GovernanceReconciliation()
    deg = DegradationProfile(
        service_level=ServiceLevel.SL2,
        degradations=["ri_freshness_missing"],
        omitted_sections=[],
        mandatory_disclaimers=[],
    )
    strategy = StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )
    rcm = RequiredCapabilityModel()
    cov = CoverageScoring()
    tel = PipelineTelemetry(
        run_id="rid-1",
        trace_events=[],
        recovery_counts={"n1": 1},
        circuit_states={},
        harvest_recall_totals={},
        llm_calls_used=3,
        llm_budget_skips=["market_pulse_llm"],
        llm_routes=[],
        adaptive_decisions=[],
        search_attempts=[],
    )
    report = FinalReport(
        input_payload=payload,
        entity_resolution=entity,
        governance=ingestion,
        governance_reconciliation=reconciliation,
        semantic_governance_fusion=None,
        people_resolution=[PersonResolution(person_id="p1", observed_name="A")],
        people_profiles=[PersonProfile(person_id="p1")],
        strategy_and_challenges=strategy,
        market_pulse=MarketPulse(skipped_reason="budget_exhausted"),
        required_capability_model=rcm,
        coverage_scoring=cov,
        improvement_hypotheses=[],
        evidence_pack=EvidencePack(),
        degradation_profile=deg,
        confidence_policy=ConfidencePolicy(
            data_completeness_score=0.5,
            evidence_quality_score=0.5,
            analytical_confidence_score=0.5,
        ),
        pipeline_telemetry=tel,
    )
    run_dir = tmp_path / "r1"
    run_dir.mkdir()
    summary = build_run_summary(report, "rid-1", run_dir, "json", "ok")
    assert summary["run_id"] == "rid-1"
    assert summary["status"] == "ok"
    assert summary["output_mode"] == "json"
    assert summary["service_level"] == "SL2"
    assert summary["market_pulse"]["skipped_reason"] == "budget_exhausted"
    assert summary["telemetry"]["llm_calls_used"] == 3
    assert "market_pulse_llm" in summary["telemetry"]["llm_budget_skips"]
    assert "final_report.json" in summary["artifacts_expected"]
    assert "run_summary.json" in summary["artifacts_expected"]
    assert "final_report.md" not in summary["artifacts_expected"]


def test_format_dry_run_plan_contains_stages():
    text = format_dry_run_plan(
        company_name="Co",
        country="BR",
        output_dir="out",
        output_mode="report",
        use_harvest=True,
        prefer_llm_fusion=True,
        use_apify=False,
        use_browserless=True,
        allow_manual_resolution=False,
        analysis_depth="standard",
        company_type_hint="auto",
    )
    assert "n0_entity_resolution" in text
    assert "Co" in text
    assert "final_report.md" in text


def test_validate_output_base_dir_writable(tmp_path):
    p = validate_output_base_dir(str(tmp_path / "nested"))
    assert p.is_dir()


def test_validate_output_base_dir_rejects_file(tmp_path):
    f = tmp_path / "not_a_directory"
    f.write_text("x", encoding="utf-8")
    with pytest.raises(OutputDirectoryError):
        validate_output_base_dir(str(f))


def test_cli_describe_run_json():
    repo = Path(__file__).resolve().parents[1]
    env = {**os.environ, "PYTHONPATH": str(repo / "src")}
    proc = subprocess.run(
        [sys.executable, "-m", "peopledd.cli", "--describe-run"],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr
    data = json.loads(proc.stdout)
    assert "pipeline_stages" in data
