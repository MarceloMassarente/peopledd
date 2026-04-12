from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from peopledd.models.common import CompanyMode, ResolutionStatus
from peopledd.models.contracts import (
    BoardMember,
    CanonicalEntity,
    GovernanceDataQuality,
    GovernanceIngestion,
    GovernanceSnapshot,
    InputPayload,
    StrategyChallenges,
)
from peopledd.runtime.context import RunContext
import peopledd.utils.io as io_utils

from peopledd.runtime.graph_runner import GraphRunner
from peopledd.services.harvest_adapter import HarvestRecallMeta, ProfileSearchOutcome


def _empty_strategy() -> StrategyChallenges:
    return StrategyChallenges(
        strategic_priorities=[],
        key_challenges=[],
        recent_triggers=[],
        company_phase_hypothesis={"phase": "mixed", "confidence": 0.5},
    )


def test_artifact_write_failure_writes_error_run_summary(tmp_path: Path) -> None:
    entity = CanonicalEntity(
        entity_id="e1",
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

    def flaky_write_json(path: Path, data: object) -> None:
        if path.name == "input.json":
            raise OSError("disk full")
        return io_utils.write_json(path, data)

    ctx = RunContext.create(str(tmp_path))
    runner = GraphRunner(ctx, MagicMock(), MagicMock(), harvest_inst, None)

    with (
        patch("peopledd.runtime.graph_runner.n0_entity_resolution.run", return_value=entity),
        patch("peopledd.runtime.graph_runner.n1_governance_ingestion.run", return_value=ingestion),
        patch("peopledd.runtime.graph_runner.HarvestAdapter", return_value=harvest_inst),
        patch("peopledd.runtime.graph_runner.n4_strategy_inference.run", return_value=_empty_strategy()),
        patch("peopledd.runtime.graph_runner.write_json", side_effect=flaky_write_json),
    ):
        with pytest.raises(OSError, match="disk full"):
            runner.run(InputPayload(company_name="Empresa Exemplo"))

    run_dir = tmp_path / ctx.run_id
    err_summary = run_dir / "run_summary.json"
    assert err_summary.is_file()
    payload = json.loads(err_summary.read_text(encoding="utf-8"))
    assert payload["status"] == "error"
    assert payload.get("error", {}).get("type") == "OSError"
