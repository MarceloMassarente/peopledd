from __future__ import annotations

"""
Which JSON/MD artifacts are written per InputPayload.output_mode.
Used by graph_runner and by CLI --describe-run / --dry-run.
"""

OUTPUT_MODES = frozenset({"report", "json", "both"})

# Lean bundle for human-oriented report folder
REPORT_ARTIFACT_KEYS = frozenset({
    "input",
    "run_trace",
    "run_log",
    "final_report_json",
    "final_report_md",
    "degradation_profile",
})

# Order matches graph_runner writes (for stable listings)
ARTIFACT_KEY_TO_FILENAME: list[tuple[str, str]] = [
    ("input", "input.json"),
    ("entity_resolution", "entity_resolution.json"),
    ("governance_formal", "governance_formal.json"),
    ("governance_current", "governance_current.json"),
    ("governance_reconciliation", "governance_reconciliation.json"),
    ("semantic_governance_fusion", "semantic_governance_fusion.json"),
    ("people_resolution", "people_resolution.json"),
    ("people_profiles", "people_profiles.json"),
    ("strategy_and_challenges", "strategy_and_challenges.json"),
    ("market_pulse", "market_pulse.json"),
    ("required_capability_model", "required_capability_model.json"),
    ("coverage_scoring", "coverage_scoring.json"),
    ("improvement_hypotheses", "improvement_hypotheses.json"),
    ("evidence_pack", "evidence_pack.json"),
    ("degradation_profile", "degradation_profile.json"),
    ("final_report_json", "final_report.json"),
    ("final_report_md", "final_report.md"),
    ("run_log", "run_log.json"),
    ("run_trace", "run_trace.json"),
]

# Due diligence brief (success path); all output modes include this file
DD_BRIEF_FILENAME = "dd_brief.json"


def validate_output_mode(mode: str) -> None:
    if mode not in OUTPUT_MODES:
        raise ValueError(
            f"Invalid output_mode {mode!r}; expected one of {sorted(OUTPUT_MODES)}"
        )


def artifact_include(artifact_key: str, mode: str) -> bool:
    validate_output_mode(mode)
    if mode == "both":
        return True
    if mode == "json":
        return artifact_key != "final_report_md"
    return artifact_key in REPORT_ARTIFACT_KEYS


def planned_artifact_filenames(output_mode: str) -> list[str]:
    """Filenames written when a run completes successfully (includes run_summary.json, dd_brief.json)."""
    validate_output_mode(output_mode)
    names = [fn for key, fn in ARTIFACT_KEY_TO_FILENAME if artifact_include(key, output_mode)]
    names.append("run_summary.json")
    names.append(DD_BRIEF_FILENAME)
    return names


def pipeline_stage_ids() -> list[str]:
    return [
        "n0_entity_resolution",
        "n1_governance_ingestion",
        "n1b_reconciliation",
        "n1c_semantic_fusion",
        "n2_person_resolution",
        "n3_profile_enrichment",
        "n4_strategy_inference",
        "market_pulse",
        "n5_required_capability_model",
        "n6_coverage_scoring",
        "n8_evidence_pack",
        "n7_improvement_hypotheses",
        "n9_report_builder",
    ]
