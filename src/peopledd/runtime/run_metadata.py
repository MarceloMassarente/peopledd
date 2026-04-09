from __future__ import annotations

from pathlib import Path
from typing import Any

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.runtime.artifact_policy import planned_artifact_filenames, pipeline_stage_ids

_ENV_HINTS: list[dict[str, Any]] = [
    {
        "name": "OPENAI_API_KEY",
        "purpose": "LLM: strategy extraction, semantic fusion judge, market pulse, private governance discovery, optional person pick.",
        "required": False,
    },
    {
        "name": "OPENAI_MODEL",
        "purpose": "Default chat model name when the code resolves model from env.",
        "required": False,
    },
    {
        "name": "OPENAI_MODEL_MINI",
        "purpose": "Smaller/cheaper model for auxiliary LLM steps when set.",
        "required": False,
    },
    {
        "name": "OPENAI_MARKET_PULSE_MODEL",
        "purpose": "Model override for market pulse structured extraction.",
        "required": False,
    },
    {
        "name": "EXA_API_KEY",
        "purpose": "Exa search (web, company/people discovery, market pulse sources).",
        "required": False,
    },
    {
        "name": "SEARXNG_URL",
        "purpose": "SearXNG base URL as alternative/complement to Exa for search orchestration.",
        "required": False,
    },
    {
        "name": "SERPER_API_KEY",
        "purpose": "Serper Google search backend when configured in vendor search.",
        "required": False,
    },
    {
        "name": "PERPLEXITY_API_KEY",
        "purpose": "Optional Perplexity Sonar briefs in strategy retrieval.",
        "required": False,
    },
    {
        "name": "HARVEST_API_KEY",
        "purpose": "Harvest API for LinkedIn profile search and enrichment.",
        "required": False,
    },
    {
        "name": "JINA_API_KEY",
        "purpose": "Jina Reader for fetch/scrape paths.",
        "required": False,
    },
    {
        "name": "BROWSERLESS_ENDPOINT",
        "purpose": "Browserless HTTP endpoint for JS rendering.",
        "required": False,
    },
    {
        "name": "BROWSERLESS_TOKEN",
        "purpose": "Browserless auth token when required by the deployment.",
        "required": False,
    },
]


def build_run_summary(
    final_report: FinalReport,
    run_id: str,
    run_dir: Path,
    output_mode: str,
    status: str,
) -> dict[str, Any]:
    """Compact JSON-serializable snapshot for run_summary.json and ops dashboards."""
    tel = final_report.pipeline_telemetry
    deg = final_report.degradation_profile
    mp = final_report.market_pulse
    return {
        "run_id": run_id,
        "status": status,
        "output_mode": output_mode,
        "run_directory": str(run_dir.resolve()),
        "service_level": deg.service_level.value,
        "entity_display_name": (
            final_report.entity_resolution.resolved_name
            or final_report.entity_resolution.input_company_name
        ),
        "degradations_count": len(deg.degradations),
        "omitted_sections": list(deg.omitted_sections),
        "market_pulse": {
            "skipped_reason": mp.skipped_reason,
            "claims_count": len(mp.claims),
            "source_hits_count": len(mp.source_hits),
        },
        "telemetry": {
            "llm_calls_used": tel.llm_calls_used if tel else 0,
            "llm_budget_skips": list(tel.llm_budget_skips[:24]) if tel else [],
            "recovery_counts": dict(tel.recovery_counts) if tel else {},
        },
        "artifacts_expected": planned_artifact_filenames(output_mode),
    }


def describe_run_payload() -> dict[str, Any]:
    """Machine-readable contract for CLI --describe-run (no network)."""
    return {
        "describe_run_version": 1,
        "pipeline_stages": pipeline_stage_ids(),
        "artifacts_by_output_mode": {
            "report": planned_artifact_filenames("report"),
            "json": planned_artifact_filenames("json"),
            "both": planned_artifact_filenames("both"),
        },
        "environment_variables": list(_ENV_HINTS),
        "input_payload_json_schema": InputPayload.model_json_schema(),
    }


def format_dry_run_plan(
    *,
    company_name: str,
    country: str,
    output_dir: str,
    output_mode: str,
    use_harvest: bool,
    prefer_llm_fusion: bool,
    use_apify: bool,
    use_browserless: bool,
    allow_manual_resolution: bool,
    analysis_depth: str,
    company_type_hint: str,
) -> str:
    """Human-readable plan for --dry-run."""
    lines = [
        "peopledd dry-run (no network, no LLM execution)",
        "",
        f"Company: {company_name!r} | Country: {country}",
        f"Output base dir: {output_dir}",
        f"Output mode: {output_mode} (artifacts listed below)",
        f"Flags: use_harvest={use_harvest}, prefer_llm_fusion={prefer_llm_fusion}, "
        f"use_apify={use_apify}, use_browserless={use_browserless}, "
        f"allow_manual_resolution={allow_manual_resolution}",
        f"Analysis depth: {analysis_depth} | Company type hint: {company_type_hint}",
        "",
        "Pipeline stages:",
    ]
    for sid in pipeline_stage_ids():
        lines.append(f"  - {sid}")
    lines += [
        "",
        f"Artifacts that would be written under <output_dir>/<run_id>/ ({output_mode}):",
    ]
    for name in planned_artifact_filenames(output_mode):
        lines.append(f"  - {name}")
    lines.append("")
    return "\n".join(lines)
