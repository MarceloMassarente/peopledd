from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.nodes import n9_report_builder
from peopledd.orchestrator import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    epilog = """Examples:
  peopledd --company-name "Itaú Unibanco" --output-dir run
  peopledd --company-name "Acme SA" --output-mode report --no-harvest
"""
    parser = argparse.ArgumentParser(
        description=(
            "Run the peopledd governance X-ray pipeline (n0–n9). "
            "Artifacts are written under OUTPUT_DIR/<run_id>/."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument("--company-name", required=True, help="Company name to analyze")
    parser.add_argument("--country", default="BR", help="Country code (default: BR)")
    parser.add_argument(
        "--company-type-hint",
        default="auto",
        choices=["auto", "listed", "private"],
        help="Whether to assume listed company, private, or infer (default: auto)",
    )
    parser.add_argument("--ticker-hint", default=None, help="Optional equity ticker hint")
    parser.add_argument("--cnpj-hint", default=None, help="Optional CNPJ hint")
    parser.add_argument(
        "--analysis-depth",
        default="standard",
        choices=["standard", "deep"],
        help="Analysis depth (default: standard)",
    )
    parser.add_argument(
        "--no-harvest",
        action="store_true",
        help="Disable Harvest for people resolution (use_harvest=false)",
    )
    parser.add_argument(
        "--no-llm-fusion",
        action="store_true",
        help="Disable LLM judge in n1c semantic fusion (prefer_llm=false; rule-based fusion only)",
    )
    parser.add_argument(
        "--no-apify",
        action="store_true",
        help="Disable Apify-backed paths where applicable (use_apify=false)",
    )
    parser.add_argument(
        "--no-browserless",
        action="store_true",
        help="Do not use Browserless for scraping (use_browserless=false)",
    )
    parser.add_argument(
        "--allow-manual-resolution",
        action="store_true",
        help="Allow manual resolution paths in the model (allow_manual_resolution=true)",
    )
    parser.add_argument(
        "--output-mode",
        default="both",
        choices=["report", "json", "both"],
        help=(
            "Artifacts: 'json' writes JSON only (no final_report.md); "
            "'report' writes a lean set plus markdown; 'both' writes all artifacts (default)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="run",
        help="Base directory for run folders (default: run). Prefer an absolute path in automation.",
    )
    return parser


def _run_folder(output_dir: str, report: FinalReport) -> Path | None:
    tel = report.pipeline_telemetry
    if tel is None or not tel.run_id:
        return None
    return Path(output_dir) / tel.run_id


def _format_run_summary(report: FinalReport, run_path: Path | None) -> str:
    lines: list[str] = []
    if run_path is not None:
        lines.append(f"Run folder: {run_path.resolve()}")
    lines.append(f"Service level: {report.degradation_profile.service_level.value}")
    name = report.entity_resolution.resolved_name or report.entity_resolution.input_company_name
    lines.append(f"Entity: {name}")
    if report.degradation_profile.mandatory_disclaimers:
        d = report.degradation_profile.mandatory_disclaimers[:3]
        lines.append("Disclaimers: " + "; ".join(d))
    tel = report.pipeline_telemetry
    if tel is not None:
        lines.append(f"LLM calls (counted): {tel.llm_calls_used}")
        if tel.llm_budget_skips:
            lines.append("LLM budget skips: " + ", ".join(tel.llm_budget_skips[:8]))
    return "\n".join(lines) + "\n"


def main() -> None:
    args = build_parser().parse_args()
    payload = InputPayload(
        company_name=args.company_name,
        country=args.country,
        company_type_hint=args.company_type_hint,
        ticker_hint=args.ticker_hint,
        cnpj_hint=args.cnpj_hint,
        analysis_depth=args.analysis_depth,
        output_mode=args.output_mode,
        use_harvest=not args.no_harvest,
        prefer_llm=not args.no_llm_fusion,
        use_apify=not args.no_apify,
        use_browserless=not args.no_browserless,
        allow_manual_resolution=args.allow_manual_resolution,
    )
    report = run_pipeline(payload, output_dir=args.output_dir)

    run_path = _run_folder(args.output_dir, report)
    print(_format_run_summary(report, run_path), file=sys.stderr, end="")

    if args.output_mode == "report":
        print(n9_report_builder.to_markdown(report))
    elif args.output_mode in ("json", "both"):
        print(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
