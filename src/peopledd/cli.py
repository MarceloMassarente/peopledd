from __future__ import annotations

import argparse
import json

from peopledd.models.contracts import InputPayload
from peopledd.orchestrator import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run peopledd governance pipeline")
    parser.add_argument("--company-name", required=True)
    parser.add_argument("--country", default="BR")
    parser.add_argument("--company-type-hint", default="auto", choices=["auto", "listed", "private"])
    parser.add_argument("--ticker-hint", default=None)
    parser.add_argument("--cnpj-hint", default=None)
    parser.add_argument("--analysis-depth", default="standard", choices=["standard", "deep"])
    parser.add_argument("--output-mode", default="both", choices=["report", "json", "both"])
    parser.add_argument("--output-dir", default="run")
    return parser


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
    )
    report = run_pipeline(payload, output_dir=args.output_dir)
    print(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
