# Due diligence charter (peopledd)

## Purpose

**peopledd** supports **organizational and governance due diligence** by assembling a structured view of a company from regulatory filings (CVM), investor relations (RI), public web sources, and optional media pulse. It is a **batch pipeline** with explicit **service levels (SL1–SL5)** and **degradation signals**, not a real-time surveillance or compliance certification system.

## What this tool does

- Resolves the legal entity where possible and records ambiguity when not.
- Ingests **formal** governance (e.g. FRE-style) and **current** governance (RI scrape) and reconciles them.
- Scores coverage of board and executive capabilities against inferred requirements.
- Produces an **evidence pack** with document and claim references suitable for human review.
- Writes **machine-readable artifacts** per run (`final_report.json`, `run_summary.json`, `dd_brief.json`, traces) for automation and audit.

## What this tool does not do

- **Investment, legal, or credit advice** — outputs are informational; reviewers must apply their own policies and professional judgment.
- **Substitute for KYC/AML or regulated filings** — it does not certify accuracy of third-party data or replace official registers.
- **Guarantee completeness of media or web narrative** — market pulse uses bounded search and LLM extraction; see `skipped_reason` and disclaimers in the report when collection or budget limits apply.
- **Operate as a general conversational agent** — interaction is via CLI, library API, and files on disk.

## Use of market pulse

Public-media claims are **illustrative** of narrative context relative to official strategy text. They are **not** consensus sell-side research, price-sensitive inside information, or verified factual findings. Always validate URLs and sources before regulatory or investment use.

## Data handling

Operators are responsible for **retention**, **access control**, and **privacy** of run folders and API keys. The pipeline may persist company names, URLs, and extracted text in run artifacts.

## Success and failure

- **`run_summary.json` with `status: "ok"`** indicates the pipeline finished and wrote the success artifact set for the chosen `output_mode`.
- **`status: "error"`** indicates an exception or artifact write failure; see `error` and `error_phase` fields. Partial files may exist in the run directory; treat as **not** a completed DD pack until re-run or manual review.

For the full technical contract, run `peopledd --describe-run`.
