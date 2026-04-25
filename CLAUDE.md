# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (OpenAI-dependent paths require the [strategy] extra)
pip install -e ".[strategy]"

# Run all tests
pytest

# Run a single test file
pytest tests/test_pipeline.py

# Full pipeline run (outputs to run/<uuid>/)
python -m peopledd.cli --company-name "Itaú Unibanco" --output-dir run --output-mode both

# Inspect pipeline contract without running (stages, artifacts, env vars, InputPayload schema)
python -m peopledd.cli --describe-run

# Validate output dir and print execution plan (no network)
python -m peopledd.cli --company-name "Acme SA" --dry-run --output-dir run

# Inspect completed runs
python -m peopledd.cli --output-dir run --list-runs
python -m peopledd.cli --output-dir run --show-run <run_id>
python -m peopledd.cli --output-dir run --diff-runs <uuid_a> <uuid_b>

# Offline calibration across multiple completed runs
python -m peopledd.tools.calibrate --runs-dir run

# REST API server (requires DATABASE_URL + Postgres)
python -m peopledd.api

# Background job worker
python -m peopledd.worker

# Full local stack (Postgres + API + worker)
docker-compose up
```

There is no configured linter or formatter.

## Architecture

**peopledd** is a Brazilian corporate governance due diligence pipeline. It fetches and reconciles governance data from CVM (formal registry) and RI (investor relations) sources, enriches people profiles, and produces structured reports with evidence packs.

### Pipeline stages (n0–n9)

Defined in `src/peopledd/nodes/`, each node is a pure function receiving/returning Pydantic models from `src/peopledd/models/contracts.py`:

| Stage | Purpose |
|-------|---------|
| `n0` | Entity resolution — CVM CSV lookup + RI URL discovery (Exa fallback) |
| `n1` | Governance ingestion — scrape RI, CVM formal, optional private web discovery (Exa) |
| `n1b` | Reconciliation — formal vs. current governance diff |
| `n1c` | Semantic fusion — multi-source name clustering + LLM judge (or rule-based fallback) |
| `n2` | Person resolution — Harvest or Exa people/company search + optional LLM disambiguation |
| `n3` | Profile enrichment — LinkedIn/social via Harvest or Exa |
| `n4` | Strategy inference — LLM extraction, optional Perplexity Sonar briefs (2 counted LLM calls) |
| Market pulse | News aggregation (Exa + SearXNG in parallel), one LLM extraction; runs after n4 |
| `n5–n7` | Capability model, coverage scoring, improvement hypotheses |
| `n8` | Evidence packing — clusters claims with evidence IDs (`C_*`, `D_*`) |
| `n9` | Report builder — final Markdown + JSON |

### Execution engine

`src/peopledd/runtime/graph_runner.py` (`GraphRunner`) orchestrates the pipeline:

- Creates `RunContext` (in `runtime/context.py`) which tracks LLM budget (`max_llm_calls=24` default), telemetry, trace, and `SourceMemoryStore`.
- Runs `validate_output_base_dir` before creating `RunContext`; on failure the CLI exits with code **2**.
- On success writes `run_summary.json` + `dd_brief.json` to `OUTPUT_DIR/<run_id>/`.
- On pipeline exception writes an emergency trace (`run_trace.json`, `run_log.json`, `run_summary.json` with `status: "error"`).
- `DefaultAdaptivePolicy` (`runtime/adaptive_policy.py`) decides recovery/retry via `RecoveryPlanner` catalog.
- Artifact inclusion per `--output-mode` is controlled entirely by `runtime/artifact_policy.py` — check `artifact_include()` and `planned_artifact_filenames()` there.

### LLM budget

Every LLM call inside the pipeline must go through `RunContext.try_consume_llm_call(channel_name)`. Calls that exceed the budget are skipped and logged in `llm_budget_skips`. The budget is tracked in telemetry as `llm_calls_used` / `llm_routes`.

### Input toggles that change routing

- `use_harvest=false` (`--no-harvest`): n2 switches to Exa People + Exa Company; n3 skips Harvest `get_profile`.
- `prefer_llm=false` (`--no-llm-fusion`): n1c skips the LLM judge and uses rule-based fusion only.
- `use_apify`, `use_browserless`: carried in `InputPayload` and persisted in `input.json`; scraper wiring is not yet centralized.

### Entry points

| Path | Role |
|------|------|
| `src/peopledd/cli.py` | CLI; uses `orchestrator.run_pipeline` for real runs |
| `src/peopledd/orchestrator.py` | Thin facade calling `run_pipeline_graph` |
| `src/peopledd/api.py` | FastAPI; `POST /jobs` queues to Postgres, `GET /runs/...` reads from `jobs` table |
| `src/peopledd/worker.py` | Claims `queued` jobs with `SKIP LOCKED`, runs pipeline, persists results to Postgres |
| `src/peopledd/jobs/store.py` | `JobStore` (psycopg3 wrapper) |

The Postgres schema lives in `migrations/001_jobs.sql` — apply once per environment before running the API or worker.

### Testing conventions

Patch nodes on `peopledd.runtime.graph_runner` (not on the node module itself) to keep tests offline and fast. See `tests/test_pipeline.py` for examples. `tests/conftest.py` provides environment isolation fixtures.

Key test files:
- `tests/test_pipeline.py` — core pipeline integration (with mocked nodes)
- `tests/test_graph_runner_artifact_write_failure.py` — error `run_summary` on artifact `OSError`
- `tests/test_run_metadata.py` — `build_run_summary`, `validate_output_mode`
- `tests/test_cli_ops.py` — CLI exit codes, `--input-json` dry-run

### Cross-run state

`SourceMemoryStore` (`runtime/source_memory.py`) persists RI surface hints across runs under `OUTPUT_DIR/_source_memory/`. It is wired from `GraphRunner` into `RunContext.source_memory` and consumed by `n1_governance_ingestion`.

### n0 CVM parsing note

`cad_cia_aberta.csv` is parsed by header name (`CNPJ_CIA`, `DENOM_SOCIAL`, etc.). If `CNPJ_CIA` is absent from the header row, the connector falls back to legacy fixed column positions and logs a warning. When `site_ri` is present in CVM data, n0 skips Exa resolution entirely.
