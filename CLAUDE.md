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

# Offline health aggregate across runs (failure phases, checkpoint usage, durations)
python -m peopledd.tools.runs_health --runs-dir run

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

Nodes must not import `RunContext`, `write_json`, or `write_text` directly — use `peopledd.runtime.pipeline_context` for budget/telemetry. This contract is enforced by `tests/test_node_purity.py` (AST-based).

### Execution engine

The pipeline is now split across several `runtime/` modules:

- `graph_runner.py` (`GraphRunner`) — facade: circuit logging, emergency/error summaries, delegates macro-phase work to `runtime/phases/*` and linear orchestration to `pipeline_linear.py`. Also exposes `run_batch` (via `batch_runner.py`).
- `pipeline_linear.py` (`execute_linear_pipeline`) — checkpoint resume, `phase_begin`/`phase_end` wrappers, optional post-strategy checkpoint when `PEOPLEDD_POST_STRATEGY_CHECKPOINT` is set.
- `runtime/phases/` — four callable modules (`governance_phase`, `people_phase`, `strategy_phase`, `scoring_phase`), each `run(runner, ...)`.
- `artifact_writer.py` — `write_success_pipeline_artifacts` (called only from scoring phase).
- `pipeline_merge.py` — strategy/people merge helpers used by phases.
- `pipeline_state.py` — `PipelineState`, checkpoint read/write/fingerprint.
- `run_limits.py` — `resolve_run_limits`, `env_post_strategy_checkpoint`.
- `batch_runner.py` — `run_pipeline_batch` for concurrent multi-company runs.

`GraphRunner` calls `validate_output_base_dir` before `RunContext.create`; on failure the CLI exits with code **2**. On success writes `run_summary.json` + `dd_brief.json`. On pipeline exception writes `run_trace.json`, `run_log.json`, `run_summary.json` with `status: "error"`.

### LLM budget

Every LLM call inside the pipeline must go through `RunContext.try_consume_llm_call(channel_name)`. Budget defaults to `max_llm_calls=24`; override via `InputPayload.max_llm_calls` or env `PEOPLEDD_MAX_LLM_CALLS`. Recovery budget: `PEOPLEDD_MAX_RECOVERY_STEPS` (default 8). Telemetry includes `llm_calls_used`, `llm_budget_skips`, `llm_routes`, and `per_phase_durations_ms`.

### Checkpoint / resume

After the people phase, `pipeline_linear.py` writes a `checkpoint.json` under the run folder. If `PEOPLEDD_POST_STRATEGY_CHECKPOINT` is set, a second checkpoint is written after strategy. On re-run with the same `run_id`, if the input fingerprint matches, the pipeline resumes from the saved phase — skipping governance + people (or governance + people + strategy) work.

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

### Artifact inclusion

Controlled entirely by `runtime/artifact_policy.py` (`artifact_include`, `planned_artifact_filenames`). `run_summary.json` and `dd_brief.json` are always written on success regardless of `--output-mode`. Invalid `output_mode` raises `ValueError` at the start of `execute_linear_pipeline`.

### Testing conventions

Patch nodes on `peopledd.runtime.graph_runner` (not on the node module itself) to keep tests offline and fast. See `tests/test_pipeline.py` for examples. `tests/conftest.py` provides environment isolation fixtures.

Key test files:
- `tests/test_pipeline.py` — core pipeline integration (with mocked nodes)
- `tests/test_node_purity.py` — AST guardrail: nodes must not import runtime I/O
- `tests/test_pipeline_state.py` — checkpoint read/write/fingerprint
- `tests/test_graph_runner_artifact_write_failure.py` — error `run_summary` on artifact `OSError`
- `tests/test_run_metadata.py` — `build_run_summary`, `validate_output_mode`
- `tests/test_cli_ops.py` — CLI exit codes, `--input-json` dry-run
- `tests/test_runs_health.py` — `runs_health` tool output

### Cross-run state

`SourceMemoryStore` (`runtime/source_memory.py`) persists RI surface hints across runs under `OUTPUT_DIR/_source_memory/`. It is wired from `GraphRunner` into `RunContext.source_memory` and consumed by `n1_governance_ingestion`.

### n0 CVM parsing note

`cad_cia_aberta.csv` is parsed by header name (`CNPJ_CIA`, `DENOM_SOCIAL`, etc.). If `CNPJ_CIA` is absent from the header row, the connector falls back to legacy fixed column positions and logs a warning. When `site_ri` is present in CVM data, n0 skips Exa resolution entirely.
