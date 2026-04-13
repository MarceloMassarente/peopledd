# AGENTS.md — peopledd

Reference pipeline for the governance X-ray SPEC (nodes n0–n9). This file helps humans and coding agents work safely in the repo.

## Layout

- `src/peopledd/cli.py` — CLI entry (`peopledd` script). Supports `--describe-run`, `--dry-run` (validates `--output-dir` only here), `--input-json`, `--list-runs`, `--show-run`, `--diff-runs`. On `OutputDirectoryError`, exits with code **2**. Real runs rely on validation inside `run_pipeline_graph` (single disk probe). If both `--describe-run` and `--dry-run` are passed, **only `--describe-run` runs**.
- `src/peopledd/orchestrator.py` — `run_pipeline` facade.
- `src/peopledd/runtime/graph_runner.py` — policy, trace, recovery, artifact writes, `RunContext` attachment for LLM budget. Calls `validate_output_base_dir` before `RunContext.create`. On success: `run_summary.json` + `dd_brief.json`. On pipeline exception: `_write_emergency_trace` writes `run_trace.json`, `run_log.json`, and `run_summary.json` with `status: "error"`. On `OSError` during artifact writes: best-effort error `run_summary.json` then re-raises.
- `src/peopledd/runtime/artifact_policy.py` — `validate_output_mode`, `artifact_include`, `planned_artifact_filenames`, `DD_BRIEF_FILENAME`, `pipeline_stage_ids`, `REPORT_ARTIFACT_KEYS`.
- `src/peopledd/runtime/run_metadata.py` — `build_run_summary`, `build_dd_brief`, `build_error_run_summary`, `describe_run_payload`, `format_dry_run_plan`, env hints.
- `src/peopledd/runtime/run_inspect.py` — `list_runs`, `read_run_summary`, `diff_runs` (used by CLI).
- `src/peopledd/tools/calibrate.py` — offline calibration over completed runs: scans `--runs-dir/<run_id>/final_report.json`, groups `pipeline_telemetry.adaptive_decisions` by `gap_kind` (if present) or `checkpoint`, plus `action`, and writes `calibration_report.json` + `calibration_report.md` under `--output-dir` (defaults to `--runs-dir`). Does not mutate runs. Example: `python -m peopledd.tools.calibrate --runs-dir run`.
- `src/peopledd/runtime/source_memory.py` — `SourceMemoryStore` under `OUTPUT_DIR/_source_memory/` (cross-run hints for RI surfaces). Wired from `run_pipeline_graph` into `RunContext.source_memory` and `n1_governance_ingestion` via attached context.
- `src/peopledd/runtime/recovery_planner.py` — `RecoveryPlanner` catalog used by `DefaultAdaptivePolicy` for `decide_*` recovery branches.
- `src/peopledd/utils/io.py` — `validate_output_base_dir` / `OutputDirectoryError` (writable probe), `write_json`, `write_text`.
- `src/peopledd/nodes/` — n0–n9 + n1b + n1c pipeline stages.
- `src/peopledd/services/` — connectors, Harvest, `market_pulse_retriever.py`, `perplexity_sonar.py` (Sonar Pro), strategy/RI LLM paths.
- `src/peopledd/vendor/` — scraper, search (planner/selector LLM via httpx), document store.
- `models/contracts.py` — Pydantic payloads and `FinalReport`.

## Running

```bash
pip install -e ".[strategy]"   # optional: OpenAI for strategy/search/RI extraction
pytest
python -m peopledd.cli --company-name "Example" --output-dir run
```

Use an **absolute** `--output-dir` in automation so runs do not depend on process cwd.

### CLI inspection (no network)

```bash
python -m peopledd.cli --describe-run    # JSON: stages, artifacts by mode, env hints, InputPayload schema
python -m peopledd.cli --company-name "X" --dry-run --output-dir run --output-mode report
```

`--company-name` is optional with `--describe-run`, `--list-runs`, `--show-run`, or `--diff-runs`. For a normal run it is required **unless** `--input-json` is set.

### `--input-json`

Load `InputPayload` from a UTF-8 JSON file. Overrides apply when the corresponding flag appears on the command line: `--company-name`, `--country` (only if `--country` is present), `--company-type-hint`, `--analysis-depth`, `--output-mode`, `ticker_hint` / `cnpj_hint` when not null, and boolean toggles `--no-harvest`, `--no-llm-fusion`, `--no-apify`, `--no-browserless`, `--allow-manual-resolution`.

### After a successful run

Each run folder `OUTPUT_DIR/<run_id>/` includes **`run_summary.json`** and **`dd_brief.json`**. The CLI prints a short summary to **stderr** (paths, `run_id`, pulse skip reason when set).

### After a failed run

When the pipeline raises before finishing, check **`run_summary.json`** for `status: "error"`, `error`, and `error_phase` (last trace node). Partial artifacts may exist.

## Environment

See `.env.example`. Common keys: `OPENAI_API_KEY`, `EXA_API_KEY`, `HARVEST_API_KEY`, `SEARXNG_URL`, `BROWSERLESS_*`, `JINA_API_KEY`. Optional: `PERPLEXITY_API_KEY` (two Sonar Pro calls in n4, counted on `RunContext` LLM budget), `OPENAI_MARKET_PULSE_MODEL`.

Authoritative structured list: run `peopledd --describe-run` and read `environment_variables` in the JSON.

## LLM budget

`RunContext.max_llm_calls` (default 24) limits counted LLM calls during a single `run_pipeline` when context is attached (always in `GraphRunner`). Telemetry includes `llm_calls_used`, `llm_budget_skips`, and `llm_routes` (per-channel used_llm / reason).

With `PERPLEXITY_API_KEY` set, n4’s first pass adds **two** counted calls (`perplexity_sonar_recent_facts`, `perplexity_sonar_sector_context`) before `strategy_extraction`; retries skip Sonar and reuse briefs from the first pass when the retry returns empty `external_sonar_briefs`.

## Market pulse (after n4, before n5)

- **`services/market_pulse_retriever.py`** runs immediately after strategy inference (and adaptive n4 retries). It issues **deterministic PT-BR queries** (more queries when `InputPayload.analysis_depth=deep`), fetches **SearXNG** + **Exa** `category=news` in parallel per query (no URL planner), dedupes URLs, then **one** OpenAI **`market_pulse`** extraction on `RunContext` LLM budget (`try_consume_llm_call("market_pulse")`) with strict JSON schema. Claims must cite URLs from the retrieved set only.
- **`FinalReport.market_pulse`**: `claims`, `source_hits`, `queries_used`, optional `skipped_reason` (`no_api_keys`, `no_results`, `budget_exhausted`, `llm_error`, `no_search_orchestrator` when `GraphRunner` has no `search_orchestrator`).
- **Artifacts:** `market_pulse.json` (full JSON output modes). **n8** adds `D_MARKET_PULSE`, per-hit `D_MKT_*`, and `C_MARKET_*` claims (`claim_type=market_pulse`). **n9** appends subsection “Pulse de mercado (mídia pública)” under estratégia and a short “Limites desta execução” block in the executive section (degradation, pulse, LLM skips).
- **Env:** `EXA_API_KEY` and/or `SEARXNG_URL` required for collection; `OPENAI_API_KEY` for structured claims. Optional override: `OPENAI_MARKET_PULSE_MODEL` (defaults to `gpt-5.4`).

## Output modes

`InputPayload.output_mode`: `both` writes all JSON + `final_report.md`; `json` omits markdown; `report` writes a lean artifact set (input, trace, log, degradation, final JSON + MD). Logic lives in `runtime/artifact_policy.py`.

**Always** (all modes): `run_summary.json` and `dd_brief.json` are written on successful completion. Invalid `output_mode` raises `ValueError` via `validate_output_mode` at the start of `_run_pipeline`.

## n1c semantic governance fusion (multi-source)

- Runs **after n1b**. Builds `GovernanceObservation` rows from formal + current snapshots (`governance_observation_builder`), clusters names deterministically, then fuses with an **LLM judge** (`governance_fusion_judge`, OpenAI JSON schema) when `OPENAI_API_KEY` is set, `InputPayload.prefer_llm` is true, and LLM budget allows; otherwise **rule-based fusion**. CLI: `--no-llm-fusion` sets `prefer_llm=false`.
- Optional **profile evidence** round: Harvest (or Exa people URLs when Harvest is off) adds synthetic observations for low-confidence / ambiguous decisions, then re-judges once.
- **`FinalReport.semantic_governance_fusion`** holds observations, candidates, `fusion_decisions`, `resolved_snapshot`, quality, and `unresolved_items`. **n2 still resolves people from `governance_reconciliation`** by default (backward compatible).
- Artifacts: `semantic_governance_fusion.json` (when output mode includes full JSON). Evidence pack adds `C_FUSION_DEC_*` claims linked to `observation_ids` and `fusion_decision_id`.

## n0 entity resolution (CVM + RI)

- **CVM `cad_cia_aberta.csv`** is parsed by header names (`CNPJ_CIA`, `DENOM_SOCIAL`, `DENOM_COMERC`, `SIT`, `CD_CVM`, `SETOR_ATIV` or `SETOR`, plus optional columns whose header contains `site` and `ri` / `invest` / `relac` for RI URL). If `CNPJ_CIA` is missing from the header row, the client falls back to legacy fixed positions and logs a warning.
- **`setor`** from CVM is passed to **RIConnector** / Exa company lookup when the entity is uniquely resolved and CVM did not supply a non-empty **`site_ri`**. If `site_ri` is present, **n0 skips** Exa and heuristic RI resolution (no redundant API call).
- **`CanonicalEntity.exa_company_enrichment`** is set only when the RI URL came from Exa (`resolution_method == exa_company_search`); downstream nodes may reuse `website`, `description`, `exa_score`, etc., without calling Exa again for the same snapshot.

## n1 private web governance (Exa)

When the **current** track has **no** `board_members` and **no** `executive_members` after the RI scrape (or there is no `ri_url`), **n1** may fill `current_governance_snapshot` via **`private_governance_discovery`**: Exa **company** rich search (several PT governance queries), one **LLM** structured extraction (`private_web_governance_extraction` on `RunContext` budget), then optional **Exa people** validation on candidate names. Provenance: `ingestion_metadata.private_web_discovery=1`, `private_web_anchor_website`, `private_web_reason`, `private_web_source_count`. **Conselho consultivo** is routed to **committees** (`Conselho consultivo`), not CA. `GraphRunner` passes `search_orchestrator`, `website_hint` from `exa_company_enrichment`, and `InputPayload.country`. Env: `PEOPLEDd_PRIVATE_WEB_SKIP_PEOPLE_VALIDATE=1` skips parallel people checks (tests / cost control).

## Input toggles

- `use_harvest` (CLI: default on; `--no-harvest` off) is enforced in **n2** (no `search_by_name`) and **n3** (no `get_profile`). Secondary person resolution in n2 uses **Exa People** (`category=people`, highlights-only contents) plus **Exa Company** rich search (`category=company`, single `contents.text` cap 20k chars, optional `outputSchema` text — not text+highlights in one call per Exa guidance), then optional **OpenAI** disambiguation when there are 2+ LinkedIn candidates (`exa_person_profile_pick`). Env: `PEOPLEDd_EXA_PERSON_LLM=always`, `PEOPLEDd_DISABLE_EXA_PERSON_LLM=1`. SearXNG alone does not satisfy n2 secondary sourcing.
- `prefer_llm` (CLI: `--no-llm-fusion` off) controls whether **n1c** may use the LLM judge; persisted in `input.json`.
- `use_apify` and `use_browserless` are carried on `InputPayload` and persisted in `input.json`; scraper wiring is not centralized yet—treat as reserved for connector configuration.

## Tests

Prefer patching nodes on `peopledd.runtime.graph_runner` (see `tests/test_pipeline.py`) so runs stay offline and fast. `tests/test_run_metadata.py` covers metadata and `validate_output_mode`. `tests/test_cli_ops.py` covers CLI exit codes and `--input-json` dry-run. `tests/test_run_inspect.py` covers list/show/diff. `tests/test_graph_runner_artifact_write_failure.py` covers error `run_summary` on artifact `OSError`.

Docs: `docs/DUE_DILIGENCE_CHARTER.md`, `docs/NIOSS_MAPPING.md` (template), `docs/GOLD_SET.md`.
