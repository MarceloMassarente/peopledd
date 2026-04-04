# AGENTS.md — peopledd

Reference pipeline for the governance X-ray SPEC (nodes n0–n9). This file helps humans and coding agents work safely in the repo.

## Layout

- `src/peopledd/cli.py` — CLI entry (`peopledd` script).
- `src/peopledd/orchestrator.py` — `run_pipeline` facade.
- `src/peopledd/runtime/graph_runner.py` — policy, trace, recovery, artifact writes, `RunContext` attachment for LLM budget.
- `src/peopledd/nodes/` — n0–n9 + n1b pipeline stages.
- `src/peopledd/services/` — connectors, Harvest, `perplexity_sonar.py` (Sonar Pro), strategy/RI LLM paths.
- `src/peopledd/vendor/` — scraper, search (planner/selector LLM via httpx), document store.
- `models/contracts.py` — Pydantic payloads and `FinalReport`.

## Running

```bash
pip install -e ".[strategy]"   # optional: OpenAI for strategy/search/RI extraction
pytest
python -m peopledd.cli --company-name "Example" --output-dir run
```

Use an **absolute** `--output-dir` in automation so runs do not depend on process cwd.

## Environment

See `.env.example`. Common keys: `OPENAI_API_KEY`, `EXA_API_KEY`, `HARVEST_API_KEY`, `SEARXNG_URL`, `BROWSERLESS_*`, `JINA_API_KEY`. Optional: `PERPLEXITY_API_KEY` (two Sonar Pro calls in n4, counted on `RunContext` LLM budget).

## LLM budget

`RunContext.max_llm_calls` (default 24) limits counted LLM calls during a single `run_pipeline` when context is attached (always in `GraphRunner`). Telemetry includes `llm_calls_used`, `llm_budget_skips`, and `llm_routes` (per-channel used_llm / reason).

With `PERPLEXITY_API_KEY` set, n4’s first pass adds **two** counted calls (`perplexity_sonar_recent_facts`, `perplexity_sonar_sector_context`) before `strategy_extraction`; retries skip Sonar and reuse briefs from the first pass when the retry returns empty `external_sonar_briefs`.

## Output modes

`InputPayload.output_mode`: `both` writes all JSON + `final_report.md`; `json` omits markdown; `report` writes a lean artifact set (input, trace, log, degradation, final JSON + MD).

## Input toggles

- `use_harvest` (CLI: default on; `--no-harvest` off) is enforced in **n2** (no `search_by_name`) and **n3** (no `get_profile`). Secondary person resolution in n2 uses **Exa People** (`category=people`, highlights-only contents) plus **Exa Company** rich search (`category=company`, single `contents.text` cap 20k chars, optional `outputSchema` text — not text+highlights in one call per Exa guidance), then optional **OpenAI** disambiguation when there are 2+ LinkedIn candidates (`exa_person_profile_pick`). Env: `PEOPLEDd_EXA_PERSON_LLM=always`, `PEOPLEDd_DISABLE_EXA_PERSON_LLM=1`. SearXNG alone does not satisfy n2 secondary sourcing.
- `use_apify` and `use_browserless` are carried on `InputPayload` and persisted in `input.json`; scraper wiring is not centralized yet—treat as reserved for connector configuration.

## Tests

Prefer patching nodes on `peopledd.runtime.graph_runner` (see `tests/test_pipeline.py`) so runs stay offline and fast.
