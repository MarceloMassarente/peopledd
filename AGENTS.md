# AGENTS.md — peopledd

Reference pipeline for the governance X-ray SPEC (nodes n0–n9). This file helps humans and coding agents work safely in the repo.

## Layout

- `src/peopledd/cli.py` — CLI entry (`peopledd` script).
- `src/peopledd/orchestrator.py` — `run_pipeline` facade.
- `src/peopledd/runtime/graph_runner.py` — policy, trace, recovery, artifact writes, `RunContext` attachment for LLM budget.
- `src/peopledd/nodes/` — n0–n9 + n1b + n1c pipeline stages.
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

## n1c semantic governance fusion (multi-source)

- Runs **after n1b**. Builds `GovernanceObservation` rows from formal + current snapshots (`governance_observation_builder`), clusters names deterministically, then fuses with an **LLM judge** (`governance_fusion_judge`, OpenAI JSON schema) when `InputPayload.prefer_llm` is true, `OPENAI_API_KEY` is set, and LLM budget allows; otherwise **rule-based fusion**. CLI: **`--no-llm-fusion`** sets `prefer_llm=false`.
- Optional **profile evidence** round: Harvest (or Exa people URLs when Harvest is off) adds synthetic observations for low-confidence / ambiguous decisions, then re-judges once.
- **`FinalReport.semantic_governance_fusion`** holds observations, candidates, `fusion_decisions`, `resolved_snapshot`, quality, and `unresolved_items`. **n2** (and n6 board/exec sizing derived from the same view) uses **`reconciliation_with_fusion_snapshot`**: the reconciled board/exec lists come from n1c’s **`resolved_snapshot`** while n1b conflict metadata is preserved on the reconciliation object.
- Artifacts: `semantic_governance_fusion.json` (when output mode includes full JSON). Evidence pack adds `C_FUSION_DEC_*` claims linked to `observation_ids` and `fusion_decision_id`.

## n0 entity resolution (CVM + RI)

- **CVM `cad_cia_aberta.csv`** is parsed by header names (`CNPJ_CIA`, `DENOM_SOCIAL`, `DENOM_COMERC`, `SIT`, `CD_CVM`, `SETOR_ATIV` or `SETOR`, plus optional columns whose header contains `site` and `ri` / `invest` / `relac` for RI URL). If `CNPJ_CIA` is missing from the header row, the client falls back to legacy fixed positions and logs a warning.
- **`setor`** from CVM is passed to **RIConnector** / Exa company lookup when the entity is uniquely resolved and CVM did not supply a non-empty **`site_ri`**. If `site_ri` is present, **n0 skips** Exa and heuristic RI resolution (no redundant API call).
- **`CanonicalEntity.exa_company_enrichment`** is set only when the RI URL came from Exa (`resolution_method == exa_company_search`); downstream nodes may reuse `website`, `description`, `exa_score`, etc., without calling Exa again for the same snapshot.

## n1 private web governance (Exa)

When the **current** track has **no** `board_members` and **no** `executive_members` after the RI scrape (or there is no `ri_url`), **n1** may fill `current_governance_snapshot` via **`private_governance_discovery`**: Exa **company** rich search (several PT governance queries), one **LLM** structured extraction (`private_web_governance_extraction` on `RunContext` budget), then optional **Exa people** validation on candidate names. Provenance: `ingestion_metadata.private_web_discovery=1`, `private_web_anchor_website`, `private_web_reason`, `private_web_source_count`. **Conselho consultivo** is routed to **committees** (`Conselho consultivo`), not CA. `GraphRunner` passes `search_orchestrator`, `website_hint` from `exa_company_enrichment`, and `InputPayload.country`. Env: `PEOPLEDd_PRIVATE_WEB_SKIP_PEOPLE_VALIDATE=1` skips parallel people checks (tests / cost control).

## Input toggles

- `use_harvest` (CLI: default on; `--no-harvest` off) is enforced in **n2** (no `search_by_name`) and **n3** (no `get_profile`). Secondary person resolution in n2 uses **Exa People** (`category=people`, highlights-only contents) plus **Exa Company** rich search (`category=company`, single `contents.text` cap 20k chars, optional `outputSchema` text — not text+highlights in one call per Exa guidance), then optional **OpenAI** disambiguation when there are 2+ LinkedIn candidates (`exa_person_profile_pick`). Env: `PEOPLEDd_EXA_PERSON_LLM=always`, `PEOPLEDd_DISABLE_EXA_PERSON_LLM=1`. SearXNG alone does not satisfy n2 secondary sourcing.
- `use_apify` and `use_browserless` are carried on `InputPayload` and persisted in `input.json`; scraper wiring is not centralized yet—treat as reserved for connector configuration.

## Tests

Prefer patching nodes on `peopledd.runtime.graph_runner` (see `tests/test_pipeline.py`) so runs stay offline and fast.
