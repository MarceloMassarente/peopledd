# NIOSS rubric mapping (template)

This document is a **placeholder** for aligning client-specific **NIOSS** (or equivalent governance/due-diligence) dimensions to peopledd outputs.

Fill the table when your rubric is finalized. No code changes are required until dimensions are stable.

| NIOSS dimension (or ID) | Peopledd source | Artifact / field | Notes |
|-------------------------|-----------------|------------------|-------|
| *(example)* Entity clarity | Entity resolution | `entity_resolution.json`, `FinalReport.entity_resolution` | `resolution_status`, `candidate_entities` |
| *(example)* Board composition | Governance + reconciliation | `governance_reconciliation.json`, `people_resolution.json` | Dual-track vs reconciled view |
| *(example)* Capability coverage | Scoring | `coverage_scoring.json`, `degradation_profile.json` | `gap_severity`, `service_level` |
| *(example)* External narrative | Market pulse | `market_pulse.json`, report section | Use `skipped_reason` when pulse did not run |

## Related artifacts

- **`dd_brief.json`** — short executive snapshot for DD workflows (SL, high board gaps, pulse line).
- **`evidence_pack.json`** — audit trail of documents and claims.
- **`run_summary.json`** — operational snapshot including telemetry and expected artifact list.

## Roadmap

- Optional **MCP** or HTTP wrapper exposing `describe_run`, `dry_run`, and `run_pipeline` can be added outside this package if orchestration tools need a tool surface.
