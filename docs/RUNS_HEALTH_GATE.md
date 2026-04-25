# H1 gate: runs health before structural changes

After deploying telemetry (`per_phase_durations_ms`, `checkpoint` in `run_summary.json`, trace `phase_begin`/`phase_end`), operators should collect production or staging data before expanding H2 further.

## Minimum criteria (from roadmap)

1. At least **30** completed runs under a stable `OUTPUT_DIR` (or equivalent) with the new `run_summary` shape.
2. Run `python -m peopledd.tools.runs_health --runs-dir <dir>` and review `runs_health.md`.
3. Identify the **top 2–3** `error_phase` buckets (for `status=error`) and the macro-phase with highest `avg_ms` or LLM cost.

## Command

```bash
python -m peopledd.tools.runs_health --runs-dir run --output-dir run
```

Writes `runs_health.json` and `runs_health.md` (default output dir is the runs dir). Read-only on individual run folders.

## Decision

If failures cluster **after** `post_people` (strategy/scoring), consider enabling `PEOPLEDD_POST_STRATEGY_CHECKPOINT` and validating resume paths. If not, keep a single checkpoint and record the decision in `ARCHITECTURE_DECISIONS.md`.
