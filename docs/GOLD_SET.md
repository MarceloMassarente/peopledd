# Gold set (manual regression)

Use a small set of real or anonymized companies to regression-test the pipeline when connectors or models change.

## Suggested size

5–8 companies covering:

- Listed BR with strong CVM + RI
- Listed BR with weak or missing RI
- Private or unresolved entity
- At least one sector with distinct governance vocabulary (e.g. financials vs. industrials)

## How to record

For each company, store:

- Input `InputPayload` (or CLI invocation) used
- Expected **resolution status** band (resolved / partial / not_found)
- Expected **service level** band when fixtures are stable
- Links to golden **artifact hashes** or key JSON paths (optional)

## Automation

Offline tests should continue to **patch** `peopledd.runtime.graph_runner` (see `tests/test_pipeline.py`). Full gold-set replay against live APIs belongs in a separate gated job, not in default CI.
