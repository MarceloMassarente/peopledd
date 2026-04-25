from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.utils.io import ensure_dir, validate_output_base_dir


def run_pipeline_batch(
    payloads: list[InputPayload],
    output_dir: str,
    *,
    concurrency: int = 3,
    resume_on_failure: bool = True,
) -> list[FinalReport | Exception]:
    """
    Run multiple InputPayloads under output_dir (each gets its own run subfolder).

    resume_on_failure does not auto-retry failed items; when True, operators may re-run a failed
    company with the same InputPayload.run_id so post-people checkpoint.json applies (fingerprint
    must still match the payload).
    """
    from peopledd.runtime.graph_runner import run_pipeline_graph

    validate_output_base_dir(output_dir)
    seen_ids: dict[str, int] = {}
    for i, p in enumerate(payloads):
        if p.run_id is None:
            continue
        if p.run_id in seen_ids:
            raise ValueError(
                f"duplicate run_id {p.run_id!r} at indices {seen_ids[p.run_id]} and {i}"
            )
        seen_ids[p.run_id] = i

    out_root = Path(output_dir)
    ensure_dir(out_root)
    results: list[FinalReport | Exception | None] = [None] * len(payloads)

    def _one(idx: int, payload: InputPayload) -> tuple[int, FinalReport | Exception]:
        try:
            report = run_pipeline_graph(payload, output_dir=output_dir)
            return idx, report
        except Exception as e:
            return idx, e

    workers = max(1, min(concurrency, len(payloads)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_one, i, p): i for i, p in enumerate(payloads)}
        for fut in as_completed(futs):
            idx, item = fut.result()
            results[idx] = item

    batch_rows: list[dict[str, Any]] = []
    for i, p in enumerate(payloads):
        item = results[i]
        row: dict[str, Any] = {
            "index": i,
            "company_name": p.company_name,
            "run_id": p.run_id,
        }
        if isinstance(item, FinalReport):
            tel = item.pipeline_telemetry
            row["status"] = "ok"
            row["run_id_resolved"] = tel.run_id if tel else None
            row["service_level"] = item.degradation_profile.service_level.value
        elif isinstance(item, Exception):
            row["status"] = "error"
            row["error"] = f"{type(item).__name__}: {item}"
        else:
            row["status"] = "unknown"
        batch_rows.append(row)

    summary_path = out_root / "batch_summary.json"
    try:
        summary_path.write_text(
            json.dumps(
                {
                    "runs": batch_rows,
                    "resume_on_failure": resume_on_failure,
                    "note": (
                        "resume_on_failure is manual: re-run the same run_id with matching "
                        "payload fingerprint to use checkpoint.json after a partial failure."
                    ),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except OSError:
        pass

    out: list[FinalReport | Exception] = []
    for i, r in enumerate(results):
        if r is None:
            raise RuntimeError("missing batch slot")
        out.append(r)
    return out
