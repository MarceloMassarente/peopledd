"""Background worker: claims jobs from Postgres and runs run_pipeline."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

from peopledd.jobs.store import JobStore
from peopledd.models.contracts import InputPayload
from peopledd.orchestrator import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _stale_running_minutes() -> int:
    raw = os.getenv("PEOPLEDD_STALE_RUNNING_MINUTES", "60")
    try:
        n = int(raw)
    except ValueError:
        return 60
    return max(0, n)


def _load_json_file(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def process_one_job(store: JobStore, output_dir: str, job_id: str, run_id: str, payload: dict) -> None:
    run_dir = Path(output_dir) / run_id
    inp = InputPayload.model_validate(payload)
    inp.run_id = run_id
    try:
        run_pipeline(inp, output_dir=output_dir)
    except BaseException as exc:
        with store.connect() as conn:
            store.mark_failed(conn, job_id, f"{type(exc).__name__}: {exc}")
            conn.commit()
        logger.exception("Job %s failed", job_id)
        return

    final_report = _load_json_file(run_dir / "final_report.json")
    dd_brief = _load_json_file(run_dir / "dd_brief.json")
    summary = _load_json_file(run_dir / "run_summary.json")
    if summary and str(summary.get("status", "")).lower() == "error":
        err = summary.get("error")
        msg = (
            err.get("message", "pipeline error")
            if isinstance(err, dict)
            else str(err or "pipeline error")
        )
        with store.connect() as conn:
            store.mark_failed(conn, job_id, msg[:8000])
            conn.commit()
        return

    if final_report is None:
        with store.connect() as conn:
            store.mark_failed(conn, job_id, "missing final_report.json after pipeline")
            conn.commit()
        return

    with store.connect() as conn:
        store.mark_succeeded(
            conn,
            job_id,
            final_report=final_report,
            dd_brief=dd_brief,
        )
        conn.commit()
    logger.info("Job %s succeeded run_id=%s", job_id, run_id)


def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error("DATABASE_URL is required")
        sys.exit(1)
    output_dir = os.getenv("PEOPLEDD_OUTPUT_DIR", "/tmp/peopledd_runs")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    poll_sec = float(os.getenv("PEOPLEDD_WORKER_POLL_SEC", "2"))
    store = JobStore(dsn)
    logger.info("Worker started output_dir=%s poll_sec=%s", output_dir, poll_sec)

    while True:
        try:
            stale_min = _stale_running_minutes()
            with store.connect() as conn:
                requeued = store.requeue_stale_running(conn, stale_min)
                conn.commit()
            if requeued:
                logger.info("Requeued %s stale running job(s)", requeued)

            with store.connect() as conn:
                job = store.claim_next_job(conn)
                conn.commit()

            if job is None:
                time.sleep(poll_sec)
                continue

            with store.connect() as conn:
                if store.refresh_cancel_flag(conn, job.job_id):
                    store.mark_cancelled(
                        conn,
                        job.job_id,
                        "cancelled before pipeline start",
                    )
                    conn.commit()
                    logger.info("Job %s cancelled before start", job.job_id)
                    continue

            logger.info(
                "Running job %s run_id=%s company=%s",
                job.job_id,
                job.run_id,
                job.input_payload.get("company_name", ""),
            )
            process_one_job(
                store,
                output_dir,
                job.job_id,
                job.run_id,
                job.input_payload,
            )
        except Exception:
            logger.exception("Worker loop error")
            time.sleep(poll_sec)


if __name__ == "__main__":
    main()
