from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from peopledd.jobs.models import JobRecord, JobStatus

logger = logging.getLogger(__name__)


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return value


def _row_to_record(row: dict[str, Any]) -> JobRecord:
    return JobRecord(
        job_id=str(row["job_id"]),
        run_id=row["run_id"],
        status=row["status"],
        owner_sub=row["owner_sub"],
        client_request_id=row["client_request_id"],
        input_payload=row["input_payload"]
        if isinstance(row["input_payload"], dict)
        else dict(row["input_payload"]),
        cancel_requested=bool(row["cancel_requested"]),
        error_message=row["error_message"],
        final_report_json=row["final_report_json"]
        if isinstance(row["final_report_json"], dict | type(None))
        else (dict(row["final_report_json"]) if row["final_report_json"] is not None else None),
        dd_brief_json=row["dd_brief_json"]
        if isinstance(row["dd_brief_json"], dict | type(None))
        else (dict(row["dd_brief_json"]) if row["dd_brief_json"] is not None else None),
        created_at=_parse_ts(row["created_at"]) or datetime.now(timezone.utc),
        started_at=_parse_ts(row["started_at"]),
        finished_at=_parse_ts(row["finished_at"]),
    )


class JobStore:
    """Synchronous Postgres access for job queue (API via thread pool, worker direct)."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def get_by_job_id(self, conn: psycopg.Connection, job_id: str) -> JobRecord | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM jobs WHERE job_id = %s::uuid",
                (job_id,),
            )
            row = cur.fetchone()
        return _row_to_record(row) if row else None

    def get_by_run_id(self, conn: psycopg.Connection, run_id: str) -> JobRecord | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM jobs WHERE run_id = %s",
                (run_id,),
            )
            row = cur.fetchone()
        return _row_to_record(row) if row else None

    def find_by_client_request(
        self,
        conn: psycopg.Connection,
        owner_sub: str | None,
        client_request_id: str,
    ) -> JobRecord | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM jobs
                WHERE client_request_id = %s
                  AND COALESCE(owner_sub, '') = COALESCE(%s, '')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (client_request_id, owner_sub),
            )
            row = cur.fetchone()
        return _row_to_record(row) if row else None

    def count_running_global(self, conn: psycopg.Connection) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS c FROM jobs WHERE status = 'running'",
            )
            row = cur.fetchone()
        return int(row["c"]) if row else 0

    def count_running_for_owner(self, conn: psycopg.Connection, owner_sub: str | None) -> int:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS c FROM jobs
                WHERE status = 'running'
                  AND COALESCE(owner_sub, '') = COALESCE(%s, '')
                """,
                (owner_sub,),
            )
            row = cur.fetchone()
        return int(row["c"]) if row else 0

    def create_job(
        self,
        conn: psycopg.Connection,
        *,
        job_id: str,
        run_id: str,
        input_payload: dict[str, Any],
        owner_sub: str | None,
        client_request_id: str | None,
    ) -> datetime:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO jobs (
                    job_id, run_id, status, owner_sub, client_request_id, input_payload
                )
                VALUES (%s::uuid, %s, 'queued', %s, %s, %s)
                RETURNING created_at
                """,
                (job_id, run_id, owner_sub, client_request_id, Json(input_payload)),
            )
            row = cur.fetchone()
        assert row is not None
        return _parse_ts(row["created_at"]) or datetime.now(timezone.utc)

    def list_jobs_for_owner(
        self,
        conn: psycopg.Connection,
        owner_sub: str | None,
        *,
        limit: int,
        offset: int,
    ) -> tuple[list[JobRecord], int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS c FROM jobs
                WHERE COALESCE(owner_sub, '') = COALESCE(%s, '')
                """,
                (owner_sub,),
            )
            total = int(cur.fetchone()["c"])
            cur.execute(
                """
                SELECT * FROM jobs
                WHERE COALESCE(owner_sub, '') = COALESCE(%s, '')
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (owner_sub, limit, offset),
            )
            rows = cur.fetchall()
        return [_row_to_record(r) for r in rows], total

    def claim_next_job(self, conn: psycopg.Connection) -> JobRecord | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH picked AS (
                    SELECT job_id FROM jobs
                    WHERE status = 'queued'
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE jobs j
                SET status = 'running',
                    started_at = NOW()
                FROM picked
                WHERE j.job_id = picked.job_id
                RETURNING j.*;
                """
            )
            row = cur.fetchone()
        if not row:
            return None
        return _row_to_record(row)

    def refresh_cancel_flag(self, conn: psycopg.Connection, job_id: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cancel_requested FROM jobs WHERE job_id = %s::uuid",
                (job_id,),
            )
            row = cur.fetchone()
        return bool(row["cancel_requested"]) if row else False

    def mark_succeeded(
        self,
        conn: psycopg.Connection,
        job_id: str,
        *,
        final_report: dict[str, Any] | None,
        dd_brief: dict[str, Any] | None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status = 'succeeded',
                    finished_at = NOW(),
                    final_report_json = %s,
                    dd_brief_json = %s,
                    error_message = NULL,
                    cancel_requested = FALSE
                WHERE job_id = %s::uuid
                """,
                (
                    Json(final_report) if final_report is not None else None,
                    Json(dd_brief) if dd_brief is not None else None,
                    job_id,
                ),
            )

    def requeue_stale_running(self, conn: psycopg.Connection, max_minutes: int) -> int:
        """Move long-running rows back to queued (worker crash). Returns rows updated."""
        if max_minutes <= 0:
            return 0
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status = 'queued',
                    started_at = NULL
                WHERE status = 'running'
                  AND started_at IS NOT NULL
                  AND started_at < NOW() - (%s * INTERVAL '1 minute')
                """,
                (max_minutes,),
            )
            n = cur.rowcount
        return int(n)

    def mark_failed(self, conn: psycopg.Connection, job_id: str, message: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status = 'failed',
                    finished_at = NOW(),
                    error_message = %s
                WHERE job_id = %s::uuid
                """,
                (message[:8000], job_id),
            )

    def mark_cancelled(self, conn: psycopg.Connection, job_id: str, reason: str | None = None) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status = 'cancelled',
                    finished_at = NOW(),
                    error_message = COALESCE(%s, 'cancelled')
                WHERE job_id = %s::uuid AND status IN ('queued', 'running')
                """,
                (reason, job_id),
            )

    def request_cancel(
        self,
        conn: psycopg.Connection,
        job_id: str,
        owner_sub: str | None,
    ) -> bool:
        """Queued jobs become cancelled immediately; running jobs get cancel_requested."""
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs SET
                    status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
                    cancel_requested = CASE
                        WHEN status = 'running' THEN TRUE
                        ELSE cancel_requested
                    END,
                    finished_at = CASE WHEN status = 'queued' THEN NOW() ELSE finished_at END,
                    error_message = CASE WHEN status = 'queued' THEN 'cancelled' ELSE error_message END
                WHERE job_id = %s::uuid
                  AND COALESCE(owner_sub, '') = COALESCE(%s, '')
                  AND status IN ('queued', 'running')
                RETURNING job_id
                """,
                (job_id, owner_sub),
            )
            return cur.fetchone() is not None

    def owner_matches(self, record: JobRecord, owner_sub: str | None) -> bool:
        a = record.owner_sub or ""
        b = owner_sub or ""
        return a == b
