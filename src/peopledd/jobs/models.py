from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    run_id: str
    status: JobStatus
    owner_sub: str | None
    client_request_id: str | None
    input_payload: dict[str, Any]
    cancel_requested: bool
    error_message: str | None
    final_report_json: dict[str, Any] | None
    dd_brief_json: dict[str, Any] | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
