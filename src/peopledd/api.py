"""REST API: stateless job submission (Postgres) + optional filesystem reads."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field
from psycopg.errors import UniqueViolation

from peopledd.jobs.models import JobRecord
from peopledd.jobs.store import JobStore
from peopledd.models.contracts import InputPayload
from peopledd.runtime.run_inspect import diff_runs, list_runs

logger = logging.getLogger(__name__)

_output_dir: str = os.getenv("PEOPLEDD_OUTPUT_DIR", "/tmp/peopledd_runs")
_bearer = HTTPBearer(auto_error=False)


def _max_concurrent_global() -> int:
    return int(os.getenv("PEOPLEDD_MAX_CONCURRENT_GLOBAL", "12"))


def _max_concurrent_per_user() -> int:
    return int(os.getenv("PEOPLEDD_MAX_CONCURRENT_PER_USER", "2"))


def _api_key_expected() -> str | None:
    v = os.getenv("PEOPLEDD_API_KEY")
    return v if v and v.strip() else None


def _allow_legacy_unauth() -> bool:
    return os.getenv("PEOPLEDD_ALLOW_LEGACY_UNAUTH", "").lower() in ("1", "true", "yes")


def _database_url() -> str | None:
    u = os.getenv("DATABASE_URL")
    return u if u and u.strip() else None


def get_job_store() -> JobStore:
    dsn = _database_url()
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="DATABASE_URL is not configured",
        )
    return JobStore(dsn)


async def require_db_store() -> JobStore:
    return await asyncio.to_thread(get_job_store)


async def verify_service_auth(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    expected = _api_key_expected()
    if expected is None:
        return
    if creds is None or creds.credentials != expected:
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")


async def verify_service_auth_or_legacy(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> None:
    if _allow_legacy_unauth() and _api_key_expected() is None:
        return
    await verify_service_auth(creds)


def _resolve_owner_sub(
    body_owner: str | None,
    header_user: str | None,
) -> str | None:
    if body_owner is not None and body_owner.strip():
        return body_owner.strip()
    if header_user is not None and header_user.strip():
        return header_user.strip()
    return None


def _resolve_owner_for_create(body_owner: str | None, header_user: str | None) -> str | None:
    """When PEOPLEDD_API_KEY is set, X-User-Subject is the only trusted identity."""
    if _api_key_expected() is None:
        return _resolve_owner_sub(body_owner, header_user)
    hdr = header_user.strip() if header_user and header_user.strip() else None
    body = body_owner.strip() if body_owner and body_owner.strip() else None
    if hdr is None:
        raise HTTPException(
            status_code=400,
            detail="X-User-Subject is required when API key is enabled",
        )
    if body is not None and body != hdr:
        raise HTTPException(
            status_code=400,
            detail="owner_sub must match X-User-Subject when both are sent",
        )
    return hdr


class AnalysisRequest(BaseModel):
    company_name: str
    country: str = Field(default="BR")
    company_type_hint: str = Field(default="auto")
    ticker_hint: str | None = None
    cnpj_hint: str | None = None
    analysis_depth: str = Field(default="standard")
    output_mode: str = Field(default="both")
    use_harvest: bool = True
    prefer_llm: bool = True
    use_apify: bool = True
    use_browserless: bool = True
    allow_manual_resolution: bool = False


class JobCreateRequest(AnalysisRequest):
    owner_sub: str | None = Field(
        default=None,
        description="Optional mirror of X-User-Subject; must match header when API key is enabled.",
    )
    client_request_id: str | None = Field(
        default=None,
        description="Idempotency key; repeated value returns the same job_id.",
    )


class JobCreatedResponse(BaseModel):
    job_id: str
    run_id: str
    status: str = "queued"
    message: str
    created_at: str


class JobStatusResponse(BaseModel):
    job_id: str
    run_id: str
    status: str
    owner_sub: str | None = None
    cancel_requested: bool = False
    error_message: str | None = None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None


class RunListResponse(BaseModel):
    runs: list[dict[str, Any]]
    count: int


class DiffResponse(BaseModel):
    comparison: dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    version: str = "0.2.0"
    timestamp: str
    database_configured: bool


def _build_payload(req: AnalysisRequest) -> InputPayload:
    return InputPayload(
        company_name=req.company_name,
        country=req.country,
        company_type_hint=req.company_type_hint,
        ticker_hint=req.ticker_hint,
        cnpj_hint=req.cnpj_hint,
        analysis_depth=req.analysis_depth,
        output_mode=req.output_mode,
        use_harvest=req.use_harvest,
        prefer_llm=req.prefer_llm,
        use_apify=req.use_apify,
        use_browserless=req.use_browserless,
        allow_manual_resolution=req.allow_manual_resolution,
    )


def _record_to_status(rec: JobRecord) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=rec.job_id,
        run_id=rec.run_id,
        status=rec.status,
        owner_sub=rec.owner_sub,
        cancel_requested=rec.cancel_requested,
        error_message=rec.error_message,
        created_at=rec.created_at.isoformat(),
        started_at=rec.started_at.isoformat() if rec.started_at else None,
        finished_at=rec.finished_at.isoformat() if rec.finished_at else None,
    )


def _get_job_for_owner(
    store: JobStore,
    job_id: str,
    owner_sub: str | None,
) -> JobRecord:
    with store.connect() as conn:
        rec = store.get_by_job_id(conn, job_id)
        if rec is None or not store.owner_matches(rec, owner_sub):
            raise HTTPException(status_code=404, detail="job not found")
        return rec


def _get_run_for_owner(
    store: JobStore,
    run_id: str,
    owner_sub: str | None,
) -> JobRecord:
    with store.connect() as conn:
        rec = store.get_by_run_id(conn, run_id)
        if rec is None or not store.owner_matches(rec, owner_sub):
            raise HTTPException(status_code=404, detail="run not found")
        return rec


def _json_result_for_job(rec: JobRecord) -> dict[str, Any]:
    if rec.final_report_json is not None:
        return rec.final_report_json
    path = Path(_output_dir) / rec.run_id / "final_report.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="result not available yet")


def _json_brief_for_job(rec: JobRecord) -> dict[str, Any]:
    if rec.dd_brief_json is not None:
        return rec.dd_brief_json
    path = Path(_output_dir) / rec.run_id / "dd_brief.json"
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="brief not available")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path(_output_dir).mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", _output_dir)
    yield


app = FastAPI(
    title="peopledd API",
    description="REST API for governance pipeline jobs (Postgres queue + worker)",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc).isoformat(),
        database_configured=_database_url() is not None,
    )


def enqueue_job(
    store: JobStore,
    payload_dict: dict[str, Any],
    owner_sub: str | None,
    client_request_id: str | None,
) -> tuple[str, str, datetime, bool]:
    """Returns job_id, run_id, created_at, reused_existing."""
    with store.connect() as conn:
        if client_request_id:
            existing = store.find_by_client_request(conn, owner_sub, client_request_id)
            if existing is not None:
                conn.commit()
                return (
                    existing.job_id,
                    existing.run_id,
                    existing.created_at,
                    True,
                )

        if store.count_running_global(conn) >= _max_concurrent_global():
            conn.rollback()
            raise HTTPException(
                status_code=429,
                detail="global concurrent job limit reached",
            )
        if store.count_running_for_owner(conn, owner_sub) >= _max_concurrent_per_user():
            conn.rollback()
            raise HTTPException(
                status_code=429,
                detail="per-user concurrent job limit reached",
            )

        job_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        try:
            created_at = store.create_job(
                conn,
                job_id=job_id,
                run_id=run_id,
                input_payload=payload_dict,
                owner_sub=owner_sub,
                client_request_id=client_request_id,
            )
            conn.commit()
            return job_id, run_id, created_at, False
        except UniqueViolation as exc:
            conn.rollback()
            if not client_request_id:
                raise exc
    with store.connect() as conn2:
        existing = store.find_by_client_request(
            conn2,
            owner_sub,
            client_request_id,
        )
        if existing is None:
            raise HTTPException(
                status_code=409,
                detail="idempotent conflict; retry with same client_request_id",
            )
        conn2.commit()
        return (
            existing.job_id,
            existing.run_id,
            existing.created_at,
            True,
        )


class AnalysisResponse(BaseModel):
    job_id: str
    run_id: str
    status: str = "queued"
    message: str
    started_at: str


class AnalysisStatusResponse(BaseModel):
    run_id: str
    status: str
    completed_at: str | None = None
    error: str | None = None


def _job_payload_dict(req: AnalysisRequest) -> dict[str, Any]:
    return _build_payload(req).model_dump(mode="json")


@app.post("/jobs", response_model=JobCreatedResponse)
async def create_job(
    request: JobCreateRequest,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> JobCreatedResponse:
    owner_sub = _resolve_owner_for_create(request.owner_sub, x_user_subject)
    if request.client_request_id is not None and not request.client_request_id.strip():
        raise HTTPException(status_code=400, detail="client_request_id must be non-empty when set")

    store = await require_db_store()
    payload_dict = _job_payload_dict(request)
    cid = request.client_request_id.strip() if request.client_request_id else None

    def _run() -> tuple[str, str, datetime, bool]:
        return enqueue_job(store, payload_dict, owner_sub, cid)

    job_id, run_id, created_at, reused = await asyncio.to_thread(_run)
    msg = (
        f"existing job for client_request_id (job_id={job_id})"
        if reused
        else f"queued analysis for {request.company_name}"
    )
    return JobCreatedResponse(
        job_id=job_id,
        run_id=run_id,
        status="queued",
        message=msg,
        created_at=created_at.isoformat(),
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(
    job_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> JobStatusResponse:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(
            status_code=400,
            detail="X-User-Subject is required when API key is enabled",
        )

    def _run() -> JobRecord:
        return _get_job_for_owner(store, job_id, owner_sub)

    rec = await asyncio.to_thread(_run)
    return _record_to_status(rec)


@app.get("/jobs/{job_id}/result")
async def get_job_result(
    job_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> Any:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _load() -> dict[str, Any]:
        rec = _get_job_for_owner(store, job_id, owner_sub)
        return _json_result_for_job(rec)

    data = await asyncio.to_thread(_load)
    return JSONResponse(content=data)


@app.get("/jobs/{job_id}/brief")
async def get_job_brief(
    job_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> Any:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _load() -> dict[str, Any]:
        rec = _get_job_for_owner(store, job_id, owner_sub)
        return _json_brief_for_job(rec)

    data = await asyncio.to_thread(_load)
    return JSONResponse(content=data)


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> dict[str, str]:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _run() -> bool:
        _get_job_for_owner(store, job_id, owner_sub)
        with store.connect() as conn:
            ok = store.request_cancel(conn, job_id, owner_sub)
            conn.commit()
            return ok

    ok = await asyncio.to_thread(_run)
    if not ok:
        raise HTTPException(status_code=409, detail="job cannot be cancelled in current state")
    return {"job_id": job_id, "status": "cancel_requested_or_cancelled"}


@app.get("/jobs", response_model=RunListResponse)
async def list_jobs(
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> RunListResponse:
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")
    if (
        _database_url() is not None
        and _api_key_expected() is None
        and not (owner_sub and owner_sub.strip())
    ):
        raise HTTPException(
            status_code=400,
            detail="X-User-Subject required for job listing when database is configured",
        )
    store = await require_db_store()

    def _run() -> tuple[list[dict[str, Any]], int]:
        with store.connect() as conn:
            rows, total = store.list_jobs_for_owner(conn, owner_sub, limit=limit, offset=offset)
            conn.commit()
        out = [
            {
                "job_id": r.job_id,
                "run_id": r.run_id,
                "status": r.status,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ]
        return out, total

    runs, count = await asyncio.to_thread(_run)
    return RunListResponse(runs=runs, count=count)


@app.post("/analyze", response_model=AnalysisResponse)
async def legacy_analyze(
    request: AnalysisRequest,
    _: Annotated[None, Depends(verify_service_auth_or_legacy)],
) -> AnalysisResponse:
    if not _allow_legacy_unauth():
        raise HTTPException(
            status_code=404,
            detail="use POST /jobs when PEOPLEDD_ALLOW_LEGACY_UNAUTH is not enabled",
        )
    store = await require_db_store()
    payload_dict = _job_payload_dict(request)

    def _run() -> tuple[str, str, datetime, bool]:
        return enqueue_job(store, payload_dict, owner_sub=None, client_request_id=None)

    job_id, run_id, created_at, _ = await asyncio.to_thread(_run)
    return AnalysisResponse(
        job_id=job_id,
        run_id=run_id,
        status="queued",
        message=f"legacy queued for {request.company_name}",
        started_at=created_at.isoformat(),
    )


@app.get("/runs/{run_id}/status", response_model=AnalysisStatusResponse)
async def get_run_status(
    run_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> AnalysisStatusResponse:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _lookup() -> JobRecord:
        return _get_run_for_owner(store, run_id, owner_sub)

    rec = await asyncio.to_thread(_lookup)

    if rec.status in ("succeeded", "failed", "cancelled"):
        err = rec.error_message if rec.status == "failed" else None
        return AnalysisStatusResponse(
            run_id=run_id,
            status=rec.status,
            completed_at=rec.finished_at.isoformat() if rec.finished_at else None,
            error=err,
        )
    return AnalysisStatusResponse(
        run_id=run_id,
        status=rec.status,
        completed_at=None,
        error=None,
    )


@app.get("/runs/{run_id}/result")
async def get_run_result(
    run_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> Any:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _load() -> dict[str, Any]:
        rec = _get_run_for_owner(store, run_id, owner_sub)
        return _json_result_for_job(rec)

    data = await asyncio.to_thread(_load)
    return JSONResponse(content=data)


@app.get("/runs/{run_id}/brief")
async def get_run_brief(
    run_id: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> Any:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _load() -> dict[str, Any]:
        rec = _get_run_for_owner(store, run_id, owner_sub)
        return _json_brief_for_job(rec)

    data = await asyncio.to_thread(_load)
    return JSONResponse(content=data)


@app.get("/runs", response_model=RunListResponse)
async def list_runs_disk(
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> RunListResponse:
    if _api_key_expected():
        store = await require_db_store()
        owner_sub = _resolve_owner_sub(None, x_user_subject)
        if not (owner_sub and owner_sub.strip()):
            raise HTTPException(status_code=400, detail="X-User-Subject is required")

        def _run() -> tuple[list[dict[str, Any]], int]:
            with store.connect() as conn:
                rows, total = store.list_jobs_for_owner(
                    conn, owner_sub, limit=limit, offset=offset
                )
                conn.commit()
            out = [
                {
                    "job_id": r.job_id,
                    "run_id": r.run_id,
                    "status": r.status,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
            return out, total

        runs, count = await asyncio.to_thread(_run)
        return RunListResponse(runs=runs, count=count)
    try:
        runs, _ = (
            zip(*list_runs(Path(_output_dir)))
            if list(list_runs(Path(_output_dir)))
            else ([], [])
        )
        runs_list = list(runs)
        paginated = runs_list[offset : offset + limit]
        return RunListResponse(
            runs=[{"run_id": r} for r in paginated],
            count=len(runs_list),
        )
    except Exception:
        logger.exception("Failed to list runs")
        raise HTTPException(status_code=500, detail="list failed")


@app.get("/runs/{run_a}/diff/{run_b}")
async def diff_two_runs(
    run_a: str,
    run_b: str,
    _: Annotated[None, Depends(verify_service_auth)],
    x_user_subject: Annotated[str | None, Header(alias="X-User-Subject")] = None,
) -> DiffResponse:
    store = await require_db_store()
    owner_sub = _resolve_owner_sub(None, x_user_subject)
    if _api_key_expected() and not (owner_sub and owner_sub.strip()):
        raise HTTPException(status_code=400, detail="X-User-Subject is required")

    def _diff() -> dict[str, Any]:
        _get_run_for_owner(store, run_a, owner_sub)
        _get_run_for_owner(store, run_b, owner_sub)
        return diff_runs(Path(_output_dir), run_a, run_b)

    try:
        comparison = await asyncio.to_thread(_diff)
        return DiffResponse(comparison=comparison)
    except HTTPException:
        raise
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="run artifacts not found")
    except Exception:
        logger.exception("Diff error")
        raise HTTPException(status_code=500, detail="diff failed")


def main() -> None:
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    uvicorn.run(
        "peopledd.api:app",
        host=host,
        port=port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
