"""REST API wrapper for peopledd pipeline.

Exposes HTTP endpoints for external tools to trigger analyses,
list runs, and retrieve results.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from peopledd.models.contracts import FinalReport, InputPayload
from peopledd.orchestrator import run_pipeline
from peopledd.runtime.run_inspect import diff_runs, list_runs, read_run_summary

logger = logging.getLogger(__name__)


class AnalysisRequest(BaseModel):
    """Request to start a new analysis."""

    company_name: str
    country: str = Field(default="BR", description="ISO 3166-1 alpha-2 country code")
    company_type_hint: str = Field(
        default="auto",
        description="'auto', 'listed', or 'private'",
    )
    ticker_hint: str | None = Field(default=None)
    cnpj_hint: str | None = Field(default=None)
    analysis_depth: str = Field(default="standard", description="'standard' or 'deep'")
    output_mode: str = Field(default="both", description="'json', 'report', or 'both'")
    use_harvest: bool = Field(default=True)
    prefer_llm: bool = Field(default=True)
    use_apify: bool = Field(default=True)
    use_browserless: bool = Field(default=True)
    allow_manual_resolution: bool = Field(default=False)


class AnalysisResponse(BaseModel):
    """Response after triggering an analysis."""

    run_id: str
    status: str = "queued"
    message: str
    started_at: str


class AnalysisStatusResponse(BaseModel):
    """Response with analysis status."""

    run_id: str
    status: str
    completed_at: str | None = None
    error: str | None = None


class RunListResponse(BaseModel):
    """Response listing runs."""

    runs: list[dict[str, Any]]
    count: int


class DiffResponse(BaseModel):
    """Response comparing two runs."""

    comparison: dict[str, Any]


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str = "0.1.0"
    timestamp: str


# Global state
_output_dir: str = os.getenv("PEOPLEDD_OUTPUT_DIR", "/tmp/peopledd_runs")
_active_runs: dict[str, asyncio.Task[Any]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context for startup/shutdown."""
    Path(_output_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {_output_dir}")
    yield
    for task in _active_runs.values():
        if not task.done():
            task.cancel()


app = FastAPI(
    title="peopledd API",
    description="REST API for company governance X-ray pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS for external tools
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _build_payload(req: AnalysisRequest) -> InputPayload:
    """Build InputPayload from request."""
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


def _run_analysis_sync(payload: InputPayload, run_id: str) -> tuple[FinalReport, str]:
    """Run analysis synchronously in a thread."""
    try:
        report = run_pipeline(payload, output_dir=_output_dir)
        return report, ""
    except Exception as e:
        logger.exception(f"Analysis failed for run_id {run_id}")
        return None, str(e)


async def _run_analysis_async(payload: InputPayload, run_id: str) -> None:
    """Run analysis asynchronously (in background)."""
    loop = asyncio.get_event_loop()
    report, error = await loop.run_in_executor(
        None, _run_analysis_sync, payload, run_id
    )
    if error:
        logger.error(f"Run {run_id} failed: {error}")


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
    )


@app.post("/analyze", response_model=AnalysisResponse)
async def start_analysis(request: AnalysisRequest) -> AnalysisResponse:
    """Start a new analysis.

    Returns a run_id that can be used to poll status or fetch results.
    Analysis runs asynchronously in the background.
    """
    run_id = str(uuid.uuid4())
    payload = _build_payload(request)
    payload.run_id = run_id

    try:
        task = asyncio.create_task(_run_analysis_async(payload, run_id))
        _active_runs[run_id] = task
        logger.info(f"Started analysis: {run_id} for {request.company_name}")
    except Exception as e:
        logger.exception(f"Failed to start analysis {run_id}")
        raise HTTPException(status_code=500, detail=str(e))

    return AnalysisResponse(
        run_id=run_id,
        status="queued",
        message=f"Analysis queued for {request.company_name}",
        started_at=datetime.utcnow().isoformat(),
    )


@app.get("/runs/{run_id}/status", response_model=AnalysisStatusResponse)
async def get_run_status(run_id: str) -> AnalysisStatusResponse:
    """Get status of a run."""
    run_path = Path(_output_dir) / run_id
    run_summary_path = run_path / "run_summary.json"

    if not run_summary_path.exists():
        if run_id in _active_runs:
            return AnalysisStatusResponse(
                run_id=run_id,
                status="running",
            )
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    try:
        summary = json.loads(run_summary_path.read_text())
        status = summary.get("status", "unknown")
        error = summary.get("error", None)
        completed_at = summary.get("completed_at", None)
        return AnalysisStatusResponse(
            run_id=run_id,
            status=status,
            error=error,
            completed_at=completed_at,
        )
    except Exception as e:
        logger.exception(f"Failed to read run_summary for {run_id}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/runs/{run_id}/result")
async def get_run_result(run_id: str) -> Any:
    """Get full result of a completed run."""
    run_path = Path(_output_dir) / run_id

    final_report_path = run_path / "final_report.json"
    if final_report_path.exists():
        return JSONResponse(
            content=json.loads(final_report_path.read_text()),
        )

    run_summary_path = run_path / "run_summary.json"
    if run_summary_path.exists():
        return JSONResponse(
            content=json.loads(run_summary_path.read_text()),
        )

    raise HTTPException(
        status_code=404,
        detail=f"No results found for run {run_id}",
    )


@app.get("/runs/{run_id}/brief")
async def get_run_brief(run_id: str) -> Any:
    """Get dd_brief.json (due diligence brief) for a run."""
    run_path = Path(_output_dir) / run_id
    brief_path = run_path / "dd_brief.json"

    if not brief_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Brief not found for run {run_id}",
        )

    return JSONResponse(content=json.loads(brief_path.read_text()))


@app.get("/runs", response_model=RunListResponse)
async def list_all_runs(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> RunListResponse:
    """List recent runs."""
    try:
        runs, _ = zip(*list_runs(Path(_output_dir))) if list(list_runs(Path(_output_dir))) else ([], [])
        runs_list = list(runs)
        paginated = runs_list[offset : offset + limit]
        return RunListResponse(
            runs=[{"run_id": r} for r in paginated],
            count=len(runs_list),
        )
    except Exception as e:
        logger.exception("Failed to list runs")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/runs/{run_a}/diff/{run_b}")
async def diff_two_runs(run_a: str, run_b: str) -> DiffResponse:
    """Compare two runs."""
    try:
        comparison = diff_runs(Path(_output_dir), run_a, run_b)
        return DiffResponse(comparison=comparison)
    except FileNotFoundError as e:
        logger.exception(f"Diff failed for {run_a} vs {run_b}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Diff error for {run_a} vs {run_b}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/openapi.json")
async def get_openapi() -> dict[str, Any]:
    """Get OpenAPI schema (for tools/clients to discover endpoints)."""
    return app.openapi()


def main() -> None:
    """Run the API server."""
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
