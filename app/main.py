"""FastAPI application exposing the local MapReduce service."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import Settings
from .engine import intermediate_status
from .errors import BatchFailedError
from .models import (
    BatchResult,
    BatchUpload,
    HealthResponse,
    JobStatus,
    RouteCount,
    SimulationRequest,
)
from .service import JobNotFound, JobService, ShardNotFound
from .storage import apply_simulation

logger = logging.getLogger("mapreduce")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    yield


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the app.  ``settings`` is injectable for tests."""
    settings = settings or Settings.from_env()
    service = JobService(settings)

    app = FastAPI(
        title="Local MapReduce 5xx-by-route API",
        version="1.0.0",
        lifespan=_lifespan,
    )
    app.state.settings = settings
    app.state.service = service

    def get_service(request: Request) -> JobService:
        return request.app.state.service

    def to_result(job, report) -> BatchResult:
        return BatchResult(
            job_id=job.job_id,
            shards_mapped=len(job.shard_ids),
            partitions_merged=2,
            total_5xx=sum(report.counts.values()),
            routes=[
                RouteCount(route=route, count=report.counts[route])
                for route in sorted(report.counts)
            ],
            recovered=report.recovered,
            remap_dispatches=report.remap_dispatches,
            read_attempts=report.read_attempts,
        )

    @app.exception_handler(BatchFailedError)
    async def batch_failed_handler(
        request: Request, exc: BatchFailedError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "detail": str(exc),
                "job_id": getattr(exc, "job_id", None),
                "failures": [
                    failure.as_dict() if hasattr(failure, "as_dict") else failure
                    for failure in exc.failures
                ],
                "totals_returned": False,
            },
        )

    @app.exception_handler(JobNotFound)
    async def job_not_found_handler(request: Request, exc: JobNotFound) -> JSONResponse:
        return JSONResponse(
            status_code=404, content={"detail": f"job not found: {exc}"}
        )

    @app.exception_handler(ShardNotFound)
    async def shard_not_found_handler(
        request: Request, exc: ShardNotFound
    ) -> JSONResponse:
        return JSONResponse(
            status_code=404, content={"detail": f"shard not found: {exc}"}
        )

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(status="ok", data_dir=str(settings.data_dir))

    @app.post("/jobs", response_model=BatchResult, status_code=201)
    async def submit_batch(
        upload: BatchUpload, svc: JobService = Depends(get_service)
    ) -> BatchResult:
        job, report = await svc.create_and_run(upload)
        return to_result(job, report)

    @app.get("/jobs/{job_id}", response_model=JobStatus)
    async def job_status(
        job_id: str, svc: JobService = Depends(get_service)
    ) -> JobStatus:
        job = await svc.get_job(job_id)
        return JobStatus(
            job_id=job.job_id,
            status=job.status,  # type: ignore[arg-type]
            stages=job.stages,
            remap_dispatches=job.remap_dispatches,
            error=job.error,
        )

    @app.post("/jobs/{job_id}/reduce", response_model=BatchResult)
    async def rerun_reduce(
        job_id: str, svc: JobService = Depends(get_service)
    ) -> BatchResult:
        await svc.get_job(job_id)
        job, report = await svc.rerun_reduce(job_id)
        return to_result(job, report)

    # -- fault-injection endpoints, disabled unless explicitly enabled -----

    def require_debug() -> None:
        if not settings.debug_endpoints:
            raise HTTPException(status_code=404, detail="Not Found")

    @app.post("/jobs/{job_id}/simulate-fault")
    async def simulate_fault(
        job_id: str,
        payload: SimulationRequest,
        svc: JobService = Depends(get_service),
    ) -> dict[str, Any]:
        require_debug()
        await svc.require_shard(job_id, payload.shard_id)
        try:
            applied = await asyncio.to_thread(
                apply_simulation,
                settings.data_dir,
                job_id,
                payload.shard_id,
                payload.partitions,
                payload.drop_records,
                payload.mode,
                payload.delete_input,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"job_id": job_id, **applied}

    @app.get("/jobs/{job_id}/intermediates")
    async def intermediates(
        job_id: str, svc: JobService = Depends(get_service)
    ) -> dict[str, Any]:
        require_debug()
        job = await svc.get_job(job_id)
        return {
            "job_id": job_id,
            "shards": await asyncio.to_thread(
                intermediate_status, settings.data_dir, job_id, job.shard_ids
            ),
        }

    return app


app = create_app()
