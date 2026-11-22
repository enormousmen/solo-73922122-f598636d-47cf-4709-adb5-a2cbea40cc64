"""Job orchestration: persistence, status tracking and locking."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import Settings
from .engine import MapReduceEngine, ReduceReport
from .errors import BatchFailedError
from .models import BatchUpload
from .storage import (
    atomic_write_json,
    job_dir,
    save_input,
)


class JobNotFound(LookupError):
    pass


class ShardNotFound(LookupError):
    pass


@dataclass
class JobRecord:
    job_id: str
    shard_ids: list[str]
    created_at: float
    status: str = "running"
    stages: dict[str, str] = field(
        default_factory=lambda: {"map": "pending", "reduce": "pending"}
    )
    result_counts: dict[str, int] | None = None
    error: str | None = None
    failures: list[dict[str, Any]] = field(default_factory=list)
    remap_dispatches: dict[str, int] = field(default_factory=dict)
    recovered: dict[str, list[str]] = field(default_factory=dict)
    read_attempts: int = 0
    updated_at: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "shard_ids": list(self.shard_ids),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "status": self.status,
            "stages": dict(self.stages),
            "result_counts": self.result_counts,
            "error": self.error,
            "failures": list(self.failures),
            "remap_dispatches": dict(self.remap_dispatches),
            "recovered": {k: list(v) for k, v in self.recovered.items()},
            "read_attempts": self.read_attempts,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JobRecord":
        return cls(
            job_id=payload["job_id"],
            shard_ids=list(payload["shard_ids"]),
            created_at=payload.get("created_at", 0.0),
            status=payload.get("status", "running"),
            stages=dict(
                payload.get("stages") or {"map": "pending", "reduce": "pending"}
            ),
            result_counts=payload.get("result_counts"),
            error=payload.get("error"),
            failures=list(payload.get("failures") or []),
            remap_dispatches=dict(payload.get("remap_dispatches") or {}),
            recovered={k: list(v) for k, v in (payload.get("recovered") or {}).items()},
            read_attempts=int(payload.get("read_attempts") or 0),
            updated_at=payload.get("updated_at", 0.0),
        )


class JobService:
    """Owns the in-memory job cache and the per-job serialization lock."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._jobs: dict[str, JobRecord] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._guard = asyncio.Lock()
        settings.data_dir.mkdir(parents=True, exist_ok=True)

    # -- plumbing ----------------------------------------------------------

    def _job_path(self, job_id: str) -> Path:
        return job_dir(self.settings.data_dir, job_id) / "job.json"

    async def _lock_for(self, job_id: str) -> asyncio.Lock:
        async with self._guard:
            return self._locks.setdefault(job_id, asyncio.Lock())

    def _persist(self, record: JobRecord) -> None:
        atomic_write_json(self._job_path(record.job_id), record.as_dict())

    async def _save(self, record: JobRecord) -> None:
        record.updated_at = time.time()
        self._jobs[record.job_id] = record
        await asyncio.to_thread(self._persist, record)

    def _load(self, job_id: str) -> JobRecord:
        cached = self._jobs.get(job_id)
        if cached is not None:
            return cached
        path = self._job_path(job_id)
        try:
            raw = path.read_bytes()
        except FileNotFoundError as exc:
            raise JobNotFound(job_id) from exc
        except OSError as exc:
            raise JobNotFound(f"{job_id}: {exc}") from exc
        try:
            record = JobRecord.from_dict(json.loads(raw))
        except (ValueError, KeyError, TypeError) as exc:
            raise JobNotFound(f"{job_id}: corrupt job metadata: {exc}") from exc
        self._jobs[job_id] = record
        return record

    async def get_job(self, job_id: str) -> JobRecord:
        return await asyncio.to_thread(self._load, job_id)

    # -- job lifecycle -----------------------------------------------------

    async def create_and_run(
        self, upload: BatchUpload
    ) -> tuple[JobRecord, ReduceReport]:
        job_id = uuid.uuid4().hex
        shard_ids = [shard.shard_id for shard in upload.shards]
        record = JobRecord(
            job_id=job_id,
            shard_ids=shard_ids,
            created_at=time.time(),
        )
        lock = await self._lock_for(job_id)
        async with lock:
            await self._persist_inputs(job_id, upload)
            await self._save(record)
            report = await self._run(record, do_map=True)
        return record, report

    async def rerun_reduce(self, job_id: str) -> tuple[JobRecord, ReduceReport]:
        await self.get_job(job_id)
        lock = await self._lock_for(job_id)
        async with lock:
            record = await self.get_job(job_id)
            record.stages = dict(record.stages)
            record.stages["reduce"] = "pending"
            record.status = "running"
            record.error = None
            record.failures = []
            await self._save(record)
            report = await self._run(record, do_map=False)
        return record, report

    async def _persist_inputs(self, job_id: str, upload: BatchUpload) -> None:
        for shard in upload.shards:
            payload = [
                {"route": entry.route, "status": entry.status}
                for entry in shard.entries
            ]
            await asyncio.to_thread(
                save_input, self.settings.data_dir, job_id, shard.shard_id, payload
            )

    async def _run(self, record: JobRecord, *, do_map: bool) -> ReduceReport:
        engine = MapReduceEngine(
            self.settings.data_dir,
            record.job_id,
            record.shard_ids,
            remap_attempts=self.settings.remap_attempts,
            remap_delay=self.settings.remap_delay,
        )
        try:
            if do_map:
                record.stages["map"] = "running"
                await self._save(record)
                await engine.map_all()
                record.stages["map"] = "completed"
                await self._save(record)

            record.stages["reduce"] = "running"
            await self._save(record)
            report = await engine.reduce()
            record.stages["reduce"] = "completed"
            record.status = "completed"
            record.result_counts = dict(report.counts)
            record.remap_dispatches = dict(report.remap_dispatches)
            record.recovered = report.recovered
            record.read_attempts = report.read_attempts
            record.error = None
            record.failures = []
            await self._save(record)
            return report
        except BatchFailedError as exc:
            exc.job_id = record.job_id
            record.status = "failed"
            record.error = str(exc)
            record.failures = [failure.as_dict() for failure in exc.failures]
            record.result_counts = None
            record.remap_dispatches = dict(engine.remaps.dispatch_counts())
            for stage in ("map", "reduce"):
                if record.stages.get(stage) == "running":
                    record.stages[stage] = "failed"
            await self._save(record)
            raise
        except Exception as exc:  # noqa: BLE001 - record any unexpected failure
            record.status = "failed"
            record.error = f"{type(exc).__name__}: {exc}"
            record.result_counts = None
            for stage in ("map", "reduce"):
                if record.stages.get(stage) == "running":
                    record.stages[stage] = "failed"
            await self._save(record)
            raise

    # -- shard-level helpers for the debug API -----------------------------

    async def require_shard(self, job_id: str, shard_id: str) -> JobRecord:
        record = await self.get_job(job_id)
        if shard_id not in record.shard_ids:
            raise ShardNotFound(shard_id)
        return record
