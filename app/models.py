"""Pydantic request/response models for the MapReduce API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator


class LogEntry(BaseModel):
    """One access-log record: a route and its HTTP status code."""

    route: str = Field(min_length=1, max_length=2048)
    status: int = Field(ge=100, le=599)

    @field_validator("route")
    @classmethod
    def route_must_not_be_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("route must not be blank")
        return value


class ShardUpload(BaseModel):
    """A log shard: a stable shard id plus its records."""

    shard_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9._:-]+$")
    entries: list[LogEntry] = Field(min_length=1)


class BatchUpload(BaseModel):
    """A whole batch: many log shards mapped in one job."""

    shards: list[ShardUpload] = Field(min_length=1)

    @field_validator("shards")
    @classmethod
    def shard_ids_must_be_unique(cls, value: list[ShardUpload]) -> list[ShardUpload]:
        seen: set[str] = set()
        for shard in value:
            if shard.shard_id in seen:
                raise ValueError(f"duplicate shard_id: {shard.shard_id}")
            seen.add(shard.shard_id)
        return value


class RouteCount(BaseModel):
    route: str
    count: int


class BatchResult(BaseModel):
    job_id: str
    shards_mapped: int
    partitions_merged: int
    total_5xx: int
    routes: list[RouteCount]
    recovered: dict[str, list[str]] = Field(default_factory=dict)
    remap_dispatches: dict[str, int] = Field(default_factory=dict)
    read_attempts: int = 0


class JobStatus(BaseModel):
    job_id: str
    status: Literal["running", "completed", "failed"]
    stages: dict[str, str]
    remap_dispatches: dict[str, int] = Field(default_factory=dict)
    error: str | None = None


class SimulationRequest(BaseModel):
    """Test-only fault injection for one shard's intermediate files."""

    shard_id: str
    mode: Literal["truncate_tail", "delete_file"] = "truncate_tail"
    partitions: list[int] = Field(default_factory=lambda: [0, 1])
    drop_records: int = Field(default=1, ge=1)
    delete_input: bool = False

    @field_validator("partitions")
    @classmethod
    def partitions_must_be_0_or_1(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("partitions must not be empty")
        if any(partition not in (0, 1) for partition in value):
            raise ValueError("partitions may only contain 0 and 1")
        return sorted(set(value))


class HealthResponse(BaseModel):
    status: Literal["ok"]
    data_dir: str
