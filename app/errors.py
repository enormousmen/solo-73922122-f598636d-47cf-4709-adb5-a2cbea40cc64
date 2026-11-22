"""Domain errors used by the MapReduce engine and API layer."""

from __future__ import annotations

from dataclasses import dataclass, field


class MapReduceError(RuntimeError):
    """Base class for job failures."""


class TransientIntermediateError(MapReduceError):
    """An intermediate file is missing, truncated, unreadable or malformed.

    The engine treats this as recoverable: the shard is re-mapped from its
    persisted input and the partition is read again.
    """


class RemapError(MapReduceError):
    """Re-running the map task for a shard failed."""


class BatchFailedError(MapReduceError):
    """The whole batch failed; no aggregate result may be returned."""

    def __init__(
        self,
        message: str,
        failures: list[PartitionFailure] | None = None,
        *,
        shard_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.failures = failures or []
        self.shard_id = shard_id
        # Filled in by JobService so the HTTP error can identify the job.
        self.job_id: str | None = None


@dataclass
class PartitionFailure:
    """Diagnostic record for one failed partition task."""

    partition: int
    stage: str
    shard_id: str | None
    reason: str
    attempts: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "partition": f"p{self.partition}",
            "stage": self.stage,
            "shard_id": self.shard_id,
            "reason": self.reason,
            "attempts": self.attempts,
        }


@dataclass
class ReduceOutcome:
    """Successful result of merging both partitions."""

    counts: dict[str, int]
    recovered: dict[str, list[str]] = field(default_factory=dict)
    remap_dispatches: dict[str, int] = field(default_factory=dict)
