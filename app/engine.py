"""Map/reduce engine with fault recovery for intermediate files.

Guarantees implemented here:

* Mapping is derived only from the persisted input shard, so re-running it is
  idempotent.
* Every route is hashed into exactly one of two partitions.
* Both partitions reduce concurrently in separate asyncio tasks; their
  CPU/blocking file work runs in worker threads.
* A truncated, missing or otherwise unreadable intermediate file re-maps
  only the affected shard.
* Concurrent re-map requests for the same shard are coalesced: one dispatch
  per shard per job run, however many partitions request it.
* A shard contributes to a partition total only after its whole file,
  trailer and manifest digest validate.  Failed reads discard their local
  dict, so retries cannot double-count.
* Files are streamed line by line; merge memory is O(distinct routes), not
  O(intermediate file size).
* If a re-map fails, or a re-read after a successful re-map still fails, the
  whole batch fails and no aggregate is returned.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable

from .errors import (
    BatchFailedError,
    MapReduceError,
    PartitionFailure,
    ReduceOutcome,
    RemapError,
    TransientIntermediateError,
)
from .partitioning import PARTITION_COUNT, partition_for_route, partition_label
from .storage import (
    intermediate_path,
    load_input_entries,
    read_intermediate,
    read_manifest,
    write_intermediates,
)

FailureHook = Callable[[str, int, TransientIntermediateError], Awaitable[None]]


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def intermediate_status(
    data_dir: Path, job_id: str, shard_ids: Iterable[str]
) -> list[dict[str, Any]]:
    """Report, per shard and partition, whether the file matches its manifest."""
    rows: list[dict[str, Any]] = []
    for shard_id in shard_ids:
        row: dict[str, Any] = {"shard_id": shard_id}
        try:
            manifest = read_manifest(data_dir, job_id, shard_id)
        except TransientIntermediateError as exc:
            row["manifest_error"] = str(exc)
            rows.append(row)
            continue
        row["generation"] = manifest.get("generation")
        files = []
        for partition in range(PARTITION_COUNT):
            label = partition_label(partition)
            path = intermediate_path(data_dir, job_id, shard_id, partition)
            meta = (manifest.get("files") or {}).get(label, {})
            entry: dict[str, Any] = {
                "partition": label,
                "expected_bytes": meta.get("bytes"),
                "expected_sha256": meta.get("sha256"),
                "path": str(path),
            }
            try:
                entry["actual_bytes"] = path.stat().st_size
            except OSError as exc:
                entry["actual_bytes"] = None
                entry["stat_error"] = str(exc)
            entry["matches_manifest"] = entry.get("actual_bytes") is not None and entry[
                "actual_bytes"
            ] == entry.get("expected_bytes")
            files.append(entry)
        row["files"] = files
        rows.append(row)
    return rows


@dataclass
class MapShardResult:
    shard_id: str
    entries: int
    five_xx: int
    partition_records: dict[str, int]
    generation: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "entries": self.entries,
            "five_xx": self.five_xx,
            "partition_records": dict(self.partition_records),
            "generation": self.generation,
        }


@dataclass
class ShardRead:
    shard_id: str
    partition: int
    counts: dict[str, int]
    attempts: int
    recovered: bool


@dataclass
class PartitionReport:
    partition: int
    counts: dict[str, int]
    read_attempts: int
    recovered_shards: list[str] = field(default_factory=list)


@dataclass
class ReduceReport:
    counts: dict[str, int]
    partition_reports: list[PartitionReport]
    remap_dispatches: dict[str, int]
    elapsed_seconds: float

    @property
    def total_5xx(self) -> int:
        return sum(self.counts.values())

    @property
    def recovered(self) -> dict[str, list[str]]:
        result: dict[str, set[str]] = defaultdict(set)
        for report in self.partition_reports:
            for shard_id in report.recovered_shards:
                result[shard_id].add(partition_label(report.partition))
        return {shard: sorted(labels) for shard, labels in sorted(result.items())}

    @property
    def read_attempts(self) -> int:
        return sum(report.read_attempts for report in self.partition_reports)

    def as_outcome(self) -> ReduceOutcome:
        return ReduceOutcome(
            counts=dict(self.counts),
            recovered=self.recovered,
            remap_dispatches=dict(self.remap_dispatches),
        )


# ---------------------------------------------------------------------------
# Synchronous map work (runs in a worker thread)
# ---------------------------------------------------------------------------


def count_five_xx(entries: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Count 5xx responses per route from validated input entries."""
    counts: dict[str, int] = defaultdict(int)
    for entry in entries:
        route = entry.get("route")
        status = entry.get("status")
        if not isinstance(route, str) or not isinstance(status, int):
            raise RemapError("persisted input contains an invalid entry")
        if 500 <= status <= 599:
            counts[route] += 1
    return dict(counts)


def map_shard_sync(data_dir: Path, job_id: str, shard_id: str) -> MapShardResult:
    """Map one persisted shard into its two intermediate partition files."""
    entries = load_input_entries(data_dir, job_id, shard_id)
    counts = count_five_xx(entries)

    by_partition: dict[int, dict[str, int]] = {
        partition: {} for partition in range(PARTITION_COUNT)
    }
    for route, count in counts.items():
        by_partition[partition_for_route(route)][route] = count

    manifest = write_intermediates(data_dir, job_id, shard_id, by_partition)
    partition_records = {
        label: int(meta["records"]) for label, meta in manifest["files"].items()
    }
    return MapShardResult(
        shard_id=shard_id,
        entries=len(entries),
        five_xx=sum(counts.values()),
        partition_records=partition_records,
        generation=int(manifest["generation"]),
    )


# ---------------------------------------------------------------------------
# Single-flight re-map coordinator
# ---------------------------------------------------------------------------


class RemapCoordinator:
    """Run at most one re-map task per shard for one job run.

    Calls that arrive while a task is in flight join it.  Calls that arrive
    after it has finished receive the cached result, so a wave of failures
    caused by one corrupt generation cannot fan out into repeated maps.
    """

    def __init__(
        self,
        data_dir: Path,
        job_id: str,
        shard_ids: Iterable[str],
        *,
        attempt_delay: float = 0.0,
    ) -> None:
        self._data_dir = data_dir
        self._job_id = job_id
        self._known = frozenset(shard_ids)
        self._tasks: dict[str, asyncio.Task[MapShardResult]] = {}
        self._dispatches: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        self._attempt_delay = attempt_delay

    @property
    def dispatch_count(self) -> int:
        return sum(self._dispatches.values())

    def dispatch_counts(self) -> dict[str, int]:
        return {shard: count for shard, count in self._dispatches.items() if count}

    async def _map_once(self, shard_id: str) -> MapShardResult:
        if self._attempt_delay:
            await asyncio.sleep(self._attempt_delay)
        return await asyncio.to_thread(
            map_shard_sync, self._data_dir, self._job_id, shard_id
        )

    async def ensure_remapped(self, shard_id: str) -> MapShardResult:
        if shard_id not in self._known:
            raise RemapError(f"unknown shard {shard_id!r}")
        async with self._lock:
            task = self._tasks.get(shard_id)
            if task is None:
                self._dispatches[shard_id] += 1
                task = asyncio.create_task(self._map_once(shard_id))
                self._tasks[shard_id] = task
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - surfaced as a job failure
            raise RemapError(f"re-map of shard {shard_id!r} failed: {exc}") from exc
        if result.shard_id != shard_id:
            raise RemapError("re-map returned a result for the wrong shard")
        return result


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class MapReduceEngine:
    """Two-partition mapper/reducer for one job run."""

    def __init__(
        self,
        data_dir: Path,
        job_id: str,
        shard_ids: Iterable[str],
        *,
        remap_attempts: int = 1,
        generation_retries: int = 4,
        failure_hook: FailureHook | None = None,
        remap_delay: float = 0.0,
    ) -> None:
        if remap_attempts < 1:
            raise ValueError("remap_attempts must be >= 1")
        if generation_retries < 1:
            raise ValueError("generation_retries must be >= 1")
        self.data_dir = data_dir
        self.job_id = job_id
        self.shard_ids = list(shard_ids)
        # A shard may be re-mapped once per read attempt; the coordinator
        # still guarantees a single physical dispatch per shard.
        self._remap_attempts = remap_attempts
        self._generation_retries = generation_retries
        self._failure_hook = failure_hook
        self.remaps = RemapCoordinator(
            data_dir, job_id, self.shard_ids, attempt_delay=remap_delay
        )
        self.map_results: list[MapShardResult] = []

    # -- mapping -----------------------------------------------------------

    async def map_all(self) -> list[MapShardResult]:
        """Map every shard concurrently.

        A failure in any shard fails the whole job: there is no safe way to
        reduce a batch whose input could not be fully mapped.
        """
        shards = list(self.shard_ids)
        results = await asyncio.gather(
            *(
                asyncio.to_thread(map_shard_sync, self.data_dir, self.job_id, shard_id)
                for shard_id in shards
            ),
            return_exceptions=True,
        )
        failures: list[PartitionFailure] = []
        mapped: list[MapShardResult] = []
        for shard_id, result in zip(shards, results):
            if isinstance(result, BaseException):
                reason = (
                    str(result)
                    if isinstance(result, MapReduceError)
                    else f"{type(result).__name__}: {result}"
                )
                # A map failure is not partition-specific; report it under
                # partition p0 with the offending shard named.
                failures.append(
                    PartitionFailure(
                        partition=0,
                        stage="map",
                        shard_id=shard_id,
                        reason=reason,
                    )
                )
            else:
                mapped.append(result)
        if failures:
            raise BatchFailedError(
                "map phase failed; no aggregate is available", failures
            )
        self.map_results = mapped
        return mapped

    # -- reading -----------------------------------------------------------

    async def _read_shard(self, shard_id: str, partition: int) -> ShardRead:
        """Read one shard for one partition, re-mapping it if necessary.

        The local count dict is only returned after a full file plus trailer
        and manifest validation, and after confirming the generation did not
        change during the read.  Any failure starts the next attempt from an
        empty dict, so partially-read counts are never merged.
        """
        generation: int | None = None
        generation_retries = 0
        remaps_used = 0
        recovered = False
        attempts = 0

        while True:
            attempts += 1
            try:
                manifest = await asyncio.to_thread(
                    read_manifest, self.data_dir, self.job_id, shard_id
                )
                generation = int(manifest["generation"])
                counts = await asyncio.to_thread(
                    read_intermediate,
                    self.data_dir,
                    self.job_id,
                    shard_id,
                    partition,
                    manifest,
                )
                latest = await self._generation_or_none(shard_id)
                if latest != generation:
                    generation_retries += 1
                    if generation_retries > self._generation_retries:
                        raise BatchFailedError(
                            f"shard {shard_id!r} kept changing while partition "
                            f"p{partition} was reading it",
                            shard_id=shard_id,
                        )
                    continue
                return ShardRead(
                    shard_id=shard_id,
                    partition=partition,
                    counts=counts,
                    attempts=attempts,
                    recovered=recovered or remaps_used > 0,
                )
            except TransientIntermediateError as exc:
                if self._failure_hook is not None:
                    await self._failure_hook(shard_id, partition, exc)

                latest = await self._generation_or_none(shard_id)
                advanced = latest is not None and (
                    generation is None or latest > generation
                )
                if advanced:
                    # Another partition (or a previous attempt) already
                    # published a newer generation: just read it again.
                    # This read also benefited from that re-map, so record
                    # the recovery against this partition as well.
                    recovered = True
                    generation_retries += 1
                    if generation_retries > self._generation_retries:
                        raise BatchFailedError(
                            f"shard {shard_id!r} kept changing while partition "
                            f"p{partition} was reading it",
                            shard_id=shard_id,
                        ) from exc
                    generation = latest
                    continue

                if remaps_used >= self._remap_attempts:
                    raise BatchFailedError(
                        f"shard {shard_id!r} is still unreadable for partition "
                        f"p{partition} after re-map: {exc}",
                        shard_id=shard_id,
                    ) from exc

                try:
                    result = await self.remaps.ensure_remapped(shard_id)
                except RemapError as remap_exc:
                    raise BatchFailedError(
                        f"cannot recover shard {shard_id!r}: {remap_exc}",
                        shard_id=shard_id,
                    ) from remap_exc
                remaps_used += 1
                generation = result.generation
                # Loop and re-read the freshly published generation.

    async def _generation_or_none(self, shard_id: str) -> int | None:
        try:
            manifest = await asyncio.to_thread(
                read_manifest, self.data_dir, self.job_id, shard_id
            )
            return int(manifest["generation"])
        except (TransientIntermediateError, KeyError, TypeError, ValueError):
            return None

    # -- reduce ------------------------------------------------------------

    async def _reduce_partition(self, partition: int) -> PartitionReport:
        totals: dict[str, int] = defaultdict(int)
        attempts = 0
        recovered: list[str] = []
        for shard_id in self.shard_ids:
            read = await self._read_shard(shard_id, partition)
            attempts += read.attempts
            if read.recovered:
                recovered.append(shard_id)
            for route, count in read.counts.items():
                if partition_for_route(route) != partition:
                    raise BatchFailedError(
                        f"route {route!r} was read in the wrong reduce partition"
                    )
                totals[route] += count
        return PartitionReport(
            partition=partition,
            counts=dict(totals),
            read_attempts=attempts,
            recovered_shards=recovered,
        )

    async def reduce(self) -> ReduceReport:
        """Reduce both partitions concurrently.

        Any failure in either partition fails the whole batch.  Results of
        the healthy partition are deliberately discarded.
        """
        started = time.perf_counter()
        results = await asyncio.gather(
            *(
                self._reduce_partition(partition)
                for partition in range(PARTITION_COUNT)
            ),
            return_exceptions=True,
        )

        failures: list[PartitionFailure] = []
        for partition, result in enumerate(results):
            if isinstance(result, BaseException):
                shard_id = getattr(result, "shard_id", None)
                failures.append(
                    PartitionFailure(
                        partition=partition,
                        stage="reduce",
                        shard_id=shard_id,
                        reason=(
                            str(result)
                            if isinstance(result, MapReduceError)
                            else f"{type(result).__name__}: {result}"
                        ),
                        attempts=(
                            self.remaps.dispatch_counts().get(shard_id, 0)
                            if shard_id
                            else 0
                        ),
                    )
                )
        if failures:
            raise BatchFailedError("batch failed; no aggregate is available", failures)

        counts: dict[str, int] = defaultdict(int)
        reports: list[PartitionReport] = []
        for partition, result in enumerate(results):
            assert isinstance(result, PartitionReport)
            reports.append(result)
            for route, count in result.counts.items():
                expected = partition_for_route(route)
                if expected != partition:
                    raise BatchFailedError(
                        f"route {route!r} was merged by the wrong partition"
                    )
                counts[route] += count

        return ReduceReport(
            counts=dict(counts),
            partition_reports=reports,
            remap_dispatches=self.remaps.dispatch_counts(),
            elapsed_seconds=round(time.perf_counter() - started, 6),
        )
