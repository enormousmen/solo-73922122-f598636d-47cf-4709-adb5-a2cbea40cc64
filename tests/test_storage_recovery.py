"""Unit tests for intermediate-file truncation detection and recovery."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.engine import MapReduceEngine, map_shard_sync
from app.errors import BatchFailedError, TransientIntermediateError
from app.partitioning import partition_for_route
from app.storage import (
    delete_input,
    intermediate_path,
    read_intermediate,
    read_manifest,
    save_input,
    truncate_intermediate_tail,
)

JOB = "job-unit"


def _write_shard(data_dir: Path, shard_id: str, entries: list[tuple[str, int]]) -> None:
    save_input(
        data_dir,
        JOB,
        shard_id,
        [{"route": route, "status": status} for route, status in entries],
    )


def test_map_writes_both_partition_files(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500), ("/b", 503), ("/c", 200)])
    result = map_shard_sync(tmp_path, JOB, "s1")
    manifest = read_manifest(tmp_path, JOB, "s1")
    assert result.five_xx == 2
    assert manifest["generation"] == 1
    for partition in (0, 1):
        path = intermediate_path(tmp_path, JOB, "s1", partition)
        assert path.exists()
        assert read_intermediate(tmp_path, JOB, "s1", partition) == {
            route: 1
            for route in ("/a", "/b")
            if partition_for_route(route) == partition
        }


def test_tail_truncation_at_record_boundary_is_detected(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500), ("/b", 503)])
    map_shard_sync(tmp_path, JOB, "s1")
    partition = partition_for_route("/a")
    truncate_intermediate_tail(tmp_path, JOB, "s1", partition, drop_records=1)
    with pytest.raises(TransientIntermediateError):
        read_intermediate(tmp_path, JOB, "s1", partition)


def test_truncation_mid_record_is_detected(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500), ("/b", 503)])
    map_shard_sync(tmp_path, JOB, "s1")
    partition = partition_for_route("/a")
    path = intermediate_path(tmp_path, JOB, "s1", partition)
    raw = path.read_bytes()
    path.write_bytes(raw[: len(raw) // 2])
    with pytest.raises(TransientIntermediateError):
        read_intermediate(tmp_path, JOB, "s1", partition)


def test_empty_intermediate_file_is_detected(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500)])
    map_shard_sync(tmp_path, JOB, "s1")
    partition = partition_for_route("/a")
    intermediate_path(tmp_path, JOB, "s1", partition).write_bytes(b"")
    with pytest.raises(TransientIntermediateError):
        read_intermediate(tmp_path, JOB, "s1", partition)


def test_body_digest_mismatch_is_detected(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500), ("/b", 503)])
    map_shard_sync(tmp_path, JOB, "s1")
    partition = partition_for_route("/a")
    path = intermediate_path(tmp_path, JOB, "s1", partition)
    lines = path.read_bytes().splitlines(keepends=True)
    lines[0] = lines[0].replace(b"1", b"7", 1)
    path.write_bytes(b"".join(lines))
    with pytest.raises(TransientIntermediateError):
        read_intermediate(tmp_path, JOB, "s1", partition)


def test_trailer_missing_after_clean_record_line(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500)])
    map_shard_sync(tmp_path, JOB, "s1")
    partition = partition_for_route("/a")
    path = intermediate_path(tmp_path, JOB, "s1", partition)
    lines = path.read_bytes().splitlines(keepends=True)
    path.write_bytes(b"".join(lines[:-1]))  # body intact, trailer gone
    with pytest.raises(TransientIntermediateError):
        read_intermediate(tmp_path, JOB, "s1", partition)


def _run_reduce(tmp_path: Path, shard_ids: list[str]) -> tuple[dict[str, int], dict]:
    engine = MapReduceEngine(tmp_path, JOB, shard_ids, remap_attempts=1)
    asyncio.run(engine.map_all())
    report = asyncio.run(engine.reduce())
    return report.counts, report


def test_recovery_after_truncated_tail_returns_correct_totals(tmp_path: Path) -> None:
    entries = [("/api/orders", 500), ("/api/orders", 502), ("/login", 503)]
    _write_shard(tmp_path, "s1", entries)
    _write_shard(tmp_path, "s2", [("/api/orders", 500), ("/health", 200)])

    engine = MapReduceEngine(tmp_path, JOB, ["s1", "s2"], remap_attempts=1)
    asyncio.run(engine.map_all())
    partition = partition_for_route("/api/orders")
    truncate_intermediate_tail(tmp_path, JOB, "s1", partition, drop_records=1)

    report = asyncio.run(engine.reduce())
    assert report.counts == {"/api/orders": 3, "/login": 1}
    assert report.remap_dispatches == {"s1": 1}
    assert report.recovered == {"s1": [f"p{partition}"]}


def test_persistent_corruption_fails_the_batch(tmp_path: Path) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500)])
    engine = MapReduceEngine(tmp_path, JOB, ["s1"], remap_attempts=1)
    asyncio.run(engine.map_all())
    partition = partition_for_route("/a")
    delete_input(tmp_path, JOB, "s1")  # the re-map cannot succeed
    truncate_intermediate_tail(tmp_path, JOB, "s1", partition, drop_records=1)
    with pytest.raises(BatchFailedError) as excinfo:
        asyncio.run(engine.reduce())
    assert excinfo.value.failures
    assert "no aggregate" in str(excinfo.value)


def test_remap_read_failure_after_remap_fails_the_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_shard(tmp_path, "s1", [("/a", 500)])
    engine = MapReduceEngine(tmp_path, JOB, ["s1"], remap_attempts=1)
    asyncio.run(engine.map_all())
    partition = partition_for_route("/a")
    truncate_intermediate_tail(tmp_path, JOB, "s1", partition, drop_records=1)

    original = map_shard_sync

    def half_written_map(*args, **kwargs):
        """Re-map that leaves a body line without its trailer."""
        result = original(*args, **kwargs)
        path = intermediate_path(tmp_path, JOB, "s1", partition)
        lines = path.read_bytes().splitlines(keepends=True)
        path.write_bytes(b"".join(lines[:-1]))
        return result

    import app.engine as engine_module

    monkeypatch.setattr(engine_module, "map_shard_sync", half_written_map)
    with pytest.raises(BatchFailedError):
        asyncio.run(engine.reduce())


def test_other_partition_merges_normally_during_recovery(tmp_path: Path) -> None:
    # One route per partition, so the healthy partition has real work to do.
    damaged_route = "/api/orders"  # partition 0
    healthy_route = "/login"  # partition 1
    assert partition_for_route(damaged_route) != partition_for_route(healthy_route)
    _write_shard(tmp_path, "s1", [(damaged_route, 500), (healthy_route, 503)])
    engine = MapReduceEngine(tmp_path, JOB, ["s1"], remap_attempts=1)
    asyncio.run(engine.map_all())
    partition = partition_for_route(damaged_route)
    other = partition_for_route(healthy_route)
    truncate_intermediate_tail(tmp_path, JOB, "s1", partition, drop_records=1)

    report = asyncio.run(engine.reduce())
    assert report.counts[damaged_route] == 1
    assert report.counts[healthy_route] == 1
    # The healthy partition merged normally: its first read either completed
    # before the re-map published a new generation, or it noticed the new
    # generation and re-read.  Either way it must not have failed or double
    # counted.
    other_report = next(r for r in report.partition_reports if r.partition == other)
    assert other_report.read_attempts >= 1
    assert other_report.counts == {healthy_route: 1}
    assert sum(report.counts.values()) == 2
