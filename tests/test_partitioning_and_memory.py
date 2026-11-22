"""Partition stability and bounded-memory merge tests."""

from __future__ import annotations

import hashlib
import json
import tracemalloc
from pathlib import Path

from app.engine import map_shard_sync
from app.partitioning import partition_for_route, partition_label
from app.storage import (
    INTERMEDIATE_MAGIC,
    INTERMEDIATE_VERSION,
    atomic_write_bytes,
    atomic_write_json,
    intermediate_path,
    manifest_path,
    read_intermediate,
    save_input,
)

JOB = "job-mem"


def test_partition_assignment_is_stable_across_processes() -> None:
    # Frozen expectations: changing the hash would silently move routes
    # between partitions and invalidate existing intermediate files.
    assert partition_for_route("/api/orders") == 0
    assert partition_for_route("/checkout") == 0
    assert partition_for_route("/login") == 1
    assert partition_for_route("/catalog") == 1
    # Every route maps to exactly one of the two partitions.
    assert {partition_for_route(f"/r{i}") for i in range(100)} == {0, 1}


def test_merge_memory_does_not_scale_with_intermediate_file_size(
    tmp_path: Path,
) -> None:
    """Feed the reader a large file for a single route.

    The mapper normally aggregates per route, but the reader must stream any
    valid file line by line.  With one route the route accumulator stays
    tiny, so peak memory reflects only per-line overhead: anonymous RSS-like
    growth would mean the file was buffered.
    """
    shard_id = "big"
    route = "/repeat"
    partition = partition_for_route(route)
    line = (json.dumps({"r": route, "c": 1}) + "\n").encode()
    repeats = 100_000
    body = line * repeats
    trailer = {
        "_trailer": True,
        "magic": INTERMEDIATE_MAGIC,
        "version": INTERMEDIATE_VERSION,
        "shard_id": shard_id,
        "partition": partition,
        "records": repeats,
        "body_bytes": len(body),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "total_5xx": repeats,
    }
    trailer_bytes = (json.dumps(trailer, separators=(",", ":")) + "\n").encode()
    data = body + trailer_bytes
    path = intermediate_path(tmp_path, JOB, shard_id, partition)
    atomic_write_bytes(path, data)
    assert path.stat().st_size > 1_000_000

    other_label = partition_label(1 - partition)
    atomic_write_bytes(intermediate_path(tmp_path, JOB, shard_id, 1 - partition), b"")
    atomic_write_json(
        manifest_path(tmp_path, JOB, shard_id),
        {
            "generation": 1,
            "files": {
                partition_label(partition): {
                    "records": repeats,
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                },
                other_label: {"records": 0, "bytes": 0, "sha256": ""},
            },
        },
    )

    tracemalloc.start()
    counts = read_intermediate(tmp_path, JOB, shard_id, partition)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert counts == {route: repeats}
    # Streaming the file must not require memory proportional to its size.
    assert peak < path.stat().st_size / 10, (peak, path.stat().st_size)
    assert peak < 1_000_000, peak


def test_map_output_is_per_route_not_per_record(tmp_path: Path) -> None:
    save_input(tmp_path, JOB, "s", [{"route": "/repeat", "status": 500}] * 1000)
    map_shard_sync(tmp_path, JOB, "s")
    partition = partition_for_route("/repeat")
    path = intermediate_path(tmp_path, JOB, "s", partition)
    body_lines = [
        json.loads(line)
        for line in path.read_text().splitlines()
        if '"_trailer"' not in line
    ]
    assert body_lines == [{"r": "/repeat", "c": 1000}]
