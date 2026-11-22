"""Both reduce partitions must run concurrently, not one after the other."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import app.engine as engine_module
from app.engine import MapReduceEngine
from app.storage import save_input


def test_partitions_reduce_concurrently(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    job_id = "job-par"
    save_input(data_dir, job_id, "s1", [{"route": "/a", "status": 500}])
    save_input(data_dir, job_id, "s2", [{"route": "/b", "status": 503}])

    engine = MapReduceEngine(data_dir, job_id, ["s1", "s2"])
    asyncio.run(engine.map_all())

    # Both partition tasks call read_intermediate through worker threads.
    # A barrier of 2 only releases if both are inside the reader at once; if
    # partitions ran sequentially it would time out and the test would fail.
    barrier = threading.Barrier(2, timeout=5)
    original = engine_module.read_intermediate

    def rendezvous_read(*args, **kwargs):
        barrier.wait()
        return original(*args, **kwargs)

    monkeypatch.setattr(engine_module, "read_intermediate", rendezvous_read)
    report = asyncio.run(engine.reduce())

    assert report.counts == {"/a": 1, "/b": 1}
    assert len(report.partition_reports) == 2
