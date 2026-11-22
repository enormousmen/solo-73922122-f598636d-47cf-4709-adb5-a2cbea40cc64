"""HTTP-level tests: API contract, failure semantics and concurrency."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from app.config import Settings
from app.engine import MapReduceEngine
from app.main import create_app
from app.partitioning import partition_for_route


def _batch(shards: dict[str, list[tuple[str, int]]]) -> dict[str, Any]:
    return {
        "shards": [
            {
                "shard_id": shard_id,
                "entries": [
                    {"route": route, "status": status} for route, status in entries
                ],
            }
            for shard_id, entries in shards.items()
        ]
    }


SHARDS = {
    "shard-a": [("/api/orders", 500), ("/api/orders", 504), ("/login", 503)],
    "shard-b": [("/api/orders", 500), ("/health", 200), ("/login", 500)],
    "shard-c": [("/checkout", 500), ("/health", 500), ("/checkout", 200)],
}


def _expected(shards: dict[str, list[tuple[str, int]]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for entries in shards.values():
        for route, status in entries:
            if 500 <= status <= 599:
                totals[route] = totals.get(route, 0) + 1
    return totals


def _by_route(payload: dict[str, Any]) -> dict[str, int]:
    return {row["route"]: row["count"] for row in payload["routes"]}


# ---------------------------------------------------------------------------


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_batch_returns_per_route_totals(client: TestClient) -> None:
    response = client.post("/jobs", json=_batch(SHARDS))
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["partitions_merged"] == 2
    assert body["shards_mapped"] == 3
    assert _by_route(body) == _expected(SHARDS)
    assert body["total_5xx"] == sum(_expected(SHARDS).values())
    assert body["recovered"] == {}
    assert body["remap_dispatches"] == {}


def test_each_route_appears_once_across_partitions(
    client: TestClient, settings: Settings
) -> None:
    response = client.post("/jobs", json=_batch(SHARDS))
    job_id = response.json()["job_id"]
    seen: dict[str, int] = {}
    for shard_id in SHARDS:
        for partition in (0, 1):
            path = (
                settings.data_dir
                / "jobs"
                / job_id
                / "map"
                / shard_id
                / f"p{partition}.jsonl"
            )
            for line in path.read_text().splitlines():
                record = json.loads(line)
                if record.get("_trailer"):
                    continue
                if record["r"] in seen:
                    assert seen[record["r"]] == partition
                seen[record["r"]] = partition
    assert set(seen) == set(_expected(SHARDS))


def test_invalid_status_is_rejected(client: TestClient) -> None:
    payload = {
        "shards": [{"shard_id": "s", "entries": [{"route": "/x", "status": 99}]}]
    }
    assert client.post("/jobs", json=payload).status_code == 422


def test_duplicate_shard_ids_are_rejected(client: TestClient) -> None:
    payload = _batch({"s": [("/a", 500)]})
    payload["shards"].append(payload["shards"][0])
    assert client.post("/jobs", json=payload).status_code == 422


def test_truncated_tail_recovers_via_partial_remap(
    client: TestClient, settings: Settings
) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    assert created.status_code == 201
    job_id = created.json()["job_id"]

    partition = partition_for_route("/api/orders")
    other = 1 - partition
    sim = client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={
            "shard_id": "shard-a",
            "mode": "truncate_tail",
            "partitions": [partition],
            "drop_records": 1,
        },
    )
    assert sim.status_code == 200, sim.text

    status = client.get(f"/jobs/{job_id}/intermediates").json()
    shard_a = next(row for row in status["shards"] if row["shard_id"] == "shard-a")
    damaged = next(f for f in shard_a["files"] if f["partition"] == f"p{partition}")
    assert damaged["matches_manifest"] is False
    untouched = next(f for f in shard_a["files"] if f["partition"] == f"p{other}")
    assert untouched["matches_manifest"] is True

    rerun = client.post(f"/jobs/{job_id}/reduce")
    assert rerun.status_code == 200, rerun.text
    body = rerun.json()
    assert _by_route(body) == _expected(SHARDS)
    assert body["recovered"] == {"shard-a": [f"p{partition}"]}
    assert body["remap_dispatches"] == {"shard-a": 1}
    assert body["read_attempts"] > 3  # one failed read, then a retry


def test_both_partitions_requesting_same_shard_dispatch_once(
    client: TestClient, settings: Settings
) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    job_id = created.json()["job_id"]

    # Damage *both* partition files of one shard, so both reduce tasks hit a
    # fault for the same shard at roughly the same time.
    sim = client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={
            "shard_id": "shard-b",
            "mode": "truncate_tail",
            "partitions": [0, 1],
            "drop_records": 1,
        },
    )
    assert sim.status_code == 200, sim.text

    rerun = client.post(f"/jobs/{job_id}/reduce")
    assert rerun.status_code == 200, rerun.text
    body = rerun.json()
    assert _by_route(body) == _expected(SHARDS)
    assert body["remap_dispatches"] == {"shard-b": 1}
    assert body["recovered"] == {"shard-b": ["p0", "p1"]}


def test_recovery_does_not_double_count(client: TestClient) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    job_id = created.json()["job_id"]
    partition = partition_for_route("/login")
    client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={
            "shard_id": "shard-a",
            "mode": "truncate_tail",
            "partitions": [partition],
            "drop_records": 1,
        },
    )
    first = client.post(f"/jobs/{job_id}/reduce").json()
    second = client.post(f"/jobs/{job_id}/reduce").json()
    assert _by_route(first) == _expected(SHARDS)
    assert _by_route(second) == _expected(SHARDS)
    assert second["remap_dispatches"] == {}


def test_missing_intermediate_file_is_recovered(
    client: TestClient, settings: Settings
) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    job_id = created.json()["job_id"]
    sim = client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={"shard_id": "shard-c", "mode": "delete_file", "partitions": [0, 1]},
    )
    assert sim.status_code == 200, sim.text
    body = client.post(f"/jobs/{job_id}/reduce").json()
    assert _by_route(body) == _expected(SHARDS)
    assert body["remap_dispatches"] == {"shard-c": 1}


def test_remap_failure_returns_500_without_totals(
    client: TestClient, settings: Settings
) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    job_id = created.json()["job_id"]
    sim = client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={
            "shard_id": "shard-a",
            "mode": "truncate_tail",
            "partitions": [0, 1],
            "drop_records": 1,
            "delete_input": True,
        },
    )
    assert sim.status_code == 200, sim.text

    response = client.post(f"/jobs/{job_id}/reduce")
    assert response.status_code == 500
    body = response.json()
    assert body["totals_returned"] is False
    assert "routes" not in body
    assert body["failures"]

    status = client.get(f"/jobs/{job_id}").json()
    assert status["status"] == "failed"


def test_rerun_after_failed_job_can_succeed_if_fault_cleared(
    client: TestClient,
) -> None:
    created = client.post("/jobs", json=_batch(SHARDS))
    job_id = created.json()["job_id"]
    partition = partition_for_route("/checkout")
    client.post(
        f"/jobs/{job_id}/simulate-fault",
        json={
            "shard_id": "shard-c",
            "mode": "truncate_tail",
            "partitions": [partition],
            "drop_records": 1,
        },
    )
    ok = client.post(f"/jobs/{job_id}/reduce")
    assert ok.status_code == 200
    assert _by_route(ok.json()) == _expected(SHARDS)


def test_unknown_job_returns_404(client: TestClient) -> None:
    assert client.get("/jobs/nope").status_code == 404
    assert client.post("/jobs/nope/reduce").status_code == 404


def test_debug_endpoints_can_be_disabled(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path / "data", debug_endpoints=False)
    with TestClient(create_app(settings)) as blocked:
        created = blocked.post("/jobs", json=_batch(SHARDS))
        job_id = created.json()["job_id"]
        response = blocked.post(
            f"/jobs/{job_id}/simulate-fault",
            json={"shard_id": "shard-a", "partitions": [0], "drop_records": 1},
        )
        assert response.status_code == 404
        assert blocked.get(f"/jobs/{job_id}/intermediates").status_code == 404


# ---------------------------------------------------------------------------
# Concurrency: prove the single-flight guard, not just its side effects
# ---------------------------------------------------------------------------


def test_single_flight_coalesces_concurrent_remaps(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    job_id = "job-race"
    shard_id = "shard-race"
    from app.storage import save_input

    save_input(
        data_dir,
        job_id,
        shard_id,
        [
            {"route": "/r1", "status": 500},
            {"route": "/r2", "status": 503},
            {"route": "/r3", "status": 502},
        ],
    )
    engine = MapReduceEngine(
        data_dir, job_id, [shard_id], remap_attempts=1, remap_delay=0.2
    )
    asyncio.run(engine.map_all())

    calls = 0
    calls_lock = threading.Lock()
    original = __import__("app.engine", fromlist=["map_shard_sync"]).map_shard_sync

    def counting_map(*args, **kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
        return original(*args, **kwargs)

    import app.engine as engine_module

    engine_module.map_shard_sync = counting_map
    try:

        async def race() -> tuple[Any, Any]:
            return await asyncio.gather(
                engine.remaps.ensure_remapped(shard_id),
                engine.remaps.ensure_remapped(shard_id),
                engine.remaps.ensure_remapped(shard_id),
            )

        results = asyncio.run(race())
    finally:
        engine_module.map_shard_sync = original

    assert calls == 1
    assert len({result.generation for result in results}) == 1
    assert engine.remaps.dispatch_counts() == {shard_id: 1}
