#!/usr/bin/env python3
"""End-to-end demo for the local MapReduce 5xx API.

Runs against a live service (Docker Compose default: http://localhost:8000):

  1. Submit three log shards and print the baseline per-route 5xx totals.
  2. Cut two complete records (and the trailer) off the tail of one shard's
     intermediate file for one partition -- a torn write that leaves only
     valid-looking lines behind.
  3. Show that the file no longer matches its manifest.
  4. Re-run the reduce.  Only that shard is re-mapped, both partitions still
     merge, and the totals exactly match the baseline.
  5. Submit a second job, corrupt a shard and also delete its persisted
     input, then show the batch fails with HTTP 500 and returns no totals.

Uses only the standard library so it can run inside the service image.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any

SHARDS: dict[str, list[tuple[str, int]]] = {
    "shard-a": [
        ("/api/orders", 500),
        ("/api/orders", 504),
        ("/api/orders", 500),
        ("/checkout", 502),
        ("/checkout", 500),
        ("/search", 500),
        ("/admin", 503),
        ("/login", 503),
        ("/login", 500),
        ("/catalog", 500),
        ("/metrics", 500),
        ("/health", 200),
    ],
    "shard-b": [
        ("/api/orders", 500),
        ("/checkout", 500),
        ("/login", 500),
        ("/health", 500),
        ("/catalog", 200),
    ],
    "shard-c": [
        ("/checkout", 500),
        ("/checkout", 502),
        ("/api/users", 500),
        ("/api/users", 200),
        ("/search", 503),
    ],
}


def request(
    method: str, url: str, payload: dict[str, Any] | None = None
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, json.loads(body) if body else {}


def batch_payload(shards: dict[str, list[tuple[str, int]]]) -> dict[str, Any]:
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


def expected_totals(shards: dict[str, list[tuple[str, int]]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for entries in shards.values():
        for route, status in entries:
            if 500 <= status <= 599:
                totals[route] = totals.get(route, 0) + 1
    return totals


def by_route(payload: dict[str, Any]) -> dict[str, int]:
    return {row["route"]: row["count"] for row in payload.get("routes", [])}


def show(label: str, payload: dict[str, Any]) -> None:
    print(f"  {label}: {json.dumps(payload, ensure_ascii=False, sort_keys=True)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="http://localhost:8000",
        help="base URL of the running service",
    )
    args = parser.parse_args()
    base = args.url.rstrip("/")

    status, health = request("GET", f"{base}/health")
    if status != 200:
        print(f"service is not healthy: HTTP {status} {health}", file=sys.stderr)
        return 2

    expected = expected_totals(SHARDS)
    print("== 1. baseline ==")
    status, baseline = request("POST", f"{base}/jobs", batch_payload(SHARDS))
    if status != 201:
        print(f"job submission failed: HTTP {status} {baseline}", file=sys.stderr)
        return 1
    job_id = baseline["job_id"]
    show("job_id", {"job_id": job_id})
    show("routes", by_route(baseline))
    show("total_5xx", {"total_5xx": baseline["total_5xx"]})
    if by_route(baseline) != expected:
        print("baseline totals are wrong", file=sys.stderr)
        return 1

    # /api/orders hashes to the same partition for every shard it appears in.
    # Dropping two complete records plus the trailer leaves valid JSON lines
    # but a file that is no longer the one the manifest describes.
    print("\n== 2. tear the tail off shard-a's /api/orders partition file ==")
    status, simulated = request(
        "POST",
        f"{base}/jobs/{job_id}/simulate-fault",
        {
            "shard_id": "shard-a",
            "mode": "truncate_tail",
            "partitions": [0, 1],
            "drop_records": 2,
        },
    )
    show(f"simulate-fault (HTTP {status})", simulated)
    if status != 200:
        print("fault simulation failed; are debug endpoints enabled?", file=sys.stderr)
        return 1

    status, intermediates = request("GET", f"{base}/jobs/{job_id}/intermediates")
    assert status == 200
    damaged = [
        {
            "shard": shard["shard_id"],
            "partition": f["partition"],
            "matches_manifest": f["matches_manifest"],
        }
        for shard in intermediates["shards"]
        for f in shard.get("files", [])
    ]
    show("intermediate files", damaged)

    print("\n== 3. re-run reduce; shard-a is re-mapped, totals must match ==")
    status, recovered = request("POST", f"{base}/jobs/{job_id}/reduce")
    show(f"reduce (HTTP {status})", recovered)
    if status != 200 or by_route(recovered) != expected:
        print("recovery did not return the expected totals", file=sys.stderr)
        return 1
    if recovered["remap_dispatches"] != {"shard-a": 1}:
        print("expected exactly one re-map dispatch for shard-a", file=sys.stderr)
        return 1
    print("  RESULT: totals recovered exactly; one re-map for one shard")

    print("\n== 4. unrecoverable batch: corrupt a shard and delete its input ==")
    status, failing = request("POST", f"{base}/jobs", batch_payload(SHARDS))
    if status != 201:
        print(f"job submission failed: HTTP {status} {failing}", file=sys.stderr)
        return 1
    failing_job = failing["job_id"]
    status, simulated = request(
        "POST",
        f"{base}/jobs/{failing_job}/simulate-fault",
        {
            "shard_id": "shard-b",
            "mode": "truncate_tail",
            "partitions": [0, 1],
            "drop_records": 1,
            "delete_input": True,
        },
    )
    show(f"simulate-fault (HTTP {status})", simulated)
    status, failure = request("POST", f"{base}/jobs/{failing_job}/reduce")
    show(f"reduce (HTTP {status})", failure)
    if status != 500 or failure.get("totals_returned") is not False:
        print("expected a batch failure without totals", file=sys.stderr)
        return 1
    print("  RESULT: batch failed, no aggregate returned")

    print("\nAll demo checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
