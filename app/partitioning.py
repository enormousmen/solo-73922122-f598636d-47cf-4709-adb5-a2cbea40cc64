"""Stable assignment of routes to one of the two reduce partitions."""

from __future__ import annotations

import hashlib

PARTITION_COUNT = 2


def partition_for_route(route: str) -> int:
    """Return the deterministic partition (0 or 1) for ``route``.

    ``hashlib`` is used instead of :func:`hash` so the assignment is stable
    across processes, restarts and Python hash seeds.  The same function is
    used by the mapper, the intermediate-file validator and the reducer.
    """
    digest = hashlib.blake2b(
        route.encode("utf-8"), digest_size=8, person=b"mr5xx-prt"
    ).digest()
    return int.from_bytes(digest, "big") % PARTITION_COUNT


def partition_label(partition: int) -> str:
    return f"p{partition}"
