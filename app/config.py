"""Service configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Runtime settings.

    ``data_dir`` is the root for persisted input shards, intermediate map
    files and job manifests.  It is safe to point it at a bind mount.
    """

    data_dir: Path
    remap_attempts: int = 1
    remap_delay: float = 0.0
    debug_endpoints: bool = False
    map_partitions: int = 2

    def __post_init__(self) -> None:
        if self.map_partitions != 2:
            raise ValueError("map_partitions must be 2 for this design")
        if self.remap_attempts < 1:
            raise ValueError("remap_attempts must be >= 1")
        if self.remap_delay < 0:
            raise ValueError("remap_delay must be >= 0")

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.environ.get("MAPREDUCE_DATA_DIR", "data"))
        attempts = int(os.environ.get("MAPREDUCE_REMAP_ATTEMPTS", "1"))
        remap_delay = float(os.environ.get("MAPREDUCE_REMAP_DELAY_SECONDS", "0"))
        debug = os.environ.get("MAPREDUCE_ENABLE_DEBUG_ENDPOINTS", "0").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        partitions = int(os.environ.get("MAPREDUCE_PARTITIONS", "2"))
        return cls(
            data_dir=data_dir,
            remap_attempts=attempts,
            remap_delay=remap_delay,
            debug_endpoints=debug,
            map_partitions=partitions,
        )
