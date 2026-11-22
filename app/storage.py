"""Filesystem layout and intermediate-file format.

Layout of one job::

    <data_dir>/jobs/<job_id>/
        job.json                    # metadata and shard list
        input/<shard_id>.json       # canonical copy of the uploaded shard
        map/<shard_id>/manifest.json
        map/<shard_id>/p0.jsonl
        map/<shard_id>/p1.jsonl

An intermediate file is newline-delimited JSON.  Every body line is one
``{"r": route, "c": count}`` record.  The body is followed by a trailer line
holding the record count, body length and body SHA-256.  A manifest records
the generation, size and SHA-256 of both complete files.

A tail that was cut off at a record boundary therefore loses the trailer and
is detected even though every remaining line is valid JSON.  A cut in the
middle of a record is detected by the missing newline / JSON parse error.
Either way the partition reader raises :class:`TransientIntermediateError`
and the engine re-maps exactly that shard.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, BinaryIO, Iterable

from .errors import RemapError, TransientIntermediateError
from .partitioning import PARTITION_COUNT, partition_for_route, partition_label

INTERMEDIATE_MAGIC = "mr5xx-intermediate"
INTERMEDIATE_VERSION = 1
# Generous for a 2048-byte route plus a count and JSON punctuation, but small
# enough that a corrupt run-on line cannot pull a whole file into memory.
MAX_LINE_BYTES = 64 * 1024


# ---------------------------------------------------------------------------
# Atomic file helpers
# ---------------------------------------------------------------------------


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes, mode: int = 0o644) -> None:
    """Write ``data`` to ``path`` atomically (temp file + ``os.replace``)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, mode)
        os.replace(tmp_name, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, payload: Any) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    atomic_write_bytes(path, data + b"\n")


# ---------------------------------------------------------------------------
# Job paths
# ---------------------------------------------------------------------------


def job_dir(data_dir: Path, job_id: str) -> Path:
    return data_dir / "jobs" / job_id


def input_path(data_dir: Path, job_id: str, shard_id: str) -> Path:
    return job_dir(data_dir, job_id) / "input" / f"{shard_id}.json"


def map_dir(data_dir: Path, job_id: str, shard_id: str) -> Path:
    return job_dir(data_dir, job_id) / "map" / shard_id


def manifest_path(data_dir: Path, job_id: str, shard_id: str) -> Path:
    return map_dir(data_dir, job_id, shard_id) / "manifest.json"


def intermediate_path(
    data_dir: Path, job_id: str, shard_id: str, partition: int
) -> Path:
    return map_dir(data_dir, job_id, shard_id) / f"{partition_label(partition)}.jsonl"


# ---------------------------------------------------------------------------
# Input shards
# ---------------------------------------------------------------------------


def save_input(data_dir: Path, job_id: str, shard_id: str, entries: list[dict]) -> None:
    atomic_write_json(
        input_path(data_dir, job_id, shard_id),
        {"shard_id": shard_id, "entries": entries},
    )


def load_input_entries(data_dir: Path, job_id: str, shard_id: str) -> list[dict]:
    """Load the persisted copy of a shard.

    Raises :class:`RemapError` when the input cannot be used, which makes a
    failed re-map abort the batch instead of returning an incomplete total.
    """
    path = input_path(data_dir, job_id, shard_id)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise RemapError(f"input for shard {shard_id!r} is unreadable: {exc}") from exc
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise RemapError(f"input for shard {shard_id!r} is corrupt: {exc}") from exc
    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        raise RemapError(f"input for shard {shard_id!r} has no entry list")
    return entries


# ---------------------------------------------------------------------------
# Intermediate files: writing
# ---------------------------------------------------------------------------


def _encode_intermediate(
    shard_id: str, partition: int, counts: dict[str, int]
) -> tuple[bytes, dict[str, Any]]:
    body = bytearray()
    body_hasher = hashlib.sha256()
    records = 0
    total = 0
    for route in sorted(counts):
        count = counts[route]
        if partition_for_route(route) != partition:
            raise RemapError(
                f"route {route!r} does not belong to partition p{partition}"
            )
        line = (json.dumps({"r": route, "c": count}, ensure_ascii=False) + "\n").encode(
            "utf-8"
        )
        body.extend(line)
        body_hasher.update(line)
        records += 1
        total += count
    trailer = {
        "_trailer": True,
        "magic": INTERMEDIATE_MAGIC,
        "version": INTERMEDIATE_VERSION,
        "shard_id": shard_id,
        "partition": partition,
        "records": records,
        "body_bytes": len(body),
        "body_sha256": body_hasher.hexdigest(),
        "total_5xx": total,
    }
    data = bytes(body) + (
        json.dumps(trailer, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    meta = {
        "file": f"{partition_label(partition)}.jsonl",
        "records": records,
        "body_bytes": len(body),
        "body_sha256": body_hasher.hexdigest(),
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "total_5xx": total,
    }
    return data, meta


def write_intermediates(
    data_dir: Path,
    job_id: str,
    shard_id: str,
    counts_by_partition: dict[int, dict[str, int]],
) -> dict[str, Any]:
    """Write both partition files for a shard, then publish a new manifest.

    Each file is replaced atomically.  The manifest is written last and is
    the readers' commit point; its generation increments on every re-map.
    """
    target_dir = map_dir(data_dir, job_id, shard_id)
    target_dir.mkdir(parents=True, exist_ok=True)

    old_generation = read_generation_or_zero(data_dir, job_id, shard_id)
    generation = old_generation + 1

    files: dict[str, Any] = {}
    for partition in range(PARTITION_COUNT):
        data, meta = _encode_intermediate(
            shard_id, partition, counts_by_partition.get(partition, {})
        )
        path = target_dir / meta["file"]
        atomic_write_bytes(path, data)
        files[partition_label(partition)] = meta

    manifest = {
        "magic": INTERMEDIATE_MAGIC,
        "version": INTERMEDIATE_VERSION,
        "job_id": job_id,
        "shard_id": shard_id,
        "generation": generation,
        "partitions": PARTITION_COUNT,
        "files": files,
    }
    atomic_write_json(manifest_path(data_dir, job_id, shard_id), manifest)
    return manifest


# ---------------------------------------------------------------------------
# Intermediate files: reading
# ---------------------------------------------------------------------------


def _readline(handle: BinaryIO, limit: int = MAX_LINE_BYTES) -> bytes:
    """Read one line with a hard cap so memory cannot scale with file size."""
    try:
        line = handle.readline(limit + 1)
    except OSError as exc:
        raise TransientIntermediateError(f"read failed: {exc}") from exc
    if len(line) > limit:
        raise TransientIntermediateError(
            f"record exceeds the {limit}-byte line limit (corrupt file)"
        )
    return line


def read_manifest(data_dir: Path, job_id: str, shard_id: str) -> dict[str, Any]:
    path = manifest_path(data_dir, job_id, shard_id)
    try:
        payload = json.loads(path.read_bytes())
    except FileNotFoundError as exc:
        raise TransientIntermediateError(
            f"manifest for shard {shard_id!r} is missing"
        ) from exc
    except OSError as exc:
        raise TransientIntermediateError(
            f"manifest for shard {shard_id!r} is unreadable: {exc}"
        ) from exc
    except (ValueError, UnicodeDecodeError) as exc:
        raise TransientIntermediateError(
            f"manifest for shard {shard_id!r} is corrupt: {exc}"
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("generation"), int):
        raise TransientIntermediateError(
            f"manifest for shard {shard_id!r} has no generation"
        )
    return payload


def read_generation_or_zero(data_dir: Path, job_id: str, shard_id: str) -> int:
    try:
        return int(read_manifest(data_dir, job_id, shard_id)["generation"])
    except (TransientIntermediateError, KeyError, TypeError, ValueError):
        return 0


def read_intermediate(
    data_dir: Path,
    job_id: str,
    shard_id: str,
    partition: int,
    manifest: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Stream one intermediate file and return its route counts.

    The file is consumed one line at a time, so memory use does not depend on
    the file size.  Counts are accumulated in a local dict and returned only
    after the trailer, body digest, complete-file digest and manifest all
    check out.  If anything fails the caller discards the dict, so a partial
    read can never be added twice.
    """
    if manifest is None:
        manifest = read_manifest(data_dir, job_id, shard_id)
    label = partition_label(partition)
    try:
        meta = manifest["files"][label]
    except (KeyError, TypeError) as exc:
        raise TransientIntermediateError(
            f"manifest for shard {shard_id!r} has no entry for {label}"
        ) from exc

    path = intermediate_path(data_dir, job_id, shard_id, partition)
    counts: dict[str, int] = defaultdict(int)
    file_hasher = hashlib.sha256()
    body_hasher = hashlib.sha256()
    file_bytes = 0
    body_bytes = 0
    records = 0
    total_5xx = 0
    trailer: dict[str, Any] | None = None

    try:
        handle = path.open("rb")
    except FileNotFoundError as exc:
        raise TransientIntermediateError(
            f"intermediate file {path.name} for shard {shard_id!r} is missing"
        ) from exc
    except OSError as exc:
        raise TransientIntermediateError(
            f"intermediate file {path.name} for shard {shard_id!r} is unreadable: {exc}"
        ) from exc

    with handle:
        while True:
            line = _readline(handle)
            if not line:
                break
            file_bytes += len(line)
            file_hasher.update(line)
            if not line.endswith(b"\n"):
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    "ends in a partial record (truncated)"
                )
            if trailer is not None:
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    "has data after its trailer"
                )
            try:
                obj = json.loads(line)
            except (ValueError, UnicodeDecodeError) as exc:
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    f"has a malformed record: {exc}"
                ) from exc
            if not isinstance(obj, dict):
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    "has a non-object record"
                )
            if obj.get("_trailer") is True:
                trailer = obj
                continue

            route = obj.get("r")
            count = obj.get("c")
            if (
                not isinstance(route, str)
                or not route
                or not isinstance(count, int)
                or isinstance(count, bool)
                or count <= 0
            ):
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    "has an invalid count record"
                )
            if partition_for_route(route) != partition:
                raise TransientIntermediateError(
                    f"intermediate file {path.name} for shard {shard_id!r} "
                    f"contains route {route!r} from the wrong partition"
                )
            counts[route] += count
            records += 1
            total_5xx += count
            body_hasher.update(line)
            body_bytes += len(line)

    if trailer is None:
        raise TransientIntermediateError(
            f"intermediate file {path.name} for shard {shard_id!r} has no "
            "trailer (tail truncated)"
        )

    expected = {
        "records": records,
        "body_bytes": body_bytes,
        "body_sha256": body_hasher.hexdigest(),
        "total_5xx": total_5xx,
        "shard_id": shard_id,
        "partition": partition,
    }
    for key, value in expected.items():
        if trailer.get(key) != value:
            raise TransientIntermediateError(
                f"intermediate file {path.name} for shard {shard_id!r} "
                f"failed trailer validation on {key}"
            )

    if meta.get("sha256") != file_hasher.hexdigest() or meta.get("bytes") != file_bytes:
        raise TransientIntermediateError(
            f"intermediate file {path.name} for shard {shard_id!r} does not "
            "match its manifest digest"
        )
    if meta.get("records") != records:
        raise TransientIntermediateError(
            f"intermediate file {path.name} for shard {shard_id!r} record "
            "count does not match its manifest"
        )
    return dict(counts)


# ---------------------------------------------------------------------------
# Debug/fault-simulation helpers
# ---------------------------------------------------------------------------


def truncate_intermediate_tail(
    data_dir: Path,
    job_id: str,
    shard_id: str,
    partition: int,
    drop_records: int,
) -> dict[str, Any]:
    """Remove ``drop_records`` complete body records *and* the trailer.

    This deliberately writes in place and deliberately does not update the
    manifest, reproducing a torn tail.  Only a complete-record suffix can be
    removed; ``drop_records`` must fit in the body.
    """
    path = intermediate_path(data_dir, job_id, shard_id, partition)
    if drop_records < 1:
        raise ValueError("drop_records must be >= 1")
    try:
        lines = path.read_bytes().splitlines(keepends=True)
    except OSError as exc:
        raise ValueError(f"cannot read intermediate file: {exc}") from exc
    if not lines:
        raise ValueError("intermediate file is empty")
    try:
        trailer = json.loads(lines[-1])
    except ValueError as exc:
        raise ValueError("intermediate file has no readable trailer") from exc
    if not isinstance(trailer, dict) or trailer.get("_trailer") is not True:
        raise ValueError("intermediate file has no trailer")
    body = lines[:-1]
    if drop_records > len(body):
        raise ValueError(f"cannot drop {drop_records} records; file has {len(body)}")
    kept = body[: len(body) - drop_records]
    truncated = b"".join(kept)
    with path.open("wb") as handle:
        handle.write(truncated)
        handle.flush()
        os.fsync(handle.fileno())
    return {
        "shard_id": shard_id,
        "partition": f"p{partition}",
        "file": path.name,
        "dropped_records": drop_records,
        "records_remaining": len(kept),
        "bytes_remaining": len(truncated),
    }


def delete_input(data_dir: Path, job_id: str, shard_id: str) -> dict[str, Any]:
    path = input_path(data_dir, job_id, shard_id)
    try:
        path.unlink()
    except FileNotFoundError as exc:
        raise ValueError("input file is already missing") from exc
    except OSError as exc:
        raise ValueError(f"cannot delete input file: {exc}") from exc
    return {"shard_id": shard_id, "deleted": str(path)}


def apply_simulation(
    data_dir: Path,
    job_id: str,
    shard_id: str,
    partitions: Iterable[int],
    drop_records: int,
    mode: str,
    delete_input_too: bool,
) -> dict[str, Any]:
    applied: dict[str, Any] = {"mode": mode, "operations": []}
    if mode == "truncate_tail":
        for partition in sorted(set(partitions)):
            applied["operations"].append(
                truncate_intermediate_tail(
                    data_dir, job_id, shard_id, partition, drop_records
                )
            )
    elif mode == "delete_file":
        for partition in sorted(set(partitions)):
            path = intermediate_path(data_dir, job_id, shard_id, partition)
            try:
                path.unlink()
            except FileNotFoundError as exc:
                raise ValueError(
                    f"intermediate file {path.name} is already missing"
                ) from exc
            applied["operations"].append(
                {"shard_id": shard_id, "partition": f"p{partition}", "file": path.name}
            )
    else:
        raise ValueError(f"unknown simulation mode {mode!r}")
    if delete_input_too:
        applied["operations"].append(delete_input(data_dir, job_id, shard_id))
    return applied
