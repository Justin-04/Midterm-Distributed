"""
storage.py

Handles all disk I/O for content-addressed chunk storage.

Chunks are stored under STORAGE_DIR using a two-level sharded layout:
    <STORAGE_DIR>/<first-2-hex-chars>/<full-sha256-hex>

This prevents filesystem performance degradation caused by placing tens of
thousands of files in a single flat directory.
"""

import hashlib
import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _shard_dir(storage_dir: Path, chunk_hash: str) -> Path:
    """Return the shard sub-directory for *chunk_hash* (first 2 hex chars)."""
    return storage_dir / chunk_hash[:2]


def _chunk_path(storage_dir: Path, chunk_hash: str) -> Path:
    """Return the full path where a chunk with *chunk_hash* is stored."""
    return _shard_dir(storage_dir, chunk_hash) / chunk_hash


def _sha256(data: bytes) -> str:
    """Return the hex-encoded SHA-256 digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def _ensure_shard_dir(storage_dir: Path, chunk_hash: str) -> Path:
    """Create the shard directory if it does not exist, then return it."""
    shard = _shard_dir(storage_dir, chunk_hash)
    shard.mkdir(parents=True, exist_ok=True)
    return shard


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class HashMismatchError(ValueError):
    """Raised when the computed hash does not match the expected hash."""


class ChunkNotFoundError(FileNotFoundError):
    """Raised when the requested chunk is not present on disk."""


class ChunkCorruptedError(OSError):
    """Raised when a stored chunk fails its integrity check."""


def store_chunk(storage_dir: Path, chunk_hash: str, data: bytes) -> None:
    """
    Write *data* to disk under *chunk_hash*.

    Parameters
    ----------
    storage_dir:
        Root directory for chunk storage.
    chunk_hash:
        Expected SHA-256 hex digest of *data*.
    data:
        Raw binary payload.

    Raises
    ------
    HashMismatchError
        If ``sha256(data) != chunk_hash``.
    """
    computed = _sha256(data)
    if computed != chunk_hash.lower():
        raise HashMismatchError(
            f"Hash mismatch: provided={chunk_hash!r}, computed={computed!r}"
        )

    _ensure_shard_dir(storage_dir, chunk_hash)
    path = _chunk_path(storage_dir, chunk_hash)

    # Atomic write: write to a temp file then rename so a partial write never
    # leaves a corrupt chunk visible to concurrent readers.
    tmp_path = path.with_suffix(".tmp")
    try:
        tmp_path.write_bytes(data)
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    logger.info("Stored chunk %s (%d bytes) at %s", chunk_hash, len(data), path)


def load_chunk(storage_dir: Path, chunk_hash: str) -> bytes:
    """
    Read and integrity-check a chunk from disk.

    Parameters
    ----------
    storage_dir:
        Root directory for chunk storage.
    chunk_hash:
        SHA-256 hex digest identifying the chunk.

    Returns
    -------
    bytes
        The raw chunk data.

    Raises
    ------
    ChunkNotFoundError
        If no file exists for *chunk_hash*.
    ChunkCorruptedError
        If the file exists but its content no longer matches *chunk_hash*.
        The corrupted file is deleted before raising.
    """
    path = _chunk_path(storage_dir, chunk_hash)

    if not path.exists():
        raise ChunkNotFoundError(f"Chunk not found: {chunk_hash!r}")

    data = path.read_bytes()
    computed = _sha256(data)

    if computed != chunk_hash.lower():
        logger.error(
            "Corruption detected for chunk %s (file=%s, computed=%s) — removing.",
            chunk_hash, path, computed,
        )
        # Remove the corrupted file so subsequent writes can heal it.
        path.unlink(missing_ok=True)
        raise ChunkCorruptedError(
            f"Chunk {chunk_hash!r} is corrupted: computed hash={computed!r}"
        )

    logger.debug("Loaded chunk %s (%d bytes)", chunk_hash, len(data))
    return data


def delete_chunk(storage_dir: Path, chunk_hash: str) -> None:
    """
    Delete a chunk from disk.

    Parameters
    ----------
    storage_dir:
        Root directory for chunk storage.
    chunk_hash:
        SHA-256 hex digest identifying the chunk.

    Raises
    ------
    ChunkNotFoundError
        If no file exists for *chunk_hash*.
    """
    path = _chunk_path(storage_dir, chunk_hash)

    if not path.exists():
        raise ChunkNotFoundError(f"Chunk not found: {chunk_hash!r}")

    path.unlink()
    logger.info("Deleted chunk %s", chunk_hash)

    # Clean up the shard directory if it is now empty (best-effort).
    try:
        shard = _shard_dir(storage_dir, chunk_hash)
        if not any(shard.iterdir()):
            shard.rmdir()
    except OSError:
        pass  # Non-fatal — directory may have been recreated concurrently.


def chunk_exists(storage_dir: Path, chunk_hash: str) -> bool:
    """Return ``True`` if a chunk file is present on disk for *chunk_hash*."""
    return _chunk_path(storage_dir, chunk_hash).exists()


def count_chunks(storage_dir: Path) -> int:
    """
    Count the total number of chunk files stored under *storage_dir*.

    Walks the sharded directory tree and counts every regular file,
    ignoring any ``.tmp`` files left by interrupted writes.
    """
    if not storage_dir.exists():
        return 0

    total = 0
    for entry in storage_dir.iterdir():
        if entry.is_dir():
            total += sum(
                1
                for f in entry.iterdir()
                if f.is_file() and not f.suffix == ".tmp"
            )
    return total


def disk_free_bytes(storage_dir: Path) -> int:
    """Return free disk space (in bytes) on the volume that hosts *storage_dir*."""
    storage_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(str(storage_dir))
    return usage.free
