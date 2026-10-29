"""
scrubber.py

Background bit-rot scrubber for storage nodes.

Runs as an asyncio background task alongside the FastAPI server.  Every
``scrub_interval`` seconds, it walks the sharded chunk directory tree and
recomputes SHA-256 for every stored chunk file.  If the computed hash does
not match the filename (which IS the expected hash), the file is silently
corrupted (bit rot):

1. Delete the corrupted file immediately to prevent serving bad data.
2. Report the corruption to the gateway's repair endpoint, which fetches
   a healthy copy from a surviving replica and re-uploads it to this node.

The scrubber is self-throttling (yields control between files) so it does
not starve request-handling coroutines.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Default: scan every 60 seconds
DEFAULT_SCRUB_INTERVAL: int = 60


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file on disk (streaming to avoid loading huge files into memory)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(1024 * 1024)  # 1 MB read buffer
            if not block:
                break
            h.update(block)
    return h.hexdigest()


async def scrub_loop(
    storage_dir: Path,
    gateway_url: str,
    node_id: str,
    interval: int = DEFAULT_SCRUB_INTERVAL,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """
    Continuously scan the storage directory for bit rot.

    Parameters
    ----------
    storage_dir : root directory where chunks are stored (sharded layout).
    gateway_url  : base URL of the API gateway (for repair callbacks).
    node_id      : this storage node's identifier (for repair request).
    interval     : seconds between full scans.
    stop_event   : optional asyncio.Event for graceful shutdown.
    """
    logger.info(
        "Bit-rot scrubber started — dir=%s, interval=%ds, gateway=%s",
        storage_dir, interval, gateway_url,
    )

    while True:
        try:
            await _scrub_pass(storage_dir, gateway_url, node_id)
        except Exception as exc:
            logger.error("Scrubber pass error: %s", exc, exc_info=True)

        if stop_event is not None:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=float(interval))
                logger.info("Scrubber stopping — stop event received.")
                break
            except asyncio.TimeoutError:
                pass  # interval elapsed, continue
        else:
            await asyncio.sleep(interval)

    logger.info("Bit-rot scrubber stopped.")


async def _scrub_pass(storage_dir: Path, gateway_url: str, node_id: str) -> None:
    """
    One full scan of the chunk directory.

    For each file, recompute SHA-256 and compare with the filename.
    Corrupted files are deleted and reported to the gateway for repair.
    """
    if not storage_dir.exists():
        return

    scanned = 0
    corrupted = 0
    repaired = 0

    async with httpx.AsyncClient(timeout=60) as client:
        for shard_dir in storage_dir.iterdir():
            if not shard_dir.is_dir():
                continue

            for chunk_file in shard_dir.iterdir():
                # Skip temp files from atomic writes.
                if chunk_file.suffix == ".tmp" or chunk_file.is_dir():
                    continue

                expected_hash = chunk_file.name
                scanned += 1

                # Yield control to the event loop periodically.
                if scanned % 50 == 0:
                    await asyncio.sleep(0)

                try:
                    computed_hash = await asyncio.get_event_loop().run_in_executor(
                        None, _sha256_file, chunk_file,
                    )
                except OSError as exc:
                    logger.warning("Could not read %s: %s — quarantining.", chunk_file, exc)
                    chunk_file.unlink(missing_ok=True)
                    corrupted += 1
                    continue

                if computed_hash != expected_hash:
                    corrupted += 1
                    logger.error(
                        "BIT ROT DETECTED: chunk %s (file=%s, computed=%s). "
                        "Quarantining and requesting repair from gateway.",
                        expected_hash[:16], chunk_file, computed_hash[:16],
                    )

                    # Delete the corrupted file.
                    chunk_file.unlink(missing_ok=True)

                    # Request repair from the gateway.
                    ok = await _request_repair(client, gateway_url, expected_hash, node_id)
                    if ok:
                        repaired += 1
                        logger.info(
                            "Repair requested for chunk %s — gateway acknowledged.",
                            expected_hash[:16],
                        )
                    else:
                        logger.error(
                            "Repair request FAILED for chunk %s — "
                            "gateway may be unreachable or no healthy copy exists.",
                            expected_hash[:16],
                        )

    if scanned > 0:
        logger.info(
            "Scrub pass complete: %d scanned, %d corrupted, %d repaired.",
            scanned, corrupted, repaired,
        )


async def _request_repair(
    client: httpx.AsyncClient,
    gateway_url: str,
    chunk_hash: str,
    node_id: str,
) -> bool:
    """
    POST to the gateway's repair endpoint to trigger re-replication.

    Returns True if the gateway acknowledged the repair request.
    """
    try:
        r = await client.post(
            f"{gateway_url.rstrip('/')}/internal/repair/{chunk_hash}",
            json={"node_id": node_id},
            timeout=30,
        )
        return r.status_code in (200, 201)
    except httpx.RequestError as exc:
        logger.warning("Repair request to gateway failed: %s", exc)
        return False
