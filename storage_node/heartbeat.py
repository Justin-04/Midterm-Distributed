"""
heartbeat.py

Background asyncio task that periodically reports this storage node's
health and capacity to the Metadata Service.

Payload schema
--------------
{
    "node_id":         "<str>",   # unique node identifier (NODE_ID env var)
    "free_space_bytes": <int>,    # available bytes on the storage volume
    "chunk_count":      <int>     # number of chunks currently stored
}
"""

import asyncio
import logging
from pathlib import Path
from typing import Optional

import httpx

from storage import count_chunks, disk_free_bytes

logger = logging.getLogger(__name__)

# Default interval between heartbeat transmissions (seconds).
HEARTBEAT_INTERVAL: int = 5


async def _send_heartbeat(
    client: httpx.AsyncClient,
    metadata_url: str,
    node_id: str,
    storage_dir: Path,
) -> None:
    """
    Build and POST a single heartbeat payload to the Metadata Service.

    Errors are logged but never re-raised so that a transient network
    failure does not kill the background task.
    """
    payload = {
        "node_id": node_id,
        "free_space_bytes": disk_free_bytes(storage_dir),
        "chunk_count": count_chunks(storage_dir),
    }

    try:
        response = await client.post(
            f"{metadata_url.rstrip('/')}/heartbeat",
            json=payload,
            timeout=4.0,  # slightly shorter than the interval
        )
        response.raise_for_status()
        logger.debug(
            "Heartbeat sent to %s — node=%s chunks=%d free=%d bytes",
            metadata_url,
            node_id,
            payload["chunk_count"],
            payload["free_space_bytes"],
        )
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "Heartbeat rejected by metadata service: %s %s",
            exc.response.status_code,
            exc.response.text,
        )
    except httpx.RequestError as exc:
        # Network-level failure (DNS, connection refused, timeout, …).
        logger.warning("Heartbeat delivery failed (%s): %s", type(exc).__name__, exc)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Unexpected error in heartbeat: %s", exc, exc_info=True)


async def heartbeat_loop(
    node_id: str,
    metadata_url: str,
    storage_dir: Path,
    interval: int = HEARTBEAT_INTERVAL,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """
    Continuously send heartbeats until *stop_event* is set (or forever).

    Parameters
    ----------
    node_id:
        Unique identifier for this node, sent in every payload.
    metadata_url:
        Base URL of the Metadata Service (e.g. ``http://metadata:8000``).
    storage_dir:
        Root storage directory used to compute chunk count and free space.
    interval:
        Seconds to wait between heartbeat attempts.
    stop_event:
        Optional :class:`asyncio.Event`; when set the loop exits cleanly.
        Useful for graceful shutdown in tests and production alike.
    """
    logger.info(
        "Heartbeat loop started — node_id=%s, target=%s, interval=%ds",
        node_id, metadata_url, interval,
    )

    # Re-use a single AsyncClient across all heartbeats for connection pooling.
    async with httpx.AsyncClient() as client:
        while True:
            await _send_heartbeat(client, metadata_url, node_id, storage_dir)

            if stop_event is not None:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=float(interval))
                    logger.info("Heartbeat loop stopping — stop event received.")
                    break
                except asyncio.TimeoutError:
                    pass  # Normal path: interval elapsed, continue looping.
            else:
                await asyncio.sleep(interval)
