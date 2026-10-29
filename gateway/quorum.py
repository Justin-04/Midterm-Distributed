"""
quorum.py

Quorum-based replication with read repair.

Implements the classic W + R > N quorum protocol:
  - Write Path: send chunk to N distinct nodes, wait for W acks.
  - Read Path:  query R nodes, verify hashes, return majority-verified copy.
  - Read Repair: if any replica returned stale/missing data while a healthy
    copy exists, asynchronously push the correct data back to the lagging node.

This module is transport-agnostic — it receives an httpx.AsyncClient and the
hash ring, and operates purely on chunk hashes and byte payloads.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Optional

import httpx
from ring import ConsistentHashRing, NodeInfo

logger = logging.getLogger(__name__)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Low-level node operations (thin wrappers around httpx)
# ---------------------------------------------------------------------------

async def _head_chunk(client: httpx.AsyncClient, node: NodeInfo, chunk_hash: str) -> bool:
    """Return True if the chunk exists on *node*."""
    try:
        r = await client.head(f"{node.url}/chunks/{chunk_hash}", timeout=10)
        return r.status_code == 200
    except httpx.RequestError:
        return False


async def _put_chunk(client: httpx.AsyncClient, node: NodeInfo, chunk_hash: str, data: bytes) -> bool:
    """Upload a chunk to *node*.  Returns True on 201."""
    try:
        r = await client.put(
            f"{node.url}/chunks/{chunk_hash}",
            content=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30,
        )
        return r.status_code == 201
    except httpx.RequestError as exc:
        logger.warning("PUT chunk %s to %s failed: %s", chunk_hash[:12], node.node_id, exc)
        return False


async def _get_chunk(client: httpx.AsyncClient, node: NodeInfo, chunk_hash: str) -> Optional[bytes]:
    """Download a chunk from *node*.  Returns None on any failure."""
    try:
        r = await client.get(f"{node.url}/chunks/{chunk_hash}", timeout=30)
        if r.status_code == 200:
            return r.content
        return None
    except httpx.RequestError:
        return None


async def _delete_chunk(client: httpx.AsyncClient, node: NodeInfo, chunk_hash: str) -> bool:
    """Delete a chunk from *node*.  Returns True on 204/404."""
    try:
        r = await client.delete(f"{node.url}/chunks/{chunk_hash}", timeout=15)
        return r.status_code in (204, 404)
    except httpx.RequestError:
        return False


# ---------------------------------------------------------------------------
# Quorum Write
# ---------------------------------------------------------------------------

async def quorum_write(
    client: httpx.AsyncClient,
    ring: ConsistentHashRing,
    chunk_hash: str,
    data: bytes,
    n: int,
    w: int,
) -> tuple[bool, list[str], list[str]]:
    """
    Write *data* (with hash *chunk_hash*) to N distinct nodes via the ring.

    Returns success once W nodes acknowledge (or all N if W ≤ N).

    Parameters
    ----------
    client : shared httpx.AsyncClient
    ring   : ConsistentHashRing
    chunk_hash : SHA-256 hex of *data*
    data   : raw bytes to store
    n      : replication factor (total replicas)
    w      : write quorum (minimum acks required for success)

    Returns
    -------
    (success, successful_node_ids, failed_node_ids)
    """
    replicas = ring.get_nodes(chunk_hash, n)
    if not replicas:
        return False, [], []

    async def _try_store(node: NodeInfo) -> tuple[str, bool]:
        # Dedup: skip upload if already present.
        if await _head_chunk(client, node, chunk_hash):
            logger.debug("Chunk %s already on %s — skipping upload.", chunk_hash[:12], node.node_id)
            return node.node_id, True
        ok = await _put_chunk(client, node, chunk_hash, data)
        return node.node_id, ok

    tasks = [asyncio.create_task(_try_store(n)) for n in replicas]

    acks = 0
    successful: list[str] = []
    failed: list[str] = []

    for coro in asyncio.as_completed(tasks):
        node_id, ok = await coro
        if ok:
            acks += 1
            successful.append(node_id)
        else:
            failed.append(node_id)

        if acks >= w:
            # Cancel any remaining tasks — quorum reached.
            for t in tasks:
                if not t.done():
                    t.cancel()
            break

    success = acks >= w
    if not success:
        logger.error(
            "Quorum write FAILED for chunk %s: %d/%d acks (needed %d).",
            chunk_hash[:12], acks, len(replicas), w,
        )
    else:
        logger.debug(
            "Quorum write OK for chunk %s: %d/%d acks (needed %d).",
            chunk_hash[:12], acks, len(replicas), w,
        )

    return success, successful, failed


# ---------------------------------------------------------------------------
# Quorum Read with Read Repair
# ---------------------------------------------------------------------------

async def quorum_read(
    client: httpx.AsyncClient,
    ring: ConsistentHashRing,
    chunk_hash: str,
    r: int,
    n: int,
) -> Optional[bytes]:
    """
    Read a chunk from R nodes, verify integrity, and trigger read repair.

    Read Repair: if some replicas returned stale/missing data while at least
    one returned a valid copy, the valid copy is pushed back to the
    lagging replicas asynchronously.

    Parameters
    ----------
    client : shared httpx.AsyncClient
    ring   : ConsistentHashRing
    chunk_hash : SHA-256 hex of the expected chunk
    r      : read quorum (minimum valid responses required)
    n      : replication factor (for selecting nodes)

    Returns
    -------
    The chunk bytes if quorum was satisfied, or None if no valid copy found.
    """
    replicas = ring.get_nodes(chunk_hash, n)
    if not replicas:
        return None

    valid_data: Optional[bytes] = None
    stale_nodes: list[NodeInfo] = []  # nodes that need read repair
    valid_count = 0

    async def _try_read(node: NodeInfo) -> tuple[NodeInfo, Optional[bytes], str]:
        """Returns (node, data_or_None, status) where status is 'ok'|'corrupt'|'missing'."""
        data = await _get_chunk(client, node, chunk_hash)
        if data is None:
            return node, None, "missing"
        computed = _sha256(data)
        if computed != chunk_hash.lower():
            logger.warning(
                "Hash mismatch on %s for chunk %s (computed=%s) — marking stale.",
                node.node_id, chunk_hash[:12], computed[:12],
            )
            return node, data, "corrupt"
        return node, data, "ok"

    tasks = [asyncio.create_task(_try_read(n)) for n in replicas]

    for coro in asyncio.as_completed(tasks):
        node, data, status = await coro
        if status == "ok":
            valid_count += 1
            if valid_data is None:
                valid_data = data
        else:
            stale_nodes.append(node)

        if valid_count >= r:
            # Quorum satisfied — cancel remaining tasks.
            for t in tasks:
                if not t.done():
                    t.cancel()
            break

    # ── Read Repair ─────────────────────────────────────────────────────
    if valid_data is not None and stale_nodes:
        logger.info(
            "Read repair: pushing chunk %s to %d stale node(s): %s",
            chunk_hash[:12], len(stale_nodes),
            [n.node_id for n in stale_nodes],
        )
        repair_tasks = [
            _put_chunk(client, node, chunk_hash, valid_data)
            for node in stale_nodes
        ]
        # Fire-and-forget — don't block the read on repair.
        asyncio.create_task(_do_read_repairs(repair_tasks))

    if valid_data is None:
        logger.error(
            "Quorum read FAILED for chunk %s: 0/%d valid responses.",
            chunk_hash[:12], len(replicas),
        )

    return valid_data


async def _do_read_repairs(repair_tasks: list[asyncio.Task]) -> None:
    """Execute read-repair uploads, logging results."""
    results = await asyncio.gather(*repair_tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.warning("Read repair upload failed: %s", r)


# ---------------------------------------------------------------------------
# Quorum Delete (best-effort cleanup of all replicas)
# ---------------------------------------------------------------------------

async def quorum_delete(
    client: httpx.AsyncClient,
    ring: ConsistentHashRing,
    chunk_hash: str,
    n: int,
) -> list[str]:
    """
    Delete a chunk from all N replicas.

    Returns the list of node_ids where deletion succeeded (204 or 404).
    """
    replicas = ring.get_nodes(chunk_hash, n)

    async def _try_delete(node: NodeInfo) -> tuple[str, bool]:
        ok = await _delete_chunk(client, node, chunk_hash)
        return node.node_id, ok

    tasks = [asyncio.create_task(_try_delete(n)) for n in replicas]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    deleted: list[str] = []
    for r in results:
        if isinstance(r, tuple):
            node_id, ok = r
            if ok:
                deleted.append(node_id)
    return deleted
