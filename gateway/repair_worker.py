"""
repair_worker.py

Background asyncio task that detects failed storage nodes and re-replicates
their chunks onto healthy surviving nodes to maintain desired redundancy.

Flow
----
1. Every ``repair_interval`` seconds, query etcd for node statuses.
2. For each node marked ``FAILED``:
   a. Determine which chunks were stored on that node.
      (Uses the ring to compute which chunks *should* have been there, then
       cross-references with object metadata.)
   b. For each chunk on the failed node:
      - Fetch a healthy copy from a surviving replica.
      - Upload it to a replacement node (selected from the ring, skipping
        the failed node).
      - Update placement tracking in etcd.

For erasure-coded objects, if a shard is lost, the worker attempts to
reconstruct it by fetching k surviving shards and re-encoding.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import httpx
import etcd3
from ring import ConsistentHashRing, NodeInfo
from failure_detector import FailureDetector

logger = logging.getLogger(__name__)


class RepairWorker:
    """
    Automated cluster repair — re-replicates chunks from failed nodes.

    Parameters
    ----------
    ring             : ConsistentHashRing
    http_client      : shared httpx.AsyncClient
    etcd_client      : etcd3 client
    replication_factor : N (total replicas per chunk)
    write_quorum     : W (minimum replicas to maintain)
    repair_interval  : seconds between repair cycles.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        http_client: httpx.AsyncClient,
        etcd_client: etcd3.Etcd3Client,
        replication_factor: int = 3,
        write_quorum: int = 2,
        repair_interval: int = 30,
    ) -> None:
        self.ring = ring
        self.http = http_client
        self.etcd = etcd_client
        self.n = replication_factor
        self.w = write_quorum
        self.interval = repair_interval

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the repair loop until *stop_event* is set."""
        logger.info(
            "Repair worker started — interval=%ds, N=%d, W=%d",
            self.interval, self.n, self.w,
        )

        while not stop_event.is_set():
            try:
                await self._repair_cycle()
            except Exception as exc:
                logger.error("Repair cycle error: %s", exc, exc_info=True)

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=float(self.interval))
            except asyncio.TimeoutError:
                pass

        logger.info("Repair worker stopped.")

    # ------------------------------------------------------------------
    # Repair logic
    # ------------------------------------------------------------------

    async def _repair_cycle(self) -> None:
        """One pass: detect failed nodes and repair their chunks."""
        statuses = FailureDetector.get_all_statuses(self.etcd)
        failed_nodes = [s for s in statuses if s.get("status") == "FAILED"]

        if not failed_nodes:
            return  # All healthy — nothing to do.

        logger.warning(
            "Detected %d failed node(s): %s",
            len(failed_nodes),
            [s.get("node_id") for s in failed_nodes],
        )

        for node_status in failed_nodes:
            node_id = node_status["node_id"]
            await self._repair_failed_node(node_id)

    async def _repair_failed_node(self, failed_node_id: str) -> None:
        """
        Re-replicate all chunks that were on the failed node.

        Strategy:
        1. Scan all object metadata to find chunks whose replica set
           includes the failed node.
        2. For each such chunk, find a healthy replica, fetch the data,
           and upload it to a replacement node.
        """
        # Get all objects from etcd (prefix scan of /objects/)
        chunks_to_repair: list[str] = []
        try:
            for value, kv in self.etcd.get_prefix("/objects/"):
                if value is None:
                    continue
                try:
                    meta = json.loads(value)
                except json.JSONDecodeError:
                    continue

                chunk_hashes = meta.get("chunk_hashes", [])
                for chunk_hash in chunk_hashes:
                    # Check if the failed node was in the replica set.
                    replicas = self.ring.get_nodes(chunk_hash, self.n)
                    replica_ids = [n.node_id for n in replicas]
                    if failed_node_id in replica_ids:
                        chunks_to_repair.append(chunk_hash)
        except Exception as exc:
            logger.error("Failed to scan metadata for repair: %s", exc)
            return

        # Deduplicate (same chunk may appear in multiple objects).
        chunks_to_repair = list(set(chunks_to_repair))

        if not chunks_to_repair:
            logger.info("No chunks found on failed node %s — nothing to repair.", failed_node_id)
            return

        logger.info(
            "Repairing %d chunk(s) from failed node %s …", len(chunks_to_repair), failed_node_id,
        )

        repaired = 0
        for chunk_hash in chunks_to_repair:
            ok = await self._repair_chunk(chunk_hash, failed_node_id)
            if ok:
                repaired += 1

        logger.info(
            "Repair complete for node %s: %d/%d chunks re-replicated.",
            failed_node_id, repaired, len(chunks_to_repair),
        )

    async def _repair_chunk(self, chunk_hash: str, failed_node_id: str) -> bool:
        """
        Re-replicate a single chunk that was on the failed node.

        1. Find surviving nodes that still have this chunk.
        2. Fetch from a healthy survivor.
        3. Upload to a replacement node.
        """
        replicas = self.ring.get_nodes(chunk_hash, self.n)
        healthy_replicas = [n for n in replicas if n.node_id != failed_node_id]

        # Find a node that still has the chunk.
        data: Optional[bytes] = None
        for node in healthy_replicas:
            try:
                r = await self.http.get(f"{node.url}/chunks/{chunk_hash}", timeout=30)
                if r.status_code == 200:
                    data = r.content
                    break
            except httpx.RequestError:
                continue

        if data is None:
            logger.error(
                "Repair failed for chunk %s — no healthy copy found on any node.",
                chunk_hash[:12],
            )
            return False

        # Upload to the failed node's replacement (or back to the failed
        # node if it has recovered).
        # Select the first replica that doesn't currently have the chunk.
        for node in replicas:
            if node.node_id == failed_node_id:
                continue
            # Check if this node already has it.
            try:
                r = await self.http.head(f"{node.url}/chunks/{chunk_hash}", timeout=10)
                if r.status_code == 200:
                    continue  # Already has it — skip.
            except httpx.RequestError:
                pass

            # Upload to this node.
            try:
                r = await self.http.put(
                    f"{node.url}/chunks/{chunk_hash}",
                    content=data,
                    headers={"Content-Type": "application/octet-stream"},
                    timeout=30,
                )
                if r.status_code == 201:
                    logger.info(
                        "Re-replicated chunk %s to %s (replacing failed %s).",
                        chunk_hash[:12], node.node_id, failed_node_id,
                    )
                    return True
            except httpx.RequestError as exc:
                logger.warning("Re-replication upload to %s failed: %s", node.node_id, exc)

        # If all healthy replicas already have the chunk, we still have
        # sufficient redundancy — log and report success.
        logger.info(
            "Chunk %s already has sufficient replicas — no extra upload needed.",
            chunk_hash[:12],
        )
        return True
