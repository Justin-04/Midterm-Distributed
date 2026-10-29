"""
failure_detector.py

Background asyncio task that monitors storage node liveness and tracks
node status in etcd.

Detection strategy
------------------
- Polls each registered storage node's ``/health`` endpoint every
  ``check_interval`` seconds (default: 5 s).
- Records a timestamp + status in etcd under ``/nodes/{node_id}``.
- If a node does not respond for longer than ``failure_threshold`` seconds
  (default: 15 s), it is marked ``FAILED``.
- When a node recovers and starts responding again, it is marked ``ACTIVE``.

The failure detector is intentionally simple (active health-check polling)
rather than relying on the existing heartbeat mechanism.  This keeps the
detection logic self-contained in the gateway without requiring storage
nodes to write to etcd directly.

etcd schema
-----------
    /nodes/{node_id}/status      → "ACTIVE" | "FAILED"
    /nodes/{node_id}/last_seen   → ISO-8601 timestamp string
    /nodes/{node_id}/address     → node URL (e.g. http://storage_node1:8000)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import etcd3
from ring import ConsistentHashRing

logger = logging.getLogger(__name__)

# etcd key prefix for node tracking
_NODE_PREFIX = "/nodes/"


def _node_key(node_id: str, field: str) -> str:
    return f"{_NODE_PREFIX}{node_id}/{field}"


class FailureDetector:
    """
    Monitors storage node health and records status in etcd.

    Parameters
    ----------
    ring            : ConsistentHashRing with registered storage nodes.
    http_client     : shared httpx.AsyncClient for health probes.
    etcd_client     : etcd3 client for status persistence.
    check_interval  : seconds between health-check rounds.
    failure_threshold : seconds without a successful probe before marking FAILED.
    probe_timeout   : per-request timeout for health probes.
    """

    def __init__(
        self,
        ring: ConsistentHashRing,
        http_client: httpx.AsyncClient,
        etcd_client: etcd3.Etcd3Client,
        check_interval: int = 5,
        failure_threshold: int = 15,
        probe_timeout: float = 3.0,
    ) -> None:
        self.ring = ring
        self.http = http_client
        self.etcd = etcd_client
        self.check_interval = check_interval
        self.failure_threshold = failure_threshold
        self.probe_timeout = probe_timeout

        # In-memory cache of last-seen timestamps (avoids an etcd round-trip
        # on every probe cycle for the common "still alive" case).
        self._last_seen: dict[str, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self, stop_event: asyncio.Event) -> None:
        """
        Main detection loop — runs until *stop_event* is set.

        On each cycle, probes every registered node and updates etcd status.
        """
        logger.info(
            "Failure detector started — %d nodes, threshold=%ds, interval=%ds",
            self.ring.node_count, self.failure_threshold, self.check_interval,
        )

        # Initialize last_seen for all nodes to "now" so they start as ACTIVE.
        now = time.time()
        for node_id in self.ring.nodes:
            self._last_seen[node_id] = now
            self._set_status(node_id, "ACTIVE", self.ring.nodes[node_id].url)

        while not stop_event.is_set():
            await self._probe_all_nodes()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=float(self.check_interval))
            except asyncio.TimeoutError:
                pass  # normal path — interval elapsed, continue

        logger.info("Failure detector stopped.")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _probe_all_nodes(self) -> None:
        """Probe every registered node concurrently."""
        tasks = [
            asyncio.create_task(self._probe_node(node_id, info))
            for node_id, info in self.ring.nodes.items()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _probe_node(self, node_id: str, info) -> None:
        """Probe a single node and update its status."""
        now = time.time()
        alive = await self._health_check(info.url)

        if alive:
            self._last_seen[node_id] = now
            self._set_status(node_id, "ACTIVE", info.url)
        else:
            last = self._last_seen.get(node_id, now)
            elapsed = now - last
            if elapsed >= self.failure_threshold:
                logger.warning(
                    "Node %s FAILED — no response for %.1fs (threshold=%ds)",
                    node_id, elapsed, self.failure_threshold,
                )
                self._set_status(node_id, "FAILED", info.url)

    async def _health_check(self, url: str) -> bool:
        """Return True if the node responds to /health with 200."""
        try:
            r = await self.http.get(f"{url}/health", timeout=self.probe_timeout)
            return r.status_code == 200
        except (httpx.RequestError, httpx.TimeoutException):
            return False

    def _set_status(self, node_id: str, status: str, address: str) -> None:
        """Persist node status to etcd (fire-and-forget, best-effort)."""
        ts = datetime.now(timezone.utc).isoformat()
        try:
            self.etcd.put(_node_key(node_id, "status"), status)
            self.etcd.put(_node_key(node_id, "last_seen"), ts)
            self.etcd.put(_node_key(node_id, "address"), address)
        except Exception as exc:
            logger.warning("Failed to write node status to etcd: %s", exc)

    # ------------------------------------------------------------------
    # Status query helpers (used by repair_worker and API endpoints)
    # ------------------------------------------------------------------

    @staticmethod
    def get_node_status(etcd_client: etcd3.Etcd3Client, node_id: str) -> dict:
        """Retrieve a node's status from etcd."""
        status_raw, _ = etcd_client.get(_node_key(node_id, "status"))
        last_seen_raw, _ = etcd_client.get(_node_key(node_id, "last_seen"))
        address_raw, _ = etcd_client.get(_node_key(node_id, "address"))
        return {
            "node_id":   node_id,
            "status":    status_raw.decode() if status_raw else "UNKNOWN",
            "last_seen": last_seen_raw.decode() if last_seen_raw else None,
            "address":   address_raw.decode() if address_raw else None,
        }

    @staticmethod
    def get_all_statuses(etcd_client: etcd3.Etcd3Client) -> list[dict]:
        """Retrieve all node statuses from etcd via prefix scan."""
        results: dict[str, dict] = {}
        for value, kv in etcd_client.get_prefix(_NODE_PREFIX):
            if value is None:
                continue
            key = kv.key.decode()
            # Parse /nodes/{node_id}/{field}
            parts = key.split("/")
            if len(parts) < 4:
                continue
            node_id = parts[2]
            field = parts[3]
            if node_id not in results:
                results[node_id] = {"node_id": node_id, "status": "UNKNOWN"}
            results[node_id][field] = value.decode()
        return list(results.values())
