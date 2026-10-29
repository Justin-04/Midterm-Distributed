"""
ring.py

Consistent Hashing Ring with Virtual Nodes (vnodes).

Design
------
- Each physical node is mapped to `vnode_count` points on a 2^64 hash ring.
- Chunk hashes are mapped to their responsible node by finding the first
  vnode clockwise from the chunk's position on the ring (successor lookup).
- Virtual nodes smooth the load distribution so that adding or removing
  one physical node affects only ~1/N of the key space (where N is the
  number of nodes), rather than causing a full redistribution.

Key Operations
--------------
- add_node(node_id, url)    — add a physical node (creates vnodes)
- remove_node(node_id)      — remove a physical node (removes all its vnodes)
- get_node(chunk_hash)      — map a chunk hash → (node_id, url)
- get_nodes(chunk_hash, n)  — map a chunk hash → n distinct nodes (replication)
- migration_stats(...)      — calculate % of keys that migrate after topology change
"""

from __future__ import annotations

import hashlib
import bisect
import logging
from dataclasses import dataclass
from typing import Iterator

logger = logging.getLogger(__name__)

# Default number of virtual nodes per physical node.
DEFAULT_VNODE_COUNT: int = 150


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class NodeInfo:
    """Metadata for a physical storage node."""
    node_id: str   # logical name, e.g. "node-1"
    url:     str   # base URL, e.g. "http://storage_node1:8000"


# ---------------------------------------------------------------------------
# Consistent Hash Ring
# ---------------------------------------------------------------------------

class ConsistentHashRing:
    """
    Thread-safe*-ish consistent hash ring with virtual nodes.

    (*Python GIL provides implicit safety for single-threaded async usage.
     Use an asyncio.Lock around mutations if you mutate from multiple tasks.)

    Parameters
    ----------
    vnode_count:
        Number of virtual nodes to create per physical node.
        Higher values → better load balance, more memory.
    """

    def __init__(self, vnode_count: int = DEFAULT_VNODE_COUNT) -> None:
        self.vnode_count = vnode_count

        # Sorted list of ring positions (uint64 ints).
        self._ring_keys: list[int] = []

        # Map: ring_position → NodeInfo
        self._ring_map: dict[int, NodeInfo] = {}

        # Map: node_id → NodeInfo  (for O(1) existence checks)
        self._nodes: dict[str, NodeInfo] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_node(self, node_id: str, url: str) -> None:
        """
        Register a new physical node and create its virtual nodes on the ring.

        Re-adding an existing node_id updates its URL and re-hashes its vnodes.
        """
        if node_id in self._nodes:
            # Remove old vnodes first to handle URL-change case cleanly.
            self.remove_node(node_id)

        info = NodeInfo(node_id=node_id, url=url)
        self._nodes[node_id] = info

        for i in range(self.vnode_count):
            pos = self._hash_vnode(node_id, i)
            self._ring_map[pos] = info
            bisect.insort(self._ring_keys, pos)

        logger.debug("Added node %r (%s) with %d vnodes.", node_id, url, self.vnode_count)

    def remove_node(self, node_id: str) -> None:
        """
        Unregister a physical node and remove all its virtual nodes.

        Raises
        ------
        KeyError
            If *node_id* is not currently in the ring.
        """
        if node_id not in self._nodes:
            raise KeyError(f"Node {node_id!r} is not in the ring.")

        del self._nodes[node_id]

        for i in range(self.vnode_count):
            pos = self._hash_vnode(node_id, i)
            if pos in self._ring_map:
                del self._ring_map[pos]
                idx = bisect.bisect_left(self._ring_keys, pos)
                if idx < len(self._ring_keys) and self._ring_keys[idx] == pos:
                    self._ring_keys.pop(idx)

        logger.debug("Removed node %r from the ring.", node_id)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get_node(self, chunk_hash: str) -> NodeInfo:
        """
        Return the :class:`NodeInfo` responsible for *chunk_hash*.

        Raises
        ------
        RuntimeError
            If the ring is empty.
        """
        if not self._ring_keys:
            raise RuntimeError("The hash ring is empty — add at least one node.")

        pos = self._hash_key(chunk_hash)
        idx = bisect.bisect_left(self._ring_keys, pos)

        # Wrap around (ring topology).
        if idx >= len(self._ring_keys):
            idx = 0

        return self._ring_map[self._ring_keys[idx]]

    def get_nodes(self, chunk_hash: str, count: int) -> list[NodeInfo]:
        """
        Return up to *count* distinct :class:`NodeInfo` objects for replication.

        Walks clockwise from the responsible vnode, skipping duplicate
        physical nodes until *count* distinct nodes are collected or the ring
        is exhausted.

        Parameters
        ----------
        chunk_hash: SHA-256 hex string identifying the chunk.
        count:      Desired replication factor.
        """
        if not self._ring_keys:
            raise RuntimeError("The hash ring is empty.")

        count = min(count, len(self._nodes))
        pos = self._hash_key(chunk_hash)
        start_idx = bisect.bisect_left(self._ring_keys, pos)

        seen_ids: set[str] = set()
        result: list[NodeInfo] = []
        total = len(self._ring_keys)

        for offset in range(total):
            idx = (start_idx + offset) % total
            info = self._ring_map[self._ring_keys[idx]]
            if info.node_id not in seen_ids:
                seen_ids.add(info.node_id)
                result.append(info)
                if len(result) == count:
                    break

        return result

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    @property
    def node_count(self) -> int:
        """Number of physical nodes currently in the ring."""
        return len(self._nodes)

    @property
    def nodes(self) -> dict[str, NodeInfo]:
        """Read-only view of registered physical nodes."""
        return dict(self._nodes)

    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def __repr__(self) -> str:
        return (
            f"ConsistentHashRing(nodes={list(self._nodes)}, "
            f"vnodes_per_node={self.vnode_count})"
        )

    # ------------------------------------------------------------------
    # Hash helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _hash_vnode(node_id: str, index: int) -> int:
        """Map a (node_id, vnode_index) pair to a uint64 ring position."""
        raw = hashlib.md5(f"{node_id}:{index}".encode()).digest()
        # Use the first 8 bytes as an unsigned 64-bit integer.
        return int.from_bytes(raw[:8], "big")

    @staticmethod
    def _hash_key(chunk_hash: str) -> int:
        """Map a chunk SHA-256 hex string to a uint64 ring position."""
        # Re-hash the already-hex string for uniform ring distribution.
        raw = hashlib.md5(chunk_hash.encode()).digest()
        return int.from_bytes(raw[:8], "big")


# ---------------------------------------------------------------------------
# Migration analysis utilities
# ---------------------------------------------------------------------------

def simulate_migration(
    old_ring: ConsistentHashRing,
    new_ring: ConsistentHashRing,
    sample_hashes: list[str],
) -> dict:
    """
    Compare key assignments between *old_ring* and *new_ring* for a sample
    of chunk hashes.

    Returns a dict with:
      - total          : number of sample hashes evaluated
      - migrated       : number whose responsible node changed
      - pct_migrated   : percentage that migrated (float, 0–100)
      - stayed         : number whose responsible node stayed the same
      - pct_stayed     : percentage that stayed (float, 0–100)
    """
    migrated = 0
    for h in sample_hashes:
        old_node = old_ring.get_node(h).node_id
        new_node = new_ring.get_node(h).node_id
        if old_node != new_node:
            migrated += 1

    total = len(sample_hashes)
    stayed = total - migrated
    return {
        "total":        total,
        "migrated":     migrated,
        "pct_migrated": round(100 * migrated / total, 2) if total else 0.0,
        "stayed":       stayed,
        "pct_stayed":   round(100 * stayed  / total, 2) if total else 0.0,
    }


def load_distribution(
    ring: ConsistentHashRing,
    sample_hashes: list[str],
) -> dict[str, int]:
    """
    Count how many of *sample_hashes* each physical node is responsible for.

    Returns a dict mapping node_id → count.
    """
    counts: dict[str, int] = {nid: 0 for nid in ring.nodes}
    for h in sample_hashes:
        node = ring.get_node(h)
        counts[node.node_id] = counts.get(node.node_id, 0) + 1
    return counts


def _generate_sample_hashes(n: int) -> list[str]:
    """Generate *n* deterministic synthetic SHA-256 hex strings for testing."""
    return [
        hashlib.sha256(f"synthetic-key-{i}".encode()).hexdigest()
        for i in range(n)
    ]
