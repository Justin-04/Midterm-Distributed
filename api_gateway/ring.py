"""
ring.py — Consistent hash ring (self-contained copy for api_gateway).
"""
from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeInfo:
    node_id: str
    url:     str


class ConsistentHashRing:
    def __init__(self, vnode_count: int = 150) -> None:
        self.vnode_count = vnode_count
        self._ring_keys: list[int]       = []
        self._ring_map:  dict[int, NodeInfo] = {}
        self._nodes:     dict[str, NodeInfo] = {}

    def add_node(self, node_id: str, url: str) -> None:
        if node_id in self._nodes:
            self.remove_node(node_id)
        info = NodeInfo(node_id=node_id, url=url)
        self._nodes[node_id] = info
        for i in range(self.vnode_count):
            pos = self._vnode_pos(node_id, i)
            self._ring_map[pos] = info
            bisect.insort(self._ring_keys, pos)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        del self._nodes[node_id]
        for i in range(self.vnode_count):
            pos = self._vnode_pos(node_id, i)
            self._ring_map.pop(pos, None)
            idx = bisect.bisect_left(self._ring_keys, pos)
            if idx < len(self._ring_keys) and self._ring_keys[idx] == pos:
                self._ring_keys.pop(idx)

    def get_nodes(self, key: str, count: int) -> list[NodeInfo]:
        if not self._ring_keys:
            return []
        count   = min(count, len(self._nodes))
        pos     = self._key_pos(key)
        start   = bisect.bisect_left(self._ring_keys, pos)
        seen: set[str] = set()
        result: list[NodeInfo] = []
        for offset in range(len(self._ring_keys)):
            idx  = (start + offset) % len(self._ring_keys)
            info = self._ring_map[self._ring_keys[idx]]
            if info.node_id not in seen:
                seen.add(info.node_id)
                result.append(info)
                if len(result) == count:
                    break
        return result

    @property
    def nodes(self) -> dict[str, NodeInfo]:
        return dict(self._nodes)

    @staticmethod
    def _vnode_pos(node_id: str, i: int) -> int:
        raw = hashlib.md5(f"{node_id}:{i}".encode()).digest()
        return int.from_bytes(raw[:8], "big")

    @staticmethod
    def _key_pos(key: str) -> int:
        raw = hashlib.md5(key.encode()).digest()
        return int.from_bytes(raw[:8], "big")
