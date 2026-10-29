"""
store.py — Thin async wrappers around etcd and storage nodes.

All HTTP calls go through a shared httpx.AsyncClient (created in main.py
lifespan and injected via module-level singletons).

etcd schema
-----------
/buckets/{bucket}/config          → JSON bucket config
/objects/{bucket}/{key}           → JSON object metadata
/chunks/{hash}/refcount           → decimal string
/nodes/{node_id}/status           → "ACTIVE" | "FAILED"
/nodes/{node_id}/last_seen        → ISO-8601
/nodes/{node_id}/free_space_bytes → decimal string
/nodes/{node_id}/chunk_count      → decimal string
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import etcd3
import httpx

logger = logging.getLogger(__name__)

# Module-level singletons set by main.py lifespan
_etcd:   Optional[etcd3.Etcd3Client]  = None
_http:   Optional[httpx.AsyncClient]  = None
_nodes:  dict[str, str]               = {}   # node_id → url


def init(etcd_client, http_client, nodes: dict[str, str]) -> None:
    global _etcd, _http, _nodes
    _etcd  = etcd_client
    _http  = http_client
    _nodes = nodes


def etcd() -> etcd3.Etcd3Client:
    assert _etcd is not None, "store not initialised"
    return _etcd


def http() -> httpx.AsyncClient:
    assert _http is not None, "store not initialised"
    return _http


# ── etcd key helpers ──────────────────────────────────────────────────────────
def bk(bucket: str)           -> str: return f"/buckets/{bucket}/config"
def ok(bucket: str, key: str) -> str: return f"/objects/{bucket}/{key}"
def ck(h: str)                -> str: return f"/chunks/{h}/refcount"
def nk(nid: str, f: str)      -> str: return f"/nodes/{nid}/{f}"


# ── Bucket helpers ─────────────────────────────────────────────────────────────
def put_bucket(bucket: str, config: dict) -> None:
    etcd().put(bk(bucket), json.dumps(config))


def get_bucket(bucket: str) -> Optional[dict]:
    raw, _ = etcd().get(bk(bucket))
    return json.loads(raw) if raw else None


def delete_bucket_config(bucket: str) -> None:
    etcd().delete(bk(bucket))


def list_buckets() -> list[dict]:
    results = []
    for raw, kv in etcd().get_prefix("/buckets/"):
        if raw and kv.key.decode().endswith("/config"):
            try:
                results.append(json.loads(raw))
            except Exception:
                pass
    return results


# ── Object metadata ───────────────────────────────────────────────────────────
class ObjectNotFound(KeyError):
    pass


def put_object(meta: dict) -> None:
    bucket, key = meta["bucket"], meta["key"]
    for h in meta.get("chunk_hashes", []):
        _inc_ref(h)
    etcd().put(ok(bucket, key), json.dumps(meta))


def get_object(bucket: str, key: str) -> dict:
    raw, _ = etcd().get(ok(bucket, key))
    if raw is None:
        raise ObjectNotFound(f"{bucket}/{key}")
    return json.loads(raw)


def delete_object(bucket: str, key: str) -> tuple[dict, list[str]]:
    meta = get_object(bucket, key)
    zero: list[str] = []
    for h in meta.get("chunk_hashes", []):
        if _dec_ref(h) <= 0:
            zero.append(h)
    etcd().delete(ok(bucket, key))
    return meta, zero


def list_objects_in_bucket(bucket: str) -> list[dict]:
    prefix = f"/objects/{bucket}/"
    results = []
    for raw, _ in etcd().get_prefix(prefix):
        if raw:
            try:
                obj = json.loads(raw)
                results.append({
                    "key":          obj.get("key"),
                    "size":         obj.get("size"),
                    "content_type": obj.get("content_type"),
                    "encoding":     obj.get("encoding", "replication"),
                    "chunk_count":  obj.get("chunk_count", 0),
                    "creation_date":obj.get("creation_date"),
                    "version":      obj.get("version"),
                    "tags":         obj.get("tags", {}),
                })
            except Exception:
                pass
    return results


def object_exists(bucket: str, key: str) -> bool:
    raw, _ = etcd().get(ok(bucket, key))
    return raw is not None


# ── Reference counting ────────────────────────────────────────────────────────
def _cas_ref(h: str, delta: int) -> int:
    key = ck(h)
    while True:
        raw, _ = etcd().get(key)
        old = int(raw) if raw else 0
        new = max(old + delta, 0)
        if raw is None:
            ok_, _ = etcd().transaction(
                compare=[etcd().transactions.version(key) == 0],
                success=[etcd().transactions.put(key, str(new))],
                failure=[],
            )
        else:
            ok_, _ = etcd().transaction(
                compare=[etcd().transactions.value(key) == raw],
                success=[etcd().transactions.put(key, str(new))],
                failure=[],
            )
        if ok_:
            return new


def _inc_ref(h: str) -> int: return _cas_ref(h, +1)
def _dec_ref(h: str) -> int: return _cas_ref(h, -1)
def get_ref(h: str)  -> int:
    raw, _ = etcd().get(ck(h))
    return int(raw) if raw else 0


# ── Storage-node HTTP helpers ─────────────────────────────────────────────────
async def node_head(node_url: str, h: str) -> bool:
    try:
        r = await http().head(f"{node_url}/chunks/{h}", timeout=8)
        return r.status_code == 200
    except httpx.RequestError:
        return False


async def node_put(node_url: str, h: str, data: bytes) -> bool:
    try:
        r = await http().put(
            f"{node_url}/chunks/{h}", content=data,
            headers={"Content-Type": "application/octet-stream"}, timeout=60,
        )
        return r.status_code == 201
    except httpx.RequestError:
        return False


async def node_get(node_url: str, h: str) -> Optional[bytes]:
    try:
        r = await http().get(f"{node_url}/chunks/{h}", timeout=60)
        return r.content if r.status_code == 200 else None
    except httpx.RequestError:
        return None


async def node_delete(node_url: str, h: str) -> bool:
    try:
        r = await http().delete(f"{node_url}/chunks/{h}", timeout=15)
        return r.status_code in (204, 404)
    except httpx.RequestError:
        return False


async def node_stats(node_url: str) -> Optional[dict]:
    try:
        r = await http().get(f"{node_url}/stats", timeout=5)
        return r.json() if r.status_code == 200 else None
    except httpx.RequestError:
        return None


# ── Node-status helpers ───────────────────────────────────────────────────────
def get_all_node_statuses() -> list[dict]:
    results: dict[str, dict] = {}
    for raw, kv in etcd().get_prefix("/nodes/"):
        if raw is None:
            continue
        parts = kv.key.decode().split("/")
        if len(parts) < 4:
            continue
        nid, field = parts[2], parts[3]
        results.setdefault(nid, {"node_id": nid})
        results[nid][field] = raw.decode()
    return list(results.values())


# ── SHA-256 ───────────────────────────────────────────────────────────────────
def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
