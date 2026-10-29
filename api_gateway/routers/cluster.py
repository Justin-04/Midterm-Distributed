"""
routers/cluster.py

Cluster observability:
    GET  /cluster/status    — node health, disk space, chunk counts, degraded objects
    GET  /cluster/nodes     — per-node details
    GET  /ring/stats        — hash ring distribution
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, Request
from auth import TokenPayload, get_current_user
from limiter import limiter
import store

logger = logging.getLogger(__name__)
router = APIRouter(tags=["cluster"])


@router.get("/cluster/status", summary="Cluster health dashboard")
@limiter.limit("30/minute")
async def cluster_status(
    request: Request,
    current: TokenPayload = Depends(get_current_user),
):
    """
    Aggregate telemetry from etcd and live storage-node stats.

    Returns:
    - Per-node status, free space, chunk count
    - Cluster-wide totals
    - Degraded / under-replicated object list
    """
    # ── Node statuses from etcd ───────────────────────────────────────────
    node_statuses = await asyncio.get_event_loop().run_in_executor(
        None, store.get_all_node_statuses
    )
    status_map = {s["node_id"]: s for s in node_statuses}

    # ── Live stats from each node ─────────────────────────────────────────
    all_nodes = store._nodes
    live_tasks = {nid: asyncio.create_task(store.node_stats(url))
                  for nid, url in all_nodes.items()}
    live_results: dict[str, Any] = {}
    for nid, task in live_tasks.items():
        try:
            live_results[nid] = await task
        except Exception:
            live_results[nid] = None

    # ── Build per-node report ─────────────────────────────────────────────
    node_report = []
    total_free  = 0
    total_used  = 0
    total_chunks = 0
    active_count = 0
    failed_count = 0

    for nid, url in all_nodes.items():
        etcd_status = status_map.get(nid, {}).get("status", "UNKNOWN")
        live        = live_results.get(nid) or {}
        free_bytes  = live.get("free_space_bytes", 0)
        chunk_count = live.get("chunk_count", 0)
        free_gb     = live.get("free_space_gb", 0.0)

        if etcd_status == "ACTIVE":
            active_count += 1
        elif etcd_status == "FAILED":
            failed_count += 1

        total_free   += free_bytes
        total_chunks += chunk_count

        node_report.append({
            "node_id":          nid,
            "url":              url,
            "status":           etcd_status,
            "last_seen":        status_map.get(nid, {}).get("last_seen"),
            "free_space_bytes": free_bytes,
            "free_space_gb":    free_gb,
            "chunk_count":      chunk_count,
        })

    # ── Degraded object detection ─────────────────────────────────────────
    failed_node_ids = {
        nid for nid, s in status_map.items() if s.get("status") == "FAILED"
    }
    degraded = []
    if failed_node_ids:
        degraded = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _find_degraded(failed_node_ids)
        )

    return {
        "summary": {
            "total_nodes":    len(all_nodes),
            "active_nodes":   active_count,
            "failed_nodes":   failed_count,
            "total_chunks":   total_chunks,
            "total_free_gb":  round(total_free / (1024 ** 3), 2),
        },
        "nodes":   node_report,
        "degraded_objects": {
            "count":   len(degraded),
            "objects": degraded[:50],   # cap at 50 for readability
        },
    }


@router.get("/cluster/nodes", summary="Per-node status details")
@limiter.limit("60/minute")
async def cluster_nodes(
    request: Request,
    current: TokenPayload = Depends(get_current_user),
):
    statuses = await asyncio.get_event_loop().run_in_executor(
        None, store.get_all_node_statuses,
    )
    return {"nodes": statuses}


@router.get("/ring/stats", summary="Hash ring load distribution")
@limiter.limit("30/minute")
async def ring_stats(
    request: Request,
    current: TokenPayload = Depends(get_current_user),
):
    import hashlib
    ring  = store._ring
    sample_hashes = [
        hashlib.sha256(f"key-{i}".encode()).hexdigest()
        for i in range(10_000)
    ]
    counts: dict[str, int] = {nid: 0 for nid in ring.nodes}
    for h in sample_hashes:
        nodes = ring.get_nodes(h, 1)
        if nodes:
            counts[nodes[0].node_id] = counts.get(nodes[0].node_id, 0) + 1

    return {
        "vnode_count_per_node": ring.vnode_count,
        "physical_nodes":       len(ring.nodes),
        "sample_size":          len(sample_hashes),
        "distribution": {
            nid: {"chunks": c, "pct": round(100 * c / len(sample_hashes), 2)}
            for nid, c in sorted(counts.items())
        },
    }


def _find_degraded(failed_node_ids: set[str]) -> list[dict]:
    """Scan etcd and find objects whose chunks were on failed nodes."""
    ring     = store._ring
    n        = 3   # assume default replication factor for quick check
    degraded = []

    for raw, kv in store.etcd().get_prefix("/objects/"):
        if raw is None:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        affected_chunks = []
        for h in obj.get("chunk_hashes", []):
            replicas    = ring.get_nodes(h, n)
            replica_ids = {r.node_id for r in replicas}
            if replica_ids & failed_node_ids:
                affected_chunks.append(h[:12] + "…")

        if affected_chunks:
            degraded.append({
                "bucket":          obj.get("bucket"),
                "key":             obj.get("key"),
                "affected_chunks": affected_chunks,
            })
    return degraded
