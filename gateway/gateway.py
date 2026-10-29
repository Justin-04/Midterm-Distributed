"""
gateway.py

S3-compatible API Gateway with quorum replication, erasure coding,
node failure detection, and automated repair.

Endpoints
---------
PUT    /{bucket}/{key}           — upload (replication or erasure-coded)
GET    /{bucket}/{key}           — download (quorum read + read repair)
DELETE /{bucket}/{key}           — delete (refcount-based GC)
GET    /{bucket}                 — list objects in a bucket
PUT    /buckets/{bucket}         — create/configure bucket (encoding mode)
GET    /health                   — liveness probe
GET    /ring/stats               — hash ring load distribution
GET    /cluster/status           — node health status from etcd
POST   /internal/repair/{hash}   — trigger chunk repair (called by scrubber)

Configuration (env vars)
------------------------
ETCD_HOST             etcd hostname                    (default: localhost)
ETCD_PORT             etcd port                        (default: 2379)
CHUNKING_MODE         "fixed" or "cdc"                 (default: fixed)
CHUNK_SIZE            fixed-mode chunk size bytes       (default: 4194304)
REPLICATION_FACTOR     N — total replicas per chunk      (default: 3)
WRITE_QUORUM          W — minimum acks for write success (default: 2)
READ_QUORUM           R — minimum valid reads required   (default: 2)
FAILURE_THRESHOLD     seconds before node marked FAILED (default: 15)
FAILURE_CHECK_INTERVAL seconds between health probes      (default: 5)
REPAIR_INTERVAL       seconds between repair cycles      (default: 30)
DEFAULT_ERASURE_K     default k for erasure buckets       (default: 2)
DEFAULT_ERASURE_M     default m for erasure buckets       (default: 2)
STORAGE_NODES         comma-separated node_id=url pairs
GATEWAY_PORT          port to listen on                  (default: 9000)
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse, Response

from chunker import make_chunker, Chunk
from ring import ConsistentHashRing, NodeInfo, load_distribution, _generate_sample_hashes
from metadata import (
    get_client as get_etcd_client,
    put_object_metadata,
    put_object_metadata_ext,
    get_object_metadata,
    delete_object_metadata,
    delete_object_metadata_ext,
    list_objects,
    put_bucket_config,
    get_bucket_config,
    close_client,
    ObjectNotFoundError,
)
from quorum import quorum_write, quorum_read, quorum_delete
from erasure import encode_chunk, decode_chunk, verify_shard, ErasureConfig
from failure_detector import FailureDetector
from repair_worker import RepairWorker

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ETCD_HOST:                str = os.getenv("ETCD_HOST", "localhost")
ETCD_PORT:                int = int(os.getenv("ETCD_PORT", "2379"))
CHUNKING_MODE:            str = os.getenv("CHUNKING_MODE", "fixed")
CHUNK_SIZE:               int = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))
REPLICATION_FACTOR:       int = int(os.getenv("REPLICATION_FACTOR", "3"))
WRITE_QUORUM:             int = int(os.getenv("WRITE_QUORUM", "2"))
READ_QUORUM:              int = int(os.getenv("READ_QUORUM", "2"))
FAILURE_THRESHOLD:        int = int(os.getenv("FAILURE_THRESHOLD", "15"))
FAILURE_CHECK_INTERVAL:   int = int(os.getenv("FAILURE_CHECK_INTERVAL", "5"))
REPAIR_INTERVAL:          int = int(os.getenv("REPAIR_INTERVAL", "30"))
DEFAULT_ERASURE_K:        int = int(os.getenv("DEFAULT_ERASURE_K", "2"))
DEFAULT_ERASURE_M:        int = int(os.getenv("DEFAULT_ERASURE_M", "2"))
GATEWAY_PORT:             int = int(os.getenv("GATEWAY_PORT", "9000"))

_STORAGE_NODES_RAW: str = os.getenv(
    "STORAGE_NODES",
    "node-1=http://storage_node1:8000,"
    "node-2=http://storage_node2:8000,"
    "node-3=http://storage_node3:8000,"
    "node-4=http://storage_node4:8000",
)

_HTTP_TIMEOUT: float = 30.0

# ---------------------------------------------------------------------------
# Global singletons (initialised in lifespan)
# ---------------------------------------------------------------------------

ring:           ConsistentHashRing
chunker:        object
http:           httpx.AsyncClient
etcd:           Any  # etcd3.Etcd3Client
_failure_detector: FailureDetector
_repair_worker:     RepairWorker
_stop_event:    asyncio.Event
_bg_tasks:      list[asyncio.Task] = []


def _build_ring() -> ConsistentHashRing:
    r = ConsistentHashRing(vnode_count=150)
    for pair in _STORAGE_NODES_RAW.split(","):
        pair = pair.strip()
        if not pair or "=" not in pair:
            continue
        node_id, url = pair.split("=", 1)
        r.add_node(node_id.strip(), url.strip())
        logger.info("Registered storage node %r → %s", node_id.strip(), url.strip())
    return r


# ---------------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global ring, chunker, http, etcd
    global _failure_detector, _repair_worker, _stop_event, _bg_tasks

    ring    = _build_ring()
    chunker = make_chunker(mode=CHUNKING_MODE, chunk_size=CHUNK_SIZE)
    http    = httpx.AsyncClient(timeout=_HTTP_TIMEOUT)
    etcd    = get_etcd_client()

    _stop_event = asyncio.Event()

    # ── Start background workers ──────────────────────────────────────────
    _failure_detector = FailureDetector(
        ring=ring,
        http_client=http,
        etcd_client=etcd,
        check_interval=FAILURE_CHECK_INTERVAL,
        failure_threshold=FAILURE_THRESHOLD,
    )
    _repair_worker = RepairWorker(
        ring=ring,
        http_client=http,
        etcd_client=etcd,
        replication_factor=REPLICATION_FACTOR,
        write_quorum=WRITE_QUORUM,
        repair_interval=REPAIR_INTERVAL,
    )

    _bg_tasks = [
        asyncio.create_task(
            _failure_detector.run(_stop_event), name="failure_detector"
        ),
        asyncio.create_task(
            _repair_worker.run(_stop_event), name="repair_worker"
        ),
    ]

    logger.info(
        "Gateway starting — mode=%s, N=%d, W=%d, R=%d, nodes=%d, "
        "failure_threshold=%ds, repair_interval=%ds",
        CHUNKING_MODE, REPLICATION_FACTOR, WRITE_QUORUM, READ_QUORUM,
        ring.node_count, FAILURE_THRESHOLD, REPAIR_INTERVAL,
    )

    yield

    # ── Graceful shutdown ────────────────────────────────────────────────
    _stop_event.set()
    for t in _bg_tasks:
        try:
            await asyncio.wait_for(t, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            t.cancel()

    await http.aclose()
    close_client()
    logger.info("Gateway stopped.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Distributed Storage Gateway",
    description="S3-compatible API with quorum replication, erasure coding, and self-healing.",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Replication-mode helpers
# ---------------------------------------------------------------------------

async def _store_chunk_replication(chunk: Chunk) -> list[str]:
    """Quorum write a chunk.  Returns list of successful node IDs."""
    success, successful_nodes, _failed = await quorum_write(
        http, ring, chunk.hash, chunk.data, REPLICATION_FACTOR, WRITE_QUORUM,
    )
    if not success:
        raise RuntimeError(
            f"Quorum write failed for chunk {chunk.hash[:12]}: "
            f"needed {WRITE_QUORUM} acks."
        )
    return successful_nodes


async def _fetch_chunk_replication(chunk_hash: str) -> bytes:
    """Quorum read a chunk with read repair."""
    data = await quorum_read(http, ring, chunk_hash, READ_QUORUM, REPLICATION_FACTOR)
    if data is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Chunk {chunk_hash[:12]}… unavailable on all replicas.",
        )
    return data


# ---------------------------------------------------------------------------
# Erasure-mode helpers
# ---------------------------------------------------------------------------

async def _store_chunk_erasure(chunk: Chunk, k: int, m: int) -> dict[str, Any]:
    """
    Erasure-encode a chunk and distribute shards across nodes.

    Returns shard metadata: {chunk_hash: {shards: [...], pad_len, chunk_size}}
    """
    shards, pad_len = encode_chunk(chunk.data, k, m)

    shard_infos: list[dict] = []
    upload_tasks = []

    for shard in shards:
        node = ring.get_node(shard.hash)

        async def _upload_shard(s=shard, n=node) -> dict:
            # Dedup: skip if already present.
            try:
                r = await http.head(f"{n.url}/chunks/{s.hash}", timeout=10)
                if r.status_code == 200:
                    logger.debug("Shard %s already on %s — skipping.", s.hash[:12], n.node_id)
                    return {"index": s.index, "hash": s.hash, "node": n.node_id}
            except httpx.RequestError:
                pass

            r = await http.put(
                f"{n.url}/chunks/{s.hash}",
                content=s.data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=30,
            )
            if r.status_code == 201:
                logger.debug("Shard %s stored on %s.", s.hash[:12], n.node_id)
                return {"index": s.index, "hash": s.hash, "node": n.node_id}
            raise RuntimeError(f"Failed to store shard {s.hash[:12]} on {n.node_id}")

        upload_tasks.append(_upload_shard())

    results = await asyncio.gather(*upload_tasks)

    return {
        chunk.hash: {
            "shards": sorted(results, key=lambda x: x["index"]),
            "pad_len": pad_len,
            "chunk_size": chunk.size,
        }
    }


async def _fetch_chunk_erasure(
    chunk_hash: str,
    shard_info: dict[str, Any],
    k: int,
    m: int,
) -> bytes:
    """
    Fetch shards and reconstruct the original chunk.

    Tries each shard's assigned node first, then falls back to other nodes
    that may have picked up the shard via repair.
    """
    shards_list = shard_info["shards"]
    pad_len = shard_info["pad_len"]
    original_size = shard_info["chunk_size"]

    collected_shards: list[bytes] = []
    collected_indices: list[int] = []

    for shard in shards_list:
        shard_hash = shard["hash"]
        shard_index = shard["index"]

        # Try the assigned node first.
        node = ring.get_node(shard_hash)
        try:
            r = await http.get(f"{node.url}/chunks/{shard_hash}", timeout=30)
            if r.status_code == 200 and verify_shard(r.content, shard_hash):
                collected_shards.append(r.content)
                collected_indices.append(shard_index)
                if len(collected_shards) >= k:
                    break
                continue
        except httpx.RequestError:
            pass

        # Fallback: try other nodes in the ring.
        for fallback_node in ring.get_nodes(shard_hash, REPLICATION_FACTOR):
            if fallback_node.node_id == node.node_id:
                continue
            try:
                r = await http.get(f"{fallback_node.url}/chunks/{shard_hash}", timeout=30)
                if r.status_code == 200 and verify_shard(r.content, shard_hash):
                    collected_shards.append(r.content)
                    collected_indices.append(shard_index)
                    if len(collected_shards) >= k:
                        break
                    break
            except httpx.RequestError:
                continue

        if len(collected_shards) >= k:
            break

    if len(collected_shards) < k:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"Erasure decode failed for chunk {chunk_hash[:12]}: "
                f"only {len(collected_shards)}/{k} valid shards recovered."
            ),
        )

    return decode_chunk(
        collected_shards, collected_indices, k, m, original_size, pad_len,
    )


# ---------------------------------------------------------------------------
# PUT /{bucket}/{key}
# ---------------------------------------------------------------------------

@app.put("/{bucket}/{key:path}", status_code=status.HTTP_201_CREATED)
async def put_object(bucket: str, key: str, request: Request) -> JSONResponse:
    body: bytes = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Empty body — provide file content.")

    content_type = request.headers.get("content-type", "application/octet-stream")

    # ── Determine encoding mode from bucket config ────────────────────────
    config = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_bucket_config(bucket),
    )

    encoding = "replication"
    erasure_k = DEFAULT_ERASURE_K
    erasure_m = DEFAULT_ERASURE_M
    n = REPLICATION_FACTOR
    w = WRITE_QUORUM

    if config:
        encoding = config.get("encoding", "replication")
        erasure_k = config.get("erasure_k", DEFAULT_ERASURE_K)
        erasure_m = config.get("erasure_m", DEFAULT_ERASURE_M)
        n = config.get("replication_factor", REPLICATION_FACTOR)
        w = config.get("write_quorum", WRITE_QUORUM)

    # ── Chunk the body ────────────────────────────────────────────────────
    chunks: list[Chunk] = list(chunker.chunks(body))
    chunk_hashes = [c.hash for c in chunks]

    logger.info(
        "PUT %s/%s — %d bytes, %d chunks, encoding=%s",
        bucket, key, len(body), len(chunks), encoding,
    )

    if encoding == "erasure":
        # ── Erasure coding path ───────────────────────────────────────────
        shard_map: dict[str, Any] = {}
        for chunk in chunks:
            sm = await _store_chunk_erasure(chunk, erasure_k, erasure_m)
            shard_map.update(sm)

        meta = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: put_object_metadata_ext(
                bucket=bucket,
                key=key,
                size=len(body),
                chunk_hashes=chunk_hashes,
                content_type=content_type,
                encoding="erasure",
                shard_map=shard_map,
                erasure_k=erasure_k,
                erasure_m=erasure_m,
            ),
        )
    else:
        # ── Replication + quorum path ─────────────────────────────────────
        store_tasks = [_store_chunk_replication(c) for c in chunks]
        try:
            await asyncio.gather(*store_tasks)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc

        meta = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: put_object_metadata(
                bucket=bucket,
                key=key,
                size=len(body),
                chunk_hashes=chunk_hashes,
                content_type=content_type,
            ),
        )

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "bucket":       bucket,
            "key":          key,
            "size":         len(body),
            "chunk_count":  len(chunks),
            "chunk_hashes":  chunk_hashes,
            "encoding":     encoding,
            "version":      meta["version"],
            "created_at":   meta["creation_date"],
        },
    )


# ---------------------------------------------------------------------------
# GET /{bucket}/{key}
# ---------------------------------------------------------------------------

@app.get("/{bucket}/{key:path}")
async def get_object(bucket: str, key: str) -> StreamingResponse:
    try:
        meta = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_object_metadata(bucket, key),
        )
    except ObjectNotFoundError:
        raise HTTPException(status_code=404, detail=f"Object {bucket}/{key} not found.")

    chunk_hashes: list[str] = meta["chunk_hashes"]
    encoding = meta.get("encoding", "replication")

    logger.info(
        "GET %s/%s — %d chunks, encoding=%s", bucket, key, len(chunk_hashes), encoding,
    )

    if encoding == "erasure":
        k = meta["erasure_k"]
        m = meta["erasure_m"]
        shard_map = meta.get("shard_map", {})

        fetch_tasks = [
            _fetch_chunk_erasure(h, shard_map[h], k, m)
            for h in chunk_hashes
        ]
    else:
        fetch_tasks = [
            _fetch_chunk_replication(h) for h in chunk_hashes
        ]

    try:
        chunk_data_list: list[bytes] = await asyncio.gather(*fetch_tasks)
    except HTTPException:
        raise

    async def _body():
        for piece in chunk_data_list:
            yield piece

    return StreamingResponse(
        _body(),
        media_type=meta.get("content_type", "application/octet-stream"),
        headers={
            "X-Object-Size":    str(meta["size"]),
            "X-Chunk-Count":    str(len(chunk_hashes)),
            "X-Object-Version": meta.get("version", "1"),
            "X-Object-Created": meta.get("creation_date", ""),
            "X-Encoding":       encoding,
        },
    )


# ---------------------------------------------------------------------------
# DELETE /{bucket}/{key}
# ---------------------------------------------------------------------------

@app.delete("/{bucket}/{key:path}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_object(bucket: str, key: str) -> Response:
    try:
        meta = get_object_metadata(bucket, key)
    except ObjectNotFoundError:
        raise HTTPException(status_code=404, detail=f"Object {bucket}/{key} not found.")

    encoding = meta.get("encoding", "replication")

    if encoding == "erasure":
        _meta, zero_hashes = await asyncio.get_event_loop().run_in_executor(
            None, lambda: delete_object_metadata_ext(bucket, key),
        )
    else:
        _meta, zero_hashes = await asyncio.get_event_loop().run_in_executor(
            None, lambda: delete_object_metadata(bucket, key),
        )

    # Physically delete zero-refcount chunks from storage nodes.
    async def _gc_chunk(chunk_hash: str) -> None:
        await quorum_delete(http, ring, chunk_hash, REPLICATION_FACTOR)

    if zero_hashes:
        await asyncio.gather(*[_gc_chunk(h) for h in zero_hashes])

    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# LIST /{bucket}
# ---------------------------------------------------------------------------

@app.get("/{bucket}")
async def list_bucket(bucket: str) -> JSONResponse:
    objects = await asyncio.get_event_loop().run_in_executor(
        None, lambda: list_objects(bucket),
    )
    return JSONResponse(
        content={"bucket": bucket, "object_count": len(objects), "objects": objects}
    )


# ---------------------------------------------------------------------------
# Bucket configuration endpoint
# ---------------------------------------------------------------------------

@app.put("/buckets/{bucket}", status_code=status.HTTP_201_CREATED)
async def configure_bucket(bucket: str, request: Request) -> JSONResponse:
    """
    Create or configure a bucket.

    Query params or JSON body:
        encoding           "replication" | "erasure"
        replication_factor  N
        write_quorum        W
        read_quorum         R
        erasure_k           k data fragments
        erasure_m           m parity fragments
    """
    # Parse params from JSON body or query string.
    params: dict[str, Any] = {}
    try:
        body = await request.json()
        if isinstance(body, dict):
            params.update(body)
    except Exception:
        pass  # body might be empty — fall back to query params

    # Merge query params (higher priority).
    for k in ("encoding", "replication_factor", "write_quorum",
             "read_quorum", "erasure_k", "erasure_m"):
        v = request.query_params.get(k)
        if v is not None:
            params[k] = v

    encoding = str(params.get("encoding", "replication"))
    if encoding not in ("replication", "erasure"):
        raise HTTPException(400, f"Unknown encoding: {encoding!r}")

    rf  = int(params.get("replication_factor", REPLICATION_FACTOR))
    w   = int(params.get("write_quorum", WRITE_QUORUM))
    r   = int(params.get("read_quorum", READ_QUORUM))
    ek  = int(params.get("erasure_k", DEFAULT_ERASURE_K))
    em  = int(params.get("erasure_m", DEFAULT_ERASURE_M))

    # Validate quorum constraint: W + R > N
    if encoding == "replication" and w + r <= rf:
        raise HTTPException(
            400,
            f"Quorum constraint violated: W({w}) + R({r}) must be > N({rf}).",
        )

    # Validate erasure: k + m <= node_count
    if encoding == "erasure" and ek + em > ring.node_count:
        raise HTTPException(
            400,
            f"Erasure k+m ({ek+em}) exceeds available nodes ({ring.node_count}).",
        )

    config = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: put_bucket_config(bucket, encoding, rf, w, r, ek, em),
    )
    return JSONResponse(status_code=201, content=config)


@app.get("/buckets/{bucket}")
async def get_bucket_info(bucket: str) -> JSONResponse:
    config = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_bucket_config(bucket),
    )
    if config is None:
        raise HTTPException(404, f"Bucket {bucket!r} has no configuration.")
    return JSONResponse(content=config)


# ---------------------------------------------------------------------------
# Health & ops endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(content={
        "status":   "ok",
        "nodes":    ring.node_count,
        "chunking": CHUNKING_MODE,
        "N":        REPLICATION_FACTOR,
        "W":        WRITE_QUORUM,
        "R":        READ_QUORUM,
    })


@app.get("/ring/stats")
async def ring_stats() -> JSONResponse:
    sample = _generate_sample_hashes(10_000)
    dist   = load_distribution(ring, sample)
    return JSONResponse(content={
        "vnode_count_per_node": ring.vnode_count,
        "physical_nodes":       ring.node_count,
        "total_vnodes":         ring.node_count * ring.vnode_count,
        "sample_size":          len(sample),
        "distribution": {
            nid: {"chunks": c, "pct": round(100*c/len(sample), 2),
                   "url": ring.nodes[nid].url}
            for nid, c in sorted(dist.items())
        },
    })


@app.get("/cluster/status")
async def cluster_status() -> JSONResponse:
    """Return health status of all storage nodes from etcd."""
    statuses = await asyncio.get_event_loop().run_in_executor(
        None, lambda: FailureDetector.get_all_statuses(etcd),
    )
    active  = sum(1 for s in statuses if s.get("status") == "ACTIVE")
    failed  = sum(1 for s in statuses if s.get("status") == "FAILED")
    return JSONResponse(content={
        "total":  len(statuses),
        "active": active,
        "failed": failed,
        "nodes":  statuses,
    })


# ---------------------------------------------------------------------------
# Internal repair endpoint (called by storage node scrubber)
# ---------------------------------------------------------------------------

@app.post("/internal/repair/{chunk_hash}")
async def repair_chunk(chunk_hash: str, request: Request) -> JSONResponse:
    """
    Repair a missing/corrupted chunk.

    Called by the bit-rot scrubber when it detects and quarantines a corrupted
    file.  The gateway:
      1. Finds all nodes that should have this chunk (via the ring).
      2. Locates a healthy copy.
      3. Re-uploads to nodes that are missing it.
    """
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    reporter_node = body.get("node_id", "unknown")
    logger.info("Repair request for chunk %s from node %s", chunk_hash[:12], reporter_node)

    replicas = ring.get_nodes(chunk_hash, REPLICATION_FACTOR)

    # Find which replicas still have the chunk.
    healthy: list[NodeInfo] = []
    missing: list[NodeInfo] = []

    for node in replicas:
        try:
            r = await http.head(f"{node.url}/chunks/{chunk_hash}", timeout=10)
            if r.status_code == 200:
                healthy.append(node)
            else:
                missing.append(node)
        except httpx.RequestError:
            missing.append(node)

    if not healthy:
        logger.error("Repair failed for %s — no healthy copy exists.", chunk_hash[:12])
        raise HTTPException(
            status_code=503,
            detail=f"No healthy copy of chunk {chunk_hash[:12]}… found on any node.",
        )

    # Fetch the good copy.
    data: bytes | None = None
    for node in healthy:
        try:
            r = await http.get(f"{node.url}/chunks/{chunk_hash}", timeout=30)
            if r.status_code == 200:
                data = r.content
                break
        except httpx.RequestError:
            continue

    if data is None:
        raise HTTPException(status_code=503, detail="Could not fetch healthy copy.")

    # Re-upload to missing nodes.
    repaired = 0
    for node in missing:
        try:
            r = await http.put(
                f"{node.url}/chunks/{chunk_hash}",
                content=data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=30,
            )
            if r.status_code == 201:
                repaired += 1
                logger.info("Repaired chunk %s on %s", chunk_hash[:12], node.node_id)
        except httpx.RequestError as exc:
            logger.warning("Repair upload to %s failed: %s", node.node_id, exc)

    return JSONResponse(content={
        "chunk_hash":  chunk_hash,
        "reporter":    reporter_node,
        "healthy_nodes": [n.node_id for n in healthy],
        "repaired_nodes": [n.node_id for n in missing][:repaired],
        "repaired":    repaired,
    })


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("gateway:app", host="0.0.0.0", port=GATEWAY_PORT,
                log_level="info", reload=False)
