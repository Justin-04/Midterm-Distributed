"""
routers/objects.py

Object operations:
    PUT    /buckets/{bucket}/{key}         — upload (streaming / multipart)
    GET    /buckets/{bucket}/{key}         — download (full or byte-range)
    DELETE /buckets/{bucket}/{key}         — delete
    GET    /buckets/{bucket}/objects       — list objects in bucket
    GET    /buckets/{bucket}/objects/{key} — detailed metadata
    PATCH  /buckets/{bucket}/objects/{key} — update tags
"""
from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import (
    APIRouter, Depends, Header, HTTPException, Request,
    Response, UploadFile, File, status,
)
from fastapi.responses import StreamingResponse

from auth import TokenPayload, get_current_user
from chunker import fixed_chunks, sha256
from config import CHUNK_SIZE, REPLICATION_FACTOR, WRITE_QUORUM, READ_QUORUM
from limiter import limiter
from store import (
    get_bucket, get_object, delete_object, list_objects_in_bucket,
    object_exists, put_object, node_head, node_put, node_get,
    node_delete, ObjectNotFound, get_all_node_statuses,
)
import store

logger = logging.getLogger(__name__)
router = APIRouter(tags=["objects"])


# ── List objects — registered BEFORE the {key:path} wildcard ─────────────────
@router.get("/buckets/{bucket}/objects", summary="List objects in bucket")
@limiter.limit("120/minute")
async def list_objects_route(
    request: Request,
    bucket: str,
    prefix: Optional[str] = None,
    limit: int = 1000,
    current: TokenPayload = Depends(get_current_user),
):
    objects = await asyncio.get_event_loop().run_in_executor(
        None, lambda: list_objects_in_bucket(bucket)
    )
    if prefix:
        objects = [o for o in objects if o["key"].startswith(prefix)]
    return {
        "bucket":       bucket,
        "object_count": len(objects[:limit]),
        "objects":      objects[:limit],
    }


# ── Object metadata — registered BEFORE the {key:path} wildcard ──────────────
@router.get("/buckets/{bucket}/objects/{key:path}", summary="Get object metadata")
@limiter.limit("120/minute")
async def get_object_metadata_route(
    request: Request,
    bucket: str,
    key: str,
    current: TokenPayload = Depends(get_current_user),
):
    try:
        meta = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_object(bucket, key)
        )
    except ObjectNotFound:
        raise HTTPException(404, f"Object {bucket}/{key} not found.")
    return meta


# ── helpers ───────────────────────────────────────────────────────────────────

def _cfg(bucket: str) -> dict:
    return get_bucket(bucket) or {
        "encoding": "replication",
        "replication_factor": REPLICATION_FACTOR,
        "write_quorum": WRITE_QUORUM,
        "read_quorum": READ_QUORUM,
    }


async def _write_chunk(chunk_hash: str, data: bytes, n: int, w: int) -> list[str]:
    """Quorum write one chunk to N nodes; return successful node ids."""
    replicas = store._ring.get_nodes(chunk_hash, n)
    acks: list[str] = []

    async def _try(node):
        if await node_head(node.url, chunk_hash):
            return node.node_id  # already present (dedup)
        ok = await node_put(node.url, chunk_hash, data)
        return node.node_id if ok else None

    tasks = [asyncio.create_task(_try(n)) for n in replicas]
    for fut in asyncio.as_completed(tasks):
        nid = await fut
        if nid:
            acks.append(nid)
        if len(acks) >= w:
            for t in tasks:
                if not t.done():
                    t.cancel()
            break
    return acks


async def _read_chunk(chunk_hash: str, n: int, r: int) -> bytes:
    """Quorum read one chunk.  Triggers async read-repair on stale replicas."""
    replicas = store._ring.get_nodes(chunk_hash, n)
    valid_data: Optional[bytes] = None
    stale = []
    valid_count = 0

    async def _try(node):
        data = await node_get(node.url, chunk_hash)
        if data is None:
            return node, None, "missing"
        if sha256(data) != chunk_hash:
            return node, data, "corrupt"
        return node, data, "ok"

    tasks = [asyncio.create_task(_try(nd)) for nd in replicas]
    for fut in asyncio.as_completed(tasks):
        node, data, st = await fut
        if st == "ok":
            valid_count += 1
            if valid_data is None:
                valid_data = data
        else:
            stale.append(node)
        if valid_count >= r:
            for t in tasks:
                if not t.done():
                    t.cancel()
            break

    # async read repair
    if valid_data and stale:
        async def _repair():
            for sn in stale:
                await node_put(sn.url, chunk_hash, valid_data)
        asyncio.create_task(_repair())

    if valid_data is None:
        raise HTTPException(503, f"Chunk {chunk_hash[:12]}… unavailable.")
    return valid_data


# ── PUT /buckets/{bucket}/{key} ───────────────────────────────────────────────
@router.put("/buckets/{bucket}/{key:path}", status_code=201)
@limiter.limit("60/minute")
async def put_object_route(
    request: Request,
    bucket: str,
    key: str,
    tags: Optional[str] = None,           # comma-separated tag=value pairs
    content_type: str = Header("application/octet-stream"),
    current: TokenPayload = Depends(get_current_user),
):
    """
    Upload an object.  Supports both raw body and multipart/form-data.

    For multipart, the file must be the first field named ``file``.
    Tags can be passed as a header  ``Tags: project=abc,env=prod``.
    """
    ct = request.headers.get("content-type", "application/octet-stream")

    if "multipart/form-data" in ct:
        form = await request.form()
        field = next(iter(form.values()), None)
        if field is None:
            raise HTTPException(400, "No file field in multipart body.")
        body: bytes = await field.read() if hasattr(field, "read") else bytes(field)
        actual_ct = field.content_type if hasattr(field, "content_type") else "application/octet-stream"
    else:
        body = await request.body()
        actual_ct = ct

    if not body:
        raise HTTPException(400, "Empty body — provide file content.")

    # Parse tags
    tag_dict: dict[str, str] = {}
    raw_tags = request.headers.get("tags", tags or "")
    for item in raw_tags.split(","):
        if "=" in item:
            k, v = item.split("=", 1)
            tag_dict[k.strip()] = v.strip()

    cfg = _cfg(bucket)
    n   = cfg.get("replication_factor", REPLICATION_FACTOR)
    w   = cfg.get("write_quorum", WRITE_QUORUM)

    chunks = list(fixed_chunks(body, CHUNK_SIZE))
    chunk_hashes = [c.hash for c in chunks]

    logger.info("PUT %s/%s — %d bytes, %d chunks", bucket, key, len(body), len(chunks))

    # Upload chunks concurrently
    upload_tasks = [_write_chunk(c.hash, c.data, n, w) for c in chunks]
    results = await asyncio.gather(*upload_tasks)
    for i, acks in enumerate(results):
        if len(acks) < w:
            raise HTTPException(
                503,
                f"Quorum write failed for chunk {i} ({chunks[i].hash[:12]}): "
                f"only {len(acks)}/{w} acks received.",
            )

    meta = {
        "bucket":        bucket,
        "key":           key,
        "size":          len(body),
        "version":       "1",
        "chunk_hashes":  chunk_hashes,
        "chunk_count":   len(chunks),
        "content_type":  actual_ct,
        "encoding":      cfg.get("encoding", "replication"),
        "creation_date": datetime.now(timezone.utc).isoformat(),
        "tags":          tag_dict,
    }

    await asyncio.get_event_loop().run_in_executor(None, lambda: put_object(meta))

    return {
        "bucket":       bucket,
        "key":          key,
        "size":         len(body),
        "chunk_count":  len(chunks),
        "encoding":     meta["encoding"],
        "version":      "1",
        "created_at":   meta["creation_date"],
        "tags":         tag_dict,
    }


# ── GET /buckets/{bucket}/{key} ───────────────────────────────────────────────
@router.get("/buckets/{bucket}/{key:path}")
@limiter.limit("120/minute")
async def get_object_route(
    request: Request,
    bucket: str,
    key: str,
    range: Optional[str] = Header(None),
    current: TokenPayload = Depends(get_current_user),
):
    """
    Download an object.  Supports HTTP Range header for partial downloads.

    Range: bytes=0-1048575   →  returns that byte range with 206 Partial Content.
    """
    try:
        meta = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_object(bucket, key)
        )
    except ObjectNotFound:
        raise HTTPException(404, f"Object {bucket}/{key} not found.")

    cfg = _cfg(bucket)
    n   = cfg.get("replication_factor", REPLICATION_FACTOR)
    r   = cfg.get("read_quorum", READ_QUORUM)

    chunk_hashes: list[str] = meta["chunk_hashes"]
    total_size: int = meta["size"]

    # ── Fetch all chunks (quorum read) ───────────────────────────────────
    chunk_data_list: list[bytes] = await asyncio.gather(
        *[_read_chunk(h, n, r) for h in chunk_hashes]
    )
    full_body = b"".join(chunk_data_list)

    # ── Byte-range handling ──────────────────────────────────────────────
    if range:
        try:
            unit, ranges_str = range.split("=", 1)
            if unit.strip().lower() != "bytes":
                raise ValueError
            first_range = ranges_str.split(",")[0].strip()
            start_s, end_s = first_range.split("-", 1)
            start = int(start_s) if start_s else 0
            end   = int(end_s)   if end_s   else total_size - 1
            end   = min(end, total_size - 1)
            if start > end or start >= total_size:
                raise HTTPException(416, "Range Not Satisfiable")
        except (ValueError, AttributeError):
            raise HTTPException(400, f"Invalid Range header: {range!r}")

        slice_data = full_body[start: end + 1]
        return Response(
            content=slice_data,
            status_code=206,
            media_type=meta.get("content_type", "application/octet-stream"),
            headers={
                "Content-Range":    f"bytes {start}-{end}/{total_size}",
                "Content-Length":   str(len(slice_data)),
                "Accept-Ranges":    "bytes",
                "X-Object-Size":    str(total_size),
                "X-Chunk-Count":    str(len(chunk_hashes)),
                "X-Object-Version": meta.get("version", "1"),
                "X-Encoding":       meta.get("encoding", "replication"),
            },
        )

    # ── Full response ────────────────────────────────────────────────────
    async def _stream():
        for piece in chunk_data_list:
            yield piece

    return StreamingResponse(
        _stream(),
        media_type=meta.get("content_type", "application/octet-stream"),
        headers={
            "Accept-Ranges":    "bytes",
            "X-Object-Size":    str(total_size),
            "X-Chunk-Count":    str(len(chunk_hashes)),
            "X-Object-Version": meta.get("version", "1"),
            "X-Object-Created": meta.get("creation_date", ""),
            "X-Encoding":       meta.get("encoding", "replication"),
        },
    )


# ── DELETE /buckets/{bucket}/{key} ────────────────────────────────────────────
@router.delete("/buckets/{bucket}/{key:path}", status_code=204)
@limiter.limit("60/minute")
async def delete_object_route(
    request: Request,
    bucket: str,
    key: str,
    current: TokenPayload = Depends(get_current_user),
):
    try:
        meta, zero_hashes = await asyncio.get_event_loop().run_in_executor(
            None, lambda: delete_object(bucket, key)
        )
    except ObjectNotFound:
        raise HTTPException(404, f"Object {bucket}/{key} not found.")

    cfg = _cfg(bucket)
    n   = cfg.get("replication_factor", REPLICATION_FACTOR)

    async def _gc(h: str):
        replicas = store._ring.get_nodes(h, n)
        await asyncio.gather(*[node_delete(nd.url, h) for nd in replicas])

    if zero_hashes:
        await asyncio.gather(*[_gc(h) for h in zero_hashes])

    return Response(status_code=204)


# ── Update tags ───────────────────────────────────────────────────────────────
@router.patch("/buckets/{bucket}/objects/{key:path}", summary="Update object tags")
@limiter.limit("60/minute")
async def patch_object_tags(
    request: Request,
    bucket: str,
    key: str,
    current: TokenPayload = Depends(get_current_user),
):
    """Merge new tags into an existing object's metadata."""
    import json as _json
    body = await request.json()
    new_tags: dict[str, str] = body.get("tags", {})

    try:
        meta = await asyncio.get_event_loop().run_in_executor(
            None, lambda: get_object(bucket, key)
        )
    except ObjectNotFound:
        raise HTTPException(404, f"Object {bucket}/{key} not found.")

    existing_tags = meta.get("tags", {})
    existing_tags.update(new_tags)
    meta["tags"] = existing_tags

    # Re-persist (no refcount change — metadata-only update)
    import store as _store
    _store.etcd().put(_store.ok(bucket, key), _json.dumps(meta))

    return {"bucket": bucket, "key": key, "tags": existing_tags}
