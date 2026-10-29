"""
metadata.py

Async metadata store backed by etcd.

Responsibilities
----------------
1. Object metadata CRUD
   - Stores per-object records under the key:
       /objects/<bucket>/<key>
   - Each record is a JSON blob containing:
       bucket, key, size, version, chunk_hashes[], creation_date, content_type

2. Chunk reference counting
   - Each unique chunk hash is tracked under:
       /chunks/<sha256>/refcount   →  integer (stored as decimal string)
   - Incremented on object creation, decremented on deletion.
   - When refcount reaches 0 the chunk is eligible for physical deletion from
     storage nodes (the gateway handles the actual HTTP DELETE call).

3. Bucket listing
   - Objects are enumerated by prefix-scanning /objects/<bucket>/

etcd client
-----------
Uses `etcd3-py` (async-capable, pure-Python).  The client is lazily
initialised and shared as a module-level singleton so the FastAPI app
only opens one connection pool.

Environment variables
---------------------
ETCD_HOST  : hostname / IP of the etcd server  (default: localhost)
ETCD_PORT  : etcd client port                  (default: 2379)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import etcd3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ETCD_HOST: str = os.getenv("ETCD_HOST", "localhost")
ETCD_PORT: int = int(os.getenv("ETCD_PORT", "2379"))

# etcd key prefixes
_OBJ_PREFIX:    str = "/objects/"   # /objects/<bucket>/<key>
_CHUNK_PREFIX:  str = "/chunks/"    # /chunks/<hash>/refcount
_BUCKET_PREFIX: str = "/buckets/"  # /buckets/<bucket>/config

# ---------------------------------------------------------------------------
# etcd client singleton
# ---------------------------------------------------------------------------

_client: Optional[etcd3.Etcd3Client] = None


def get_client() -> etcd3.Etcd3Client:
    """Return (and lazily create) the shared etcd3 client."""
    global _client
    if _client is None:
        _client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
        logger.info("etcd client connected to %s:%d", ETCD_HOST, ETCD_PORT)
    return _client


def close_client() -> None:
    """Close the etcd connection (call on application shutdown)."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
        logger.info("etcd client closed.")


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def _obj_key(bucket: str, key: str) -> str:
    return f"{_OBJ_PREFIX}{bucket}/{key}"


def _chunk_refcount_key(chunk_hash: str) -> str:
    return f"{_CHUNK_PREFIX}{chunk_hash}/refcount"


def _bucket_prefix(bucket: str) -> str:
    return f"{_OBJ_PREFIX}{bucket}/"


# ---------------------------------------------------------------------------
# Object metadata
# ---------------------------------------------------------------------------

class ObjectNotFoundError(KeyError):
    """Raised when a requested object does not exist in etcd."""


class ObjectAlreadyExistsError(KeyError):
    """Raised when trying to create an object that already exists (if strict)."""


def put_object_metadata(
    bucket: str,
    key: str,
    size: int,
    chunk_hashes: list[str],
    content_type: str = "application/octet-stream",
    version: str = "1",
) -> dict[str, Any]:
    """
    Store object metadata in etcd and increment reference counts for all chunks.

    Parameters
    ----------
    bucket, key    : S3-style object address.
    size           : Total object size in bytes.
    chunk_hashes   : Ordered list of SHA-256 hex strings.
    content_type   : MIME type hint.
    version        : Object version string.

    Returns
    -------
    The stored metadata dict.
    """
    client = get_client()

    # Build the metadata record.
    meta: dict[str, Any] = {
        "bucket":        bucket,
        "key":           key,
        "size":          size,
        "version":       version,
        "chunk_hashes":  chunk_hashes,
        "chunk_count":   len(chunk_hashes),
        "content_type":  content_type,
        "creation_date": datetime.now(timezone.utc).isoformat(),
    }

    # Increment reference counts for every chunk *before* committing object
    # metadata so that a crash mid-way leaves orphaned refcounts (safe) rather
    # than dangling metadata pointing at zero-refcount chunks.
    for chunk_hash in chunk_hashes:
        _increment_refcount(client, chunk_hash)

    # Commit the object record.
    etcd_key = _obj_key(bucket, key)
    client.put(etcd_key, json.dumps(meta))
    logger.info("Stored metadata for %s/%s (%d chunks, %d bytes).",
                bucket, key, len(chunk_hashes), size)
    return meta


def get_object_metadata(bucket: str, key: str) -> dict[str, Any]:
    """
    Retrieve object metadata from etcd.

    Raises
    ------
    ObjectNotFoundError
        If no record exists for (bucket, key).
    """
    client = get_client()
    raw, _ = client.get(_obj_key(bucket, key))
    if raw is None:
        raise ObjectNotFoundError(f"Object not found: {bucket}/{key}")
    return json.loads(raw)


def delete_object_metadata(
    bucket: str,
    key: str,
) -> tuple[dict[str, Any], list[str]]:
    """
    Remove object metadata from etcd and decrement chunk reference counts.

    Returns
    -------
    (metadata, zero_refcount_hashes)
        - metadata              : the deleted object record
        - zero_refcount_hashes  : chunk hashes whose refcount hit 0 and
                                  should now be physically deleted from
                                  storage nodes by the caller.
    """
    client = get_client()
    meta = get_object_metadata(bucket, key)   # raises ObjectNotFoundError if missing

    # Determine which chunks drop to zero.
    zero_hashes: list[str] = []
    for chunk_hash in meta["chunk_hashes"]:
        new_count = _decrement_refcount(client, chunk_hash)
        if new_count <= 0:
            zero_hashes.append(chunk_hash)

    # Delete the object record.
    client.delete(_obj_key(bucket, key))
    logger.info("Deleted metadata for %s/%s; %d chunks eligible for physical deletion.",
                bucket, key, len(zero_hashes))
    return meta, zero_hashes


def list_objects(bucket: str) -> list[dict[str, Any]]:
    """
    List all objects stored in *bucket*.

    Returns a list of lightweight summary dicts (excludes chunk_hashes to
    keep payload small; callers can fetch full metadata individually).
    """
    client = get_client()
    prefix = _bucket_prefix(bucket)
    results = []

    for raw, meta_kv in client.get_prefix(prefix):
        if raw is None:
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Corrupt metadata at key %s — skipping.", meta_kv.key)
            continue
        # Return a summary (omit the potentially large chunk_hashes list).
        results.append({
            "bucket":        obj.get("bucket"),
            "key":           obj.get("key"),
            "size":          obj.get("size"),
            "version":       obj.get("version"),
            "content_type":  obj.get("content_type"),
            "creation_date": obj.get("creation_date"),
            "chunk_count":   obj.get("chunk_count"),
        })
    return results


def object_exists(bucket: str, key: str) -> bool:
    """Return True if an object record exists in etcd."""
    try:
        get_object_metadata(bucket, key)
        return True
    except ObjectNotFoundError:
        return False


# ---------------------------------------------------------------------------
# Chunk reference counting
# ---------------------------------------------------------------------------

def get_refcount(chunk_hash: str) -> int:
    """Return the current reference count for *chunk_hash* (0 if not tracked)."""
    client = get_client()
    raw, _ = client.get(_chunk_refcount_key(chunk_hash))
    if raw is None:
        return 0
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 0


def _increment_refcount(client: etcd3.Etcd3Client, chunk_hash: str) -> int:
    """
    Increment the reference count for *chunk_hash* by 1 using an
    optimistic compare-and-swap loop (etcd transactions).

    Returns the new count.
    """
    rkey = _chunk_refcount_key(chunk_hash)
    while True:
        raw, kv_meta = client.get(rkey)
        old_val = int(raw) if raw is not None else 0
        new_val = old_val + 1

        if raw is None:
            # Key does not exist yet — use a transaction to create it
            # atomically (compare version == 0 means key is absent).
            txn_success, _ = client.transaction(
                compare=[client.transactions.version(rkey) == 0],
                success=[client.transactions.put(rkey, str(new_val))],
                failure=[],
            )
        else:
            txn_success, _ = client.transaction(
                compare=[client.transactions.value(rkey) == raw],
                success=[client.transactions.put(rkey, str(new_val))],
                failure=[],
            )

        if txn_success:
            logger.debug("Chunk %s refcount: %d → %d", chunk_hash[:12], old_val, new_val)
            return new_val
        # Another writer raced us — retry.


def _decrement_refcount(client: etcd3.Etcd3Client, chunk_hash: str) -> int:
    """
    Decrement the reference count for *chunk_hash* by 1.

    Returns the new count (may be 0 — caller handles physical deletion).
    """
    rkey = _chunk_refcount_key(chunk_hash)
    while True:
        raw, _ = client.get(rkey)
        old_val = int(raw) if raw is not None else 0
        new_val = max(old_val - 1, 0)

        if raw is None:
            # Nothing to decrement.
            return 0

        txn_success, _ = client.transaction(
            compare=[client.transactions.value(rkey) == raw],
            success=[client.transactions.put(rkey, str(new_val))],
            failure=[],
        )
        if txn_success:
            logger.debug("Chunk %s refcount: %d → %d", chunk_hash[:12], old_val, new_val)
            return new_val


# ---------------------------------------------------------------------------
# Bucket configuration
# ---------------------------------------------------------------------------

def _bucket_config_key(bucket: str) -> str:
    return f"{_BUCKET_PREFIX}{bucket}/config"


def put_bucket_config(
    bucket: str,
    encoding: str = "replication",
    replication_factor: int = 3,
    write_quorum: int = 2,
    read_quorum: int = 2,
    erasure_k: int = 0,
    erasure_m: int = 0,
) -> dict[str, Any]:
    """
    Store bucket-level configuration in etcd.

    Parameters
    ----------
    encoding           : "replication" or "erasure"
    replication_factor  : N (total replicas per chunk)
    write_quorum       : W (minimum acks for write success)
    read_quorum        : R (minimum valid reads required)
    erasure_k          : data fragments (for erasure mode)
    erasure_m          : parity fragments (for erasure mode)
    """
    client = get_client()
    config: dict[str, Any] = {
        "bucket":             bucket,
        "encoding":           encoding,
        "replication_factor": replication_factor,
        "write_quorum":       write_quorum,
        "read_quorum":        read_quorum,
        "erasure_k":          erasure_k,
        "erasure_m":          erasure_m,
    }
    client.put(_bucket_config_key(bucket), json.dumps(config))
    logger.info(
        "Bucket config stored for %r — encoding=%s, N=%d, W=%d, R=%d, k=%d, m=%d",
        bucket, encoding, replication_factor, write_quorum, read_quorum, erasure_k, erasure_m,
    )
    return config


def get_bucket_config(bucket: str) -> Optional[dict[str, Any]]:
    """Retrieve bucket configuration from etcd.  Returns None if unset."""
    client = get_client()
    raw, _ = client.get(_bucket_config_key(bucket))
    if raw is None:
        return None
    return json.loads(raw)


def delete_bucket_config(bucket: str) -> None:
    """Remove bucket configuration from etcd."""
    client = get_client()
    client.delete(_bucket_config_key(bucket))


# ---------------------------------------------------------------------------
# Extended object metadata (erasure coding support)
# ---------------------------------------------------------------------------

def put_object_metadata_ext(
    bucket: str,
    key: str,
    size: int,
    chunk_hashes: list[str],
    content_type: str = "application/octet-stream",
    version: str = "1",
    encoding: str = "replication",
    shard_map: Optional[dict[str, Any]] = None,
    erasure_k: Optional[int] = None,
    erasure_m: Optional[int] = None,
) -> dict[str, Any]:
    """
    Store object metadata with optional erasure-coding fields.

    For replication mode, *encoding* = "replication" and shard_map is None.
    For erasure mode, *encoding* = "erasure" and shard_map maps each chunk
    hash to its shard info:

        {
            "<chunk_hash>": {
                "shards": [{"index": 0, "hash": "<shard_hash>"}, ...],
                "pad_len": 4,
                "chunk_size": 4194304
            }
        }
    """
    client = get_client()

    meta: dict[str, Any] = {
        "bucket":        bucket,
        "key":           key,
        "size":          size,
        "version":       version,
        "chunk_hashes":  chunk_hashes,
        "chunk_count":   len(chunk_hashes),
        "content_type":  content_type,
        "encoding":      encoding,
        "creation_date": datetime.now(timezone.utc).isoformat(),
    }

    if encoding == "erasure":
        meta["erasure_k"] = erasure_k
        meta["erasure_m"] = erasure_m
        meta["shard_map"] = shard_map

    # Increment refcounts for all chunk hashes (and shards for erasure mode).
    for chunk_hash in chunk_hashes:
        _increment_refcount(client, chunk_hash)

    if encoding == "erasure" and shard_map:
        for chunk_hash, info in shard_map.items():
            for shard in info.get("shards", []):
                _increment_refcount(client, shard["hash"])

    client.put(_obj_key(bucket, key), json.dumps(meta))
    logger.info(
        "Stored metadata for %s/%s (%d chunks, %d bytes, encoding=%s).",
        bucket, key, len(chunk_hashes), size, encoding,
    )
    return meta


def delete_object_metadata_ext(
    bucket: str,
    key: str,
) -> tuple[dict[str, Any], list[str]]:
    """
    Delete object metadata and decrement refcounts for chunks AND shards.

    Returns (metadata, zero_refcount_hashes) where zero_refcount_hashes
    includes both chunk hashes and shard hashes that dropped to 0.
    """
    client = get_client()
    meta = get_object_metadata(bucket, key)

    zero_hashes: list[str] = []

    # Decrement chunk refcounts.
    for chunk_hash in meta.get("chunk_hashes", []):
        new_count = _decrement_refcount(client, chunk_hash)
        if new_count <= 0:
            zero_hashes.append(chunk_hash)

    # For erasure-coded objects, decrement shard refcounts too.
    if meta.get("encoding") == "erasure" and meta.get("shard_map"):
        for chunk_hash, info in meta["shard_map"].items():
            for shard in info.get("shards", []):
                shard_hash = shard["hash"]
                new_count = _decrement_refcount(client, shard_hash)
                if new_count <= 0:
                    zero_hashes.append(shard_hash)

    client.delete(_obj_key(bucket, key))
    logger.info(
        "Deleted metadata for %s/%s; %d hashes eligible for physical deletion.",
        bucket, key, len(zero_hashes),
    )
    return meta, zero_hashes
