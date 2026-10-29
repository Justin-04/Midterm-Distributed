"""
es_client.py — Elasticsearch client singleton + index/search helpers.

Index schema (objects):
    bucket, key, size, version, chunk_count, encoding,
    content_type, creation_date, tags (object / keyword pairs)

Environment variables:
    ES_URL          Elasticsearch base URL  (default: http://localhost:9200)
    ES_INDEX        Index name              (default: objects)
    ES_USER         Optional basic-auth user
    ES_PASSWORD     Optional basic-auth password
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

from elasticsearch import Elasticsearch, NotFoundError

logger = logging.getLogger(__name__)

ES_URL      = os.getenv("ES_URL",      "http://localhost:9200")
ES_INDEX    = os.getenv("ES_INDEX",    "objects")
ES_USER     = os.getenv("ES_USER",     "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")

_client: Optional[Elasticsearch] = None


def get_es() -> Elasticsearch:
    global _client
    if _client is None:
        kwargs: dict[str, Any] = {"hosts": [ES_URL]}
        if ES_USER and ES_PASSWORD:
            kwargs["basic_auth"] = (ES_USER, ES_PASSWORD)
        _client = Elasticsearch(**kwargs)
        _ensure_index(_client)
        logger.info("Elasticsearch client connected to %s, index=%s", ES_URL, ES_INDEX)
    return _client


def close_es() -> None:
    global _client
    if _client:
        _client.close()
        _client = None


# ── Index management ──────────────────────────────────────────────────────────
_MAPPING = {
    "mappings": {
        "properties": {
            "bucket":        {"type": "keyword"},
            "key":           {"type": "text",    "fields": {"raw": {"type": "keyword"}}},
            "size":          {"type": "long"},
            "version":       {"type": "keyword"},
            "chunk_count":   {"type": "integer"},
            "encoding":      {"type": "keyword"},
            "content_type":  {"type": "keyword"},
            "creation_date": {"type": "date"},
            "tags":          {"type": "object", "dynamic": True},
        }
    }
}


def _ensure_index(es: Elasticsearch) -> None:
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=_MAPPING)
        logger.info("Created ES index %r", ES_INDEX)


# ── Document helpers ──────────────────────────────────────────────────────────
def index_object(meta: dict) -> None:
    """Index (or re-index) an object's metadata into Elasticsearch."""
    es  = get_es()
    doc = {
        "bucket":        meta.get("bucket"),
        "key":           meta.get("key"),
        "size":          meta.get("size", 0),
        "version":       meta.get("version", "1"),
        "chunk_count":   meta.get("chunk_count", 0),
        "encoding":      meta.get("encoding", "replication"),
        "content_type":  meta.get("content_type", "application/octet-stream"),
        "creation_date": meta.get("creation_date") or datetime.now(timezone.utc).isoformat(),
        "tags":          meta.get("tags", {}),
    }
    doc_id = f"{meta['bucket']}::{meta['key']}"
    es.index(index=ES_INDEX, id=doc_id, document=doc)
    logger.debug("Indexed %s/%s into ES", meta["bucket"], meta["key"])


def delete_from_index(bucket: str, key: str) -> None:
    """Remove an object's metadata from the Elasticsearch index."""
    es     = get_es()
    doc_id = f"{bucket}::{key}"
    try:
        es.delete(index=ES_INDEX, id=doc_id)
        logger.debug("Deleted %s/%s from ES", bucket, key)
    except NotFoundError:
        pass


# ── Advanced search ───────────────────────────────────────────────────────────
def advanced_search(
    bucket:     Optional[str] = None,
    prefix:     Optional[str] = None,
    min_size:   Optional[int] = None,
    max_size:   Optional[int] = None,
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    tags:       Optional[dict] = None,
    limit:      int = 100,
    offset:     int = 0,
) -> dict:
    """
    Query Elasticsearch with full-text + range + term filters.

    Returns the same envelope as the etcd-backed search:
        { total, returned, offset, limit, results: [...] }
    """
    es = get_es()
    must: list[dict] = []

    if bucket:
        must.append({"term": {"bucket": bucket}})

    if prefix:
        must.append({"prefix": {"key.raw": prefix}})

    size_range: dict = {}
    if min_size is not None: size_range["gte"] = min_size
    if max_size is not None: size_range["lte"] = max_size
    if size_range:
        must.append({"range": {"size": size_range}})

    date_range: dict = {}
    if start_date: date_range["gte"] = start_date
    if end_date:   date_range["lte"] = end_date
    if date_range:
        must.append({"range": {"creation_date": date_range}})

    if tags:
        for k, v in tags.items():
            must.append({"term": {f"tags.{k}": v}})

    query = {"bool": {"must": must}} if must else {"match_all": {}}

    resp = es.search(
        index=ES_INDEX,
        body={
            "query": query,
            "from":  offset,
            "size":  limit,
            "sort":  [{"creation_date": {"order": "desc"}}],
        },
    )

    hits   = resp["hits"]["hits"]
    total  = resp["hits"]["total"]["value"]
    results = [
        {
            "bucket":        h["_source"].get("bucket"),
            "key":           h["_source"].get("key"),
            "size":          h["_source"].get("size"),
            "content_type":  h["_source"].get("content_type"),
            "encoding":      h["_source"].get("encoding"),
            "chunk_count":   h["_source"].get("chunk_count"),
            "creation_date": h["_source"].get("creation_date"),
            "version":       h["_source"].get("version"),
            "tags":          h["_source"].get("tags", {}),
            "_score":        h.get("_score"),
        }
        for h in hits
    ]

    return {
        "total":    total,
        "returned": len(results),
        "offset":   offset,
        "limit":    limit,
        "results":  results,
        "source":   "elasticsearch",
    }


def es_health() -> dict:
    """Return a simple health dict for the /health endpoint."""
    try:
        info = get_es().info()
        return {"status": "ok", "version": info["version"]["number"]}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
