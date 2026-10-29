"""
search.py

Advanced object search across buckets.

Supports filtering by:
  - key prefix
  - size range  (min_size / max_size in bytes)
  - date range  (start_date / end_date as ISO-8601 strings)
  - user-defined tags (passed as extra query params)

All filtering is done in-process after a prefix scan of etcd.
For very large deployments this should be backed by a secondary index;
for a midterm-scale cluster an etcd scan is sufficient.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from metadata import get_client

logger = logging.getLogger(__name__)


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def search_objects(
    prefix:     Optional[str]   = None,
    bucket:     Optional[str]   = None,
    min_size:   Optional[int]   = None,
    max_size:   Optional[int]   = None,
    start_date: Optional[str]   = None,
    end_date:   Optional[str]   = None,
    tags:       Optional[dict]  = None,
    limit:      int             = 1000,
    offset:     int             = 0,
) -> dict[str, Any]:
    """
    Search objects stored in etcd.

    Parameters
    ----------
    prefix     : key prefix filter (e.g. "reports/2024")
    bucket     : restrict to a single bucket
    min_size   : minimum object size in bytes
    max_size   : maximum object size in bytes
    start_date : ISO-8601 creation date lower bound
    end_date   : ISO-8601 creation date upper bound
    tags       : dict of tag key→value to match (exact)
    limit      : max results to return
    offset     : pagination offset

    Returns
    -------
    { total, returned, offset, results: [...] }
    """
    import json

    client = get_client()
    dt_start = _parse_dt(start_date)
    dt_end   = _parse_dt(end_date)

    # Choose etcd prefix to scan.
    scan_prefix = "/objects/"
    if bucket:
        scan_prefix = f"/objects/{bucket}/"

    matches: list[dict[str, Any]] = []

    for raw, kv_meta in client.get_prefix(scan_prefix):
        if raw is None:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        # ── Key prefix filter ─────────────────────────────────────────
        obj_key = obj.get("key", "")
        if prefix and not obj_key.startswith(prefix):
            continue

        # ── Size filter ───────────────────────────────────────────────
        obj_size = obj.get("size", 0)
        if min_size is not None and obj_size < min_size:
            continue
        if max_size is not None and obj_size > max_size:
            continue

        # ── Date filter ───────────────────────────────────────────────
        if dt_start or dt_end:
            raw_date = obj.get("creation_date")
            if raw_date:
                try:
                    obj_dt = datetime.fromisoformat(raw_date)
                    if obj_dt.tzinfo is None:
                        obj_dt = obj_dt.replace(tzinfo=timezone.utc)
                    if dt_start and obj_dt < dt_start:
                        continue
                    if dt_end and obj_dt > dt_end:
                        continue
                except ValueError:
                    continue

        # ── Tag filter ────────────────────────────────────────────────
        if tags:
            obj_tags = obj.get("tags", {})
            if not all(obj_tags.get(k) == v for k, v in tags.items()):
                continue

        matches.append({
            "bucket":        obj.get("bucket"),
            "key":           obj_key,
            "size":          obj_size,
            "content_type":  obj.get("content_type"),
            "encoding":      obj.get("encoding", "replication"),
            "chunk_count":   obj.get("chunk_count", 0),
            "creation_date": obj.get("creation_date"),
            "version":       obj.get("version"),
            "tags":          obj.get("tags", {}),
        })

    total = len(matches)
    page  = matches[offset: offset + limit]

    return {
        "total":    total,
        "returned": len(page),
        "offset":   offset,
        "limit":    limit,
        "results":  page,
    }
