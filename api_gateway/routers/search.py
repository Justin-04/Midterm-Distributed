"""
routers/search.py

Advanced cross-bucket search.

GET /search  — filter objects by prefix, size range, date range, tags.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Request
from auth import TokenPayload, get_current_user
from limiter import limiter
import store

logger = logging.getLogger(__name__)
router = APIRouter(tags=["search"])


@router.get("/search", summary="Search objects across all buckets")
@limiter.limit("60/minute")
async def search_objects(
    request: Request,
    # Scope
    bucket:     Optional[str] = None,
    prefix:     Optional[str] = None,
    # Size
    min_size:   Optional[int] = None,
    max_size:   Optional[int] = None,
    # Date
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    # Pagination
    limit:      int = 100,
    offset:     int = 0,
    # Auth
    current:    TokenPayload = Depends(get_current_user),
):
    """
    Search objects across all buckets (or within a specific one).

    **Filter parameters:**
    - `bucket`     — restrict to a single bucket
    - `prefix`     — key must start with this string
    - `min_size`   — minimum object size in bytes
    - `max_size`   — maximum object size in bytes
    - `start_date` — ISO-8601 creation date lower bound
    - `end_date`   — ISO-8601 creation date upper bound
    - Any extra query param of the form `tag_key=value` is treated as a tag filter.

    **Pagination:** use `offset` and `limit`.
    """
    # Extract tag filters from extra query params (anything not a known param)
    known = {"bucket", "prefix", "min_size", "max_size",
             "start_date", "end_date", "limit", "offset"}
    tags: dict[str, str] = {
        k: v for k, v in request.query_params.items()
        if k not in known
    }

    def _run():
        return _search_objects(
            bucket=bucket,
            prefix=prefix,
            min_size=min_size,
            max_size=max_size,
            start_date=start_date,
            end_date=end_date,
            tags=tags if tags else None,
            limit=limit,
            offset=offset,
        )

    return await asyncio.get_event_loop().run_in_executor(None, _run)


# ── Implementation ─────────────────────────────────────────────────────────────

def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if s is None:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except ValueError:
        return None


def _search_objects(
    bucket:     Optional[str],
    prefix:     Optional[str],
    min_size:   Optional[int],
    max_size:   Optional[int],
    start_date: Optional[str],
    end_date:   Optional[str],
    tags:       Optional[dict],
    limit:      int,
    offset:     int,
) -> dict:
    dt_start = _parse_dt(start_date)
    dt_end   = _parse_dt(end_date)
    scan_pfx = f"/objects/{bucket}/" if bucket else "/objects/"
    matches  = []

    for raw, kv in store.etcd().get_prefix(scan_pfx):
        if raw is None:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        obj_key  = obj.get("key", "")
        obj_size = obj.get("size", 0)

        if prefix and not obj_key.startswith(prefix):
            continue
        if min_size is not None and obj_size < min_size:
            continue
        if max_size is not None and obj_size > max_size:
            continue

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
