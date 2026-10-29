"""
routers/search_advanced.py

Advanced Elasticsearch-backed search.

GET  /search/advanced  — full ES query (prefix, size, date, tags)
GET  /search/health    — Elasticsearch liveness check
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Optional

from fastapi import APIRouter, Depends, Request
from auth import TokenPayload, get_current_user
from limiter import limiter

logger = logging.getLogger(__name__)
router = APIRouter(tags=["search"])

# Resolve path to elasticsearch_service/ sibling folder
_ES_PATH = os.path.join(
    os.path.dirname(__file__),          # api_gateway/routers/
    "..", "..",                          # project root
    "elasticsearch_service",
)
if _ES_PATH not in sys.path:
    sys.path.insert(0, os.path.abspath(_ES_PATH))

# Lazy import — works even when ES is not installed
try:
    from es_client import advanced_search, es_health
    _ES_AVAILABLE = True
except ImportError:
    _ES_AVAILABLE = False
    logger.warning("elasticsearch package not installed — /search/advanced will fall back to etcd search")


@router.get("/search/advanced", summary="Elasticsearch advanced search")
@limiter.limit("60/minute")
async def search_advanced_route(
    request:    Request,
    bucket:     Optional[str] = None,
    prefix:     Optional[str] = None,
    min_size:   Optional[int] = None,
    max_size:   Optional[int] = None,
    start_date: Optional[str] = None,
    end_date:   Optional[str] = None,
    limit:      int = 100,
    offset:     int = 0,
    current:    TokenPayload = Depends(get_current_user),
):
    """
    Search objects via Elasticsearch.

    Filters: bucket, prefix, min_size, max_size, start_date, end_date, + any tag= params.
    Falls back to etcd-backed search if Elasticsearch is unavailable.
    """
    # Extra query params treated as tag filters
    known = {"bucket","prefix","min_size","max_size","start_date","end_date","limit","offset"}
    tags  = {k: v for k, v in request.query_params.items() if k not in known}

    if _ES_AVAILABLE:
        def _run():
            return advanced_search(
                bucket=bucket, prefix=prefix,
                min_size=min_size, max_size=max_size,
                start_date=start_date, end_date=end_date,
                tags=tags or None,
                limit=limit, offset=offset,
            )
        return await asyncio.get_event_loop().run_in_executor(None, _run)

    # Fallback to etcd search
    from routers.search import _search_objects
    result = await asyncio.get_event_loop().run_in_executor(
        None,
        lambda: _search_objects(
            bucket=bucket, prefix=prefix,
            min_size=min_size, max_size=max_size,
            start_date=start_date, end_date=end_date,
            tags=tags or None,
            limit=limit, offset=offset,
        ),
    )
    result["source"] = "etcd (elasticsearch unavailable)"
    return result


@router.get("/search/health", summary="Elasticsearch health")
@limiter.limit("30/minute")
async def search_health_route(
    request: Request,
    current: TokenPayload = Depends(get_current_user),
):
    if not _ES_AVAILABLE:
        return {"status": "not_installed", "detail": "elasticsearch package missing"}

    def _run():
        return es_health()

    return await asyncio.get_event_loop().run_in_executor(None, _run)
