"""
main.py — API Gateway entry point.

Starts FastAPI with:
  - JWT authentication      (auth.py)
  - Rate limiting           (limiter.py via slowapi)
  - Bucket management       (routers/buckets.py)
  - Object CRUD + streaming (routers/objects.py)
  - Search & filtering      (routers/search.py)
  - Cluster observability   (routers/cluster.py)
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import etcd3
import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

import store
from config import (
    ETCD_HOST, ETCD_PORT, HTTP_TIMEOUT, GATEWAY_PORT,
    parse_storage_nodes,
)
from limiter import limiter
from ring import ConsistentHashRing

from auth import router as auth_router
from routers.buckets import router as buckets_router
from routers.objects import router as objects_router
from routers.cluster import router as cluster_router
from routers.search  import router as search_router
from routers.search_advanced import router as search_adv_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Build consistent hash ring
    nodes = parse_storage_nodes()
    ring  = ConsistentHashRing(vnode_count=150)
    for nid, url in nodes.items():
        ring.add_node(nid, url)
        logger.info("Registered node %s → %s", nid, url)

    etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
    http_client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)

    store.init(etcd_client, http_client, nodes)
    store._ring = ring   # expose ring for router modules

    logger.info(
        "API Gateway ready — %d nodes, etcd=%s:%d, port=%d",
        len(nodes), ETCD_HOST, ETCD_PORT, GATEWAY_PORT,
    )

    yield

    await http_client.aclose()
    etcd_client.close()
    logger.info("API Gateway stopped.")


# ── Application ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Distributed Storage — API Gateway",
    description=(
        "Production-grade S3-like gateway with JWT auth, rate limiting, "
        "byte-range downloads, advanced search, and cluster observability."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Rate limiting ─────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth_router)
app.include_router(buckets_router)
app.include_router(objects_router)
app.include_router(cluster_router)
app.include_router(search_router)
app.include_router(search_adv_router)


# ── Health / root ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health():
    return {
        "status":  "ok",
        "service": "api-gateway",
        "version": "1.0.0",
    }


@app.get("/", tags=["ops"], include_in_schema=False)
async def root():
    return {"message": "Distributed Storage API Gateway", "docs": "/docs"}


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", host="0.0.0.0", port=GATEWAY_PORT,
        log_level="info", reload=False,
    )
