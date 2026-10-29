"""
main.py

FastAPI application entry-point for the Storage Node Service.

Endpoints
---------
PUT    /chunks/{hash}   — store a raw binary chunk
GET    /chunks/{hash}   — retrieve a raw binary chunk
DELETE /chunks/{hash}   — remove a chunk
HEAD   /chunks/{hash}   — probe existence of a chunk

Startup
-------
A background asyncio task is launched that sends periodic heartbeats to the
Metadata Service (see heartbeat.py).

Configuration (environment variables)
--------------------------------------
STORAGE_DIR           Directory where chunks are persisted  (default: /data/chunks)
NODE_ID               Unique node identifier                (default: node-unknown)
METADATA_SERVICE_URL  Base URL of the Metadata Service      (default: http://localhost:8001)
HEARTBEAT_INTERVAL    Seconds between heartbeat POSTs       (default: 5)
"""

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, Response as FastAPIResponse

from heartbeat import heartbeat_loop
from storage import (
    ChunkCorruptedError,
    ChunkNotFoundError,
    HashMismatchError,
    chunk_exists,
    delete_chunk,
    load_chunk,
    store_chunk,
)
from scrubber import scrub_loop

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

STORAGE_DIR: Path = Path(os.getenv("STORAGE_DIR", "/data/chunks"))
NODE_ID: str = os.getenv("NODE_ID", "node-unknown")
METADATA_SERVICE_URL: str = os.getenv("METADATA_SERVICE_URL", "http://localhost:8001")
HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL", "5"))
GATEWAY_URL: str = os.getenv("GATEWAY_URL", "http://gateway:9000")
SCRUB_INTERVAL: int = int(os.getenv("SCRUB_INTERVAL", "60"))

# SHA-256 hex digest is always exactly 64 lowercase hex characters.
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_hash(chunk_hash: str) -> None:
    """Raise 400 immediately if *chunk_hash* is not a valid SHA-256 hex string."""
    if not _HASH_RE.match(chunk_hash.lower()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid hash format: {chunk_hash!r}. Expected 64 hex characters.",
        )


# ---------------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------------

_heartbeat_task: asyncio.Task | None = None
_scrubber_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup and shutdown."""
    global _heartbeat_task, _scrubber_task, _stop_event

    # Ensure storage directory exists before accepting requests.
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Storage node starting — node_id=%s, storage_dir=%s", NODE_ID, STORAGE_DIR
    )

    # Launch the heartbeat background task.
    _stop_event = asyncio.Event()
    _heartbeat_task = asyncio.create_task(
        heartbeat_loop(
            node_id=NODE_ID,
            metadata_url=METADATA_SERVICE_URL,
            storage_dir=STORAGE_DIR,
            interval=HEARTBEAT_INTERVAL,
            stop_event=_stop_event,
        ),
        name="heartbeat",
    )
    logger.info(
        "Heartbeat task started — target=%s, interval=%ds",
        METADATA_SERVICE_URL,
        HEARTBEAT_INTERVAL,
    )

    # Launch the bit-rot scrubber background task.
    _scrubber_task = asyncio.create_task(
        scrub_loop(
            storage_dir=STORAGE_DIR,
            gateway_url=GATEWAY_URL,
            node_id=NODE_ID,
            interval=SCRUB_INTERVAL,
            stop_event=_stop_event,
        ),
        name="scrubber",
    )
    logger.info(
        "Scrubber task started — interval=%ds, gateway=%s",
        SCRUB_INTERVAL, GATEWAY_URL,
    )

    yield  # Application is running.

    # Graceful shutdown: signal background loops and wait briefly.
    logger.info("Storage node shutting down — stopping background tasks.")
    _stop_event.set()
    try:
        await asyncio.wait_for(_heartbeat_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        _heartbeat_task.cancel()
    try:
        await asyncio.wait_for(_scrubber_task, timeout=10.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        _scrubber_task.cancel()

    logger.info("Storage node stopped.")


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Storage Node Service",
    description=(
        "Content-addressed chunk storage node. "
        "Chunks are identified and verified by their SHA-256 hash."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# PUT /chunks/{hash}
# ---------------------------------------------------------------------------

@app.put(
    "/chunks/{chunk_hash}",
    status_code=status.HTTP_201_CREATED,
    summary="Store a chunk",
    response_description="Chunk stored successfully.",
)
async def put_chunk(chunk_hash: str, request: Request) -> JSONResponse:
    """
    Receive raw binary data and store it under *chunk_hash*.

    The service computes ``sha256(body)`` and compares it against the hash
    provided in the URL path. If they differ, **400 Bad Request** is returned
    and nothing is written to disk.

    - **201 Created** — chunk stored.
    - **400 Bad Request** — hash format invalid, or hash does not match content.
    """
    _validate_hash(chunk_hash)

    data: bytes = await request.body()
    if not data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body is empty. A non-empty binary payload is required.",
        )

    try:
        store_chunk(STORAGE_DIR, chunk_hash.lower(), data)
    except HashMismatchError as exc:
        logger.warning("PUT /chunks/%s — hash mismatch: %s", chunk_hash, exc)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except OSError as exc:
        logger.error("PUT /chunks/%s — disk error: %s", chunk_hash, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to write chunk to disk.",
        ) from exc

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={"hash": chunk_hash.lower(), "size_bytes": len(data)},
    )


# ---------------------------------------------------------------------------
# GET /chunks/{hash}
# ---------------------------------------------------------------------------

@app.get(
    "/chunks/{chunk_hash}",
    summary="Retrieve a chunk",
    response_description="Raw binary chunk data.",
    responses={
        200: {"content": {"application/octet-stream": {}}},
        404: {"description": "Chunk not found."},
        500: {"description": "Chunk corrupted — deleted from node."},
    },
)
async def get_chunk(chunk_hash: str) -> FastAPIResponse:
    """
    Read and return the binary chunk identified by *chunk_hash*.

    The stored file is integrity-checked before returning. If the hash no
    longer matches, the file is deleted and **500 Internal Server Error** is
    returned so the caller can fetch the chunk from another replica.

    - **200 OK** — raw binary payload in ``application/octet-stream``.
    - **404 Not Found** — no chunk with that hash exists on this node.
    - **500 Internal Server Error** — chunk exists but is corrupted (deleted).
    """
    _validate_hash(chunk_hash)

    try:
        data = load_chunk(STORAGE_DIR, chunk_hash.lower())
    except ChunkNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk {chunk_hash!r} not found on this node.",
        )
    except ChunkCorruptedError as exc:
        logger.error("GET /chunks/%s — corruption: %s", chunk_hash, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Chunk {chunk_hash!r} was corrupted and has been removed. "
                "Please fetch from another replica."
            ),
        ) from exc
    except OSError as exc:
        logger.error("GET /chunks/%s — disk error: %s", chunk_hash, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to read chunk from disk.",
        ) from exc

    return FastAPIResponse(
        content=data,
        media_type="application/octet-stream",
        headers={"X-Content-Hash": chunk_hash.lower()},
    )


# ---------------------------------------------------------------------------
# DELETE /chunks/{hash}
# ---------------------------------------------------------------------------

@app.delete(
    "/chunks/{chunk_hash}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a chunk",
    responses={
        204: {"description": "Chunk deleted."},
        404: {"description": "Chunk not found."},
    },
)
async def delete_chunk_endpoint(chunk_hash: str) -> Response:
    """
    Remove the chunk identified by *chunk_hash* from disk.

    - **204 No Content** — chunk deleted successfully.
    - **404 Not Found** — no such chunk exists on this node.
    """
    _validate_hash(chunk_hash)

    try:
        delete_chunk(STORAGE_DIR, chunk_hash.lower())
    except ChunkNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk {chunk_hash!r} not found on this node.",
        )
    except OSError as exc:
        logger.error(
            "DELETE /chunks/%s — disk error: %s", chunk_hash, exc, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete chunk from disk.",
        ) from exc

    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ---------------------------------------------------------------------------
# HEAD /chunks/{hash}
# ---------------------------------------------------------------------------

@app.head(
    "/chunks/{chunk_hash}",
    summary="Check chunk existence",
    responses={
        200: {"description": "Chunk is present on this node."},
        404: {"description": "Chunk not found on this node."},
    },
)
async def head_chunk(chunk_hash: str) -> Response:
    """
    Probe whether a chunk is stored on this node without transferring data.

    - **200 OK** — chunk file is present (no body returned).
    - **404 Not Found** — chunk is absent (no body returned).
    """
    _validate_hash(chunk_hash)

    if not chunk_exists(STORAGE_DIR, chunk_hash.lower()):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return Response(
        status_code=status.HTTP_200_OK,
        headers={"X-Content-Hash": chunk_hash.lower()},
    )


# ---------------------------------------------------------------------------
# Health / readiness probe (bonus — useful for container orchestrators)
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    summary="Health check",
    tags=["ops"],
    response_description="Node is healthy.",
)
async def health_check() -> JSONResponse:
    """Lightweight liveness probe for container orchestrators."""
    return JSONResponse(
        content={"status": "ok", "node_id": NODE_ID},
        status_code=status.HTTP_200_OK,
    )


@app.get(
    "/stats",
    summary="Node statistics",
    tags=["ops"],
    response_description="Current storage statistics for this node.",
)
async def node_stats() -> JSONResponse:
    """
    Return live storage statistics for this node.

    Fields
    ------
    node_id         : unique identifier of this node
    chunk_count     : number of chunks currently stored on disk
    free_space_bytes: available bytes on the storage volume
    free_space_gb   : free_space_bytes expressed in GiB (2 decimal places)
    storage_dir     : absolute path of the chunk storage directory
    """
    from storage import count_chunks, disk_free_bytes

    free_bytes = disk_free_bytes(STORAGE_DIR)
    chunks = count_chunks(STORAGE_DIR)

    return JSONResponse(
        content={
            "node_id": NODE_ID,
            "chunk_count": chunks,
            "free_space_bytes": free_bytes,
            "free_space_gb": round(free_bytes / (1024 ** 3), 2),
            "storage_dir": str(STORAGE_DIR),
        },
        status_code=status.HTTP_200_OK,
    )


# ---------------------------------------------------------------------------
# Entrypoint (for local development without Docker)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level="info",
        reload=False,
    )
