"""
routers/buckets.py

Bucket management endpoints:
    PUT    /buckets/{bucket}    — create / reconfigure
    GET    /buckets             — list all
    GET    /buckets/{bucket}    — get config
    DELETE /buckets/{bucket}    — delete (must be empty)
"""
from __future__ import annotations

from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from auth import TokenPayload, get_current_user, require_admin
from limiter import limiter
from store import (
    delete_bucket_config, get_bucket, list_buckets,
    list_objects_in_bucket, put_bucket,
)

router = APIRouter(prefix="/buckets", tags=["buckets"])


class BucketConfig(BaseModel):
    encoding:           str  = Field("replication", pattern="^(replication|erasure)$")
    replication_factor: int  = Field(3, ge=1, le=10)
    write_quorum:       int  = Field(2, ge=1)
    read_quorum:        int  = Field(2, ge=1)
    erasure_k:          int  = Field(2, ge=1)
    erasure_m:          int  = Field(1, ge=1)
    versioning:         bool = False


@router.put("/{bucket}", status_code=status.HTTP_201_CREATED)
@limiter.limit("60/minute")
async def create_bucket(
    request: Request,
    bucket: str,
    current: TokenPayload = Depends(require_admin),
):
    """
    Create or update a bucket configuration.  Admin only.

    Accepts a JSON body matching BucketConfig fields.
    All fields are optional — omitted fields get sensible defaults.
    """
    # Parse body manually so the endpoint works even with empty body
    try:
        body_json = await request.json()
    except Exception:
        body_json = {}

    cfg = BucketConfig.model_validate(body_json)

    if cfg.encoding == "replication" and cfg.write_quorum + cfg.read_quorum <= cfg.replication_factor:
        raise HTTPException(
            400,
            f"Quorum constraint violated: W({cfg.write_quorum}) + R({cfg.read_quorum}) "
            f"must be > N({cfg.replication_factor}).",
        )

    config = cfg.model_dump()
    config["bucket"] = bucket
    put_bucket(bucket, config)
    return {"message": f"Bucket {bucket!r} configured.", "config": config}


@router.get("", summary="List all buckets")
@limiter.limit("120/minute")
async def list_all_buckets(
    request: Request,
    current: TokenPayload = Depends(get_current_user),
):
    return {"buckets": list_buckets()}


@router.get("/{bucket}", summary="Get bucket configuration")
@limiter.limit("120/minute")
async def get_bucket_config(
    request: Request,
    bucket: str,
    current: TokenPayload = Depends(get_current_user),
):
    cfg = get_bucket(bucket)
    if cfg is None:
        raise HTTPException(404, f"Bucket {bucket!r} not found.")
    return cfg


@router.delete("/{bucket}", status_code=status.HTTP_204_NO_CONTENT)
@limiter.limit("30/minute")
async def delete_bucket(
    request: Request,
    bucket: str,
    force: bool = False,
    current: TokenPayload = Depends(require_admin),
):
    """Delete a bucket.  Fails if objects remain unless force=true.  Admin only."""
    cfg = get_bucket(bucket)
    if cfg is None:
        raise HTTPException(404, f"Bucket {bucket!r} not found.")

    objects = list_objects_in_bucket(bucket)
    if objects and not force:
        raise HTTPException(
            409,
            f"Bucket {bucket!r} is not empty ({len(objects)} object(s)). "
            "Use ?force=true to delete anyway.",
        )
    delete_bucket_config(bucket)
    return
