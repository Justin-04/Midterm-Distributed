"""
rate_limit.py

In-memory sliding-window rate limiter implemented as a FastAPI dependency.

Each (identifier, endpoint) pair gets its own independent window so that
heavy use of one route does not penalise others.

Defaults (overridable via env vars):
    RATE_LIMIT_RPM   : requests per minute  (default 120)
    RATE_LIMIT_BURST : burst allowance      (default 20)

Usage
-----
    @app.get("/some/route")
    async def handler(
        _: None = Depends(rate_limit_dependency("some_route")),
    ):
        ...

The limiter is keyed on:
  - Authenticated requests  →  JWT subject (username)
  - Unauthenticated requests →  client IP address
"""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

from auth import TokenPayload, get_current_user

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

RATE_LIMIT_RPM:   int = int(os.getenv("RATE_LIMIT_RPM",   "120"))
RATE_LIMIT_BURST: int = int(os.getenv("RATE_LIMIT_BURST", "20"))

# Allow unauthenticated probes (health, token endpoint) at a gentler rate
UNAUTH_RPM:   int = int(os.getenv("UNAUTH_RATE_LIMIT_RPM",   "30"))
UNAUTH_BURST: int = int(os.getenv("UNAUTH_RATE_LIMIT_BURST", "10"))

# ---------------------------------------------------------------------------
# Sliding-window counter store
# ---------------------------------------------------------------------------

# { key → deque of request timestamps (float) }
_windows: dict[str, deque] = defaultdict(deque)


def _sliding_window_check(
    key: str,
    rpm: int,
    burst: int,
    now: float,
) -> tuple[bool, int]:
    """
    Sliding-window check.

    Returns (allowed, remaining_requests_in_window).
    The window is 60 seconds.
    """
    window_start = now - 60.0
    q = _windows[key]

    # Remove expired timestamps.
    while q and q[0] < window_start:
        q.popleft()

    count = len(q)

    # Allow burst: up to rpm+burst total requests in any 60s window.
    limit = rpm + burst
    if count >= limit:
        return False, 0

    q.append(now)
    return True, limit - count - 1


# ---------------------------------------------------------------------------
# FastAPI dependency factory
# ---------------------------------------------------------------------------

def rate_limit(scope: str, rpm: int = RATE_LIMIT_RPM, burst: int = RATE_LIMIT_BURST):
    """
    Return a FastAPI dependency that enforces rate limiting for *scope*.

    Authenticated requests are keyed by username; anonymous by IP.
    """

    async def _check(
        request: Request,
        current_user: TokenPayload | None = Depends(_optional_user),
    ) -> None:
        identity = current_user.sub if current_user else (
            request.client.host if request.client else "unknown"
        )
        key = f"{scope}:{identity}"
        allowed, remaining = _sliding_window_check(key, rpm, burst, time.time())

        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded for {scope!r}. Try again shortly.",
                headers={"Retry-After": "60", "X-RateLimit-Remaining": "0"},
            )

    return _check


# ---------------------------------------------------------------------------
# Optional auth (returns None instead of raising 401)
# ---------------------------------------------------------------------------

async def _optional_user(request: Request) -> TokenPayload | None:
    """Like get_current_user but returns None instead of raising on missing/bad token."""
    auth: str = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.removeprefix("Bearer ").strip()
    try:
        from auth import JWT_SECRET_KEY, JWT_ALGORITHM, get_user
        from jose import jwt as _jwt, JWTError
        payload = _jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username = payload.get("sub", "")
        role = payload.get("role", "user")
        if username and get_user(username):
            return TokenPayload(sub=username, role=role)
    except Exception:
        pass
    return None
