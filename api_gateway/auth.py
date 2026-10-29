"""
auth.py — JWT authentication with OAuth2 password flow.

Endpoints (mounted in main.py):
    POST /auth/token   — exchange username+password for Bearer JWT
    GET  /auth/me      — return current user info

Roles:  "admin" | "user"
Admin role required for bucket deletion and cluster management.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel

from config import JWT_SECRET_KEY, JWT_ALGORITHM, JWT_EXPIRE_MINS

router = APIRouter(prefix="/auth", tags=["auth"])

# ── Password hashing (PBKDF2-SHA256 — no C extension needed) ─────────────────
import hashlib, secrets


def hash_password(plain: str) -> str:
    salt = secrets.token_hex(16)
    dk   = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), 260_000)
    return f"pbkdf2:{salt}:{dk.hex()}"


def verify_password(plain: str, stored: str) -> bool:
    if not stored.startswith("pbkdf2:"):
        return False
    _, salt, expected = stored.split(":", 2)
    dk = hashlib.pbkdf2_hmac("sha256", plain.encode(), salt.encode(), 260_000)
    return secrets.compare_digest(dk.hex(), expected)


# ── In-memory user store (replace with DB in production) ──────────────────────
class UserRecord(BaseModel):
    username: str
    hashed_password: str
    role: str = "user"
    disabled: bool = False


_USERS: dict[str, UserRecord] = {
    "admin": UserRecord(
        username="admin",
        hashed_password=hash_password(os.getenv("ADMIN_PASSWORD", "adminpass")),
        role="admin",
    ),
    "user1": UserRecord(
        username="user1",
        hashed_password=hash_password(os.getenv("USER1_PASSWORD", "userpass")),
        role="user",
    ),
}


def get_user(username: str) -> Optional[UserRecord]:
    return _USERS.get(username)


def authenticate_user(username: str, password: str) -> Optional[UserRecord]:
    user = get_user(username)
    if not user or user.disabled:
        return None
    return user if verify_password(password, user.hashed_password) else None


# ── Token ─────────────────────────────────────────────────────────────────────
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class TokenPayload(BaseModel):
    sub: str
    role: str


def create_access_token(username: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINS)
    return jwt.encode(
        {"sub": username, "role": role, "exp": expire,
         "iat": datetime.now(timezone.utc)},
        JWT_SECRET_KEY, algorithm=JWT_ALGORITHM,
    )


# ── Dependency ────────────────────────────────────────────────────────────────
_oauth2 = OAuth2PasswordBearer(tokenUrl="/auth/token")

_UNAUTH = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid or expired credentials.",
    headers={"WWW-Authenticate": "Bearer"},
)


async def get_current_user(token: str = Depends(_oauth2)) -> TokenPayload:
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub", "")
        role: str = payload.get("role", "user")
        if not username:
            raise _UNAUTH
    except JWTError:
        raise _UNAUTH
    if not get_user(username):
        raise _UNAUTH
    return TokenPayload(sub=username, role=role)


async def require_admin(current: TokenPayload = Depends(get_current_user)) -> TokenPayload:
    if current.role != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required.")
    return current


# ── Routes ────────────────────────────────────────────────────────────────────
@router.post("/token", response_model=TokenResponse, summary="Obtain a Bearer JWT")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form.username, form.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user.username, user.role)
    return TokenResponse(access_token=token, expires_in=JWT_EXPIRE_MINS * 60)


@router.get("/me", summary="Current user info")
async def me(current: TokenPayload = Depends(get_current_user)):
    return {"username": current.sub, "role": current.role}
