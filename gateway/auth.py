"""
auth.py

JWT-based authentication for the gateway.

Flow
----
- POST /auth/token  : exchange username+password for a JWT access token
- All protected routes require  Authorization: Bearer <token>

Tokens carry:
    sub  : username
    role : "admin" | "user"
    exp  : expiry timestamp

Environment variables
---------------------
JWT_SECRET_KEY   : signing secret  (default: change-me-in-production)
JWT_ALGORITHM    : algorithm       (default: HS256)
JWT_EXPIRE_MINS  : token TTL       (default: 60)

For demo purposes a small in-memory user store is provided.
Replace with a real database in production.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

JWT_SECRET_KEY:  str = os.getenv("JWT_SECRET_KEY", "change-me-in-production-please")
JWT_ALGORITHM:   str = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINS: int = int(os.getenv("JWT_EXPIRE_MINS", "60"))

# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd_ctx.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_ctx.verify(plain, hashed)


# ---------------------------------------------------------------------------
# In-memory user store  (replace with DB in production)
# ---------------------------------------------------------------------------

class UserRecord(BaseModel):
    username: str
    hashed_password: str
    role: str = "user"         # "admin" | "user"
    disabled: bool = False


# Seed users — passwords are "adminpass" and "userpass"
_USERS: dict[str, UserRecord] = {
    "admin": UserRecord(
        username="admin",
        hashed_password=hash_password("adminpass"),
        role="admin",
    ),
    "user1": UserRecord(
        username="user1",
        hashed_password=hash_password("userpass"),
        role="user",
    ),
}


def get_user(username: str) -> Optional[UserRecord]:
    return _USERS.get(username)


def authenticate_user(username: str, password: str) -> Optional[UserRecord]:
    user = get_user(username)
    if not user or user.disabled:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


# ---------------------------------------------------------------------------
# Token creation
# ---------------------------------------------------------------------------

class TokenPayload(BaseModel):
    sub: str
    role: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


def create_access_token(username: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINS)
    payload = {
        "sub":  username,
        "role": role,
        "exp":  expire,
        "iat":  datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


# ---------------------------------------------------------------------------
# FastAPI dependency — decode + validate token
# ---------------------------------------------------------------------------

_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

_CREDENTIALS_EXCEPTION = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials.",
    headers={"WWW-Authenticate": "Bearer"},
)


async def get_current_user(token: str = Depends(_oauth2_scheme)) -> TokenPayload:
    """Decode and validate the Bearer JWT.  Raises 401 on any failure."""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        username: str = payload.get("sub", "")
        role:     str = payload.get("role", "user")
        if not username:
            raise _CREDENTIALS_EXCEPTION
    except JWTError:
        raise _CREDENTIALS_EXCEPTION

    user = get_user(username)
    if user is None or user.disabled:
        raise _CREDENTIALS_EXCEPTION

    return TokenPayload(sub=username, role=role)


async def require_admin(current: TokenPayload = Depends(get_current_user)) -> TokenPayload:
    """Dependency that additionally requires role == 'admin'."""
    if current.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required.",
        )
    return current
