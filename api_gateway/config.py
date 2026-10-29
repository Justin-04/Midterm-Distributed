"""
config.py — Central configuration for the API Gateway.
All values are read from environment variables with sensible defaults.
"""
from __future__ import annotations
import os

# ── etcd ─────────────────────────────────────────────────────────────────────
ETCD_HOST: str = os.getenv("ETCD_HOST", "localhost")
ETCD_PORT: int = int(os.getenv("ETCD_PORT", "2379"))

# ── Storage nodes ─────────────────────────────────────────────────────────────
# Comma-separated  node_id=http://host:port  pairs
STORAGE_NODES_RAW: str = os.getenv(
    "STORAGE_NODES",
    "node-1=http://localhost:8101,"
    "node-2=http://localhost:8102,"
    "node-3=http://localhost:8103,"
    "node-4=http://localhost:8104",
)

def parse_storage_nodes() -> dict[str, str]:
    nodes: dict[str, str] = {}
    for pair in STORAGE_NODES_RAW.split(","):
        pair = pair.strip()
        if "=" in pair:
            nid, url = pair.split("=", 1)
            nodes[nid.strip()] = url.strip()
    return nodes

# ── Replication ────────────────────────────────────────────────────────────────
REPLICATION_FACTOR: int = int(os.getenv("REPLICATION_FACTOR", "3"))
WRITE_QUORUM:       int = int(os.getenv("WRITE_QUORUM",       "2"))
READ_QUORUM:        int = int(os.getenv("READ_QUORUM",        "2"))

# ── Chunking ──────────────────────────────────────────────────────────────────
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", str(4 * 1024 * 1024)))  # 4 MiB
CHUNKING_MODE: str = os.getenv("CHUNKING_MODE", "fixed")

# ── JWT ───────────────────────────────────────────────────────────────────────
JWT_SECRET_KEY:  str = os.getenv("JWT_SECRET_KEY",  "change-me-in-production-please")
JWT_ALGORITHM:   str = os.getenv("JWT_ALGORITHM",   "HS256")
JWT_EXPIRE_MINS: int = int(os.getenv("JWT_EXPIRE_MINS", "60"))

# ── Rate limiting ─────────────────────────────────────────────────────────────
RATE_LIMIT:       str = os.getenv("RATE_LIMIT",       "120/minute")
RATE_LIMIT_BURST: int = int(os.getenv("RATE_LIMIT_BURST", "20"))

# ── Gateway ───────────────────────────────────────────────────────────────────
GATEWAY_PORT: int = int(os.getenv("GATEWAY_PORT", "9100"))
HTTP_TIMEOUT: float = float(os.getenv("HTTP_TIMEOUT", "30"))
