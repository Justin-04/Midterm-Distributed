"""
conftest.py — Shared pytest fixtures.

All tests run without Docker/etcd/storage nodes by:
  1. Replacing the FastAPI lifespan with a no-op (no real etcd/httpx created).
  2. Injecting a FakeEtcd and in-memory node-op mocks into the store module.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ── In-memory etcd replacement ────────────────────────────────────────────────
class FakeEtcd:
    def __init__(self):
        self._d: dict[str, bytes] = {}

    def put(self, key: str, value):
        self._d[key] = value.encode() if isinstance(value, str) else value

    def get(self, key: str):
        raw  = self._d.get(key)
        meta = MagicMock(); meta.key = key.encode()
        return raw, meta

    def delete(self, key: str):
        self._d.pop(key, None)

    def get_prefix(self, prefix: str):
        for k, v in list(self._d.items()):
            if k.startswith(prefix):
                meta = MagicMock(); meta.key = k.encode()
                yield v, meta

    def transaction(self, compare, success, failure):
        for op in success:
            if isinstance(op, tuple) and len(op) == 2:
                self.put(op[0], op[1])
        return True, None

    class transactions:
        @staticmethod
        def version(key): return MagicMock(__eq__=lambda s, o: True)
        @staticmethod
        def value(key): return MagicMock(__eq__=lambda s, o: True)
        @staticmethod
        def put(key, val): return (key, val)


# ── In-memory node operation store ───────────────────────────────────────────
_CHUNK_STORE: dict[str, bytes] = {}


async def _node_put(url, h, data):
    _CHUNK_STORE[h] = data
    return True

async def _node_get(url, h):
    return _CHUNK_STORE.get(h)

async def _node_head(url, h):
    return h in _CHUNK_STORE

async def _node_delete(url, h):
    _CHUNK_STORE.pop(h, None)
    return True

async def _node_stats(url):
    return {"chunk_count": len(_CHUNK_STORE), "free_space_bytes": 10**9, "free_space_gb": 1.0}


# ── Build app with patched lifespan ──────────────────────────────────────────
@pytest.fixture(scope="session")
def fake_etcd():
    return FakeEtcd()


@pytest.fixture(scope="session")
def test_app(fake_etcd):
    from ring import ConsistentHashRing
    import store

    nodes = {
        "node-1": "http://node1:8000",
        "node-2": "http://node2:8000",
        "node-3": "http://node3:8000",
        "node-4": "http://node4:8000",
    }
    ring = ConsistentHashRing(vnode_count=10)
    for nid, url in nodes.items():
        ring.add_node(nid, url)

    http_mock = MagicMock()
    store.init(fake_etcd, http_mock, nodes)
    store._ring = ring

    # Patch lifespan to a no-op so TestClient doesn't try to connect etcd
    @asynccontextmanager
    async def _noop_lifespan(app):
        yield

    with patch("main.lifespan", _noop_lifespan):
        import main
        main.app.router.lifespan_context = _noop_lifespan
        yield main.app


@pytest.fixture
def client(test_app):
    with (
        patch("store._etcd", test_app.state.__dict__.get("_etcd_ref", None) or
              _get_fake_etcd_from_store()),
        patch("routers.objects.node_put",    side_effect=_node_put),
        patch("routers.objects.node_get",    side_effect=_node_get),
        patch("routers.objects.node_head",   side_effect=_node_head),
        patch("routers.objects.node_delete", side_effect=_node_delete),
        patch("routers.cluster.store.node_stats", side_effect=_node_stats),
    ):
        with TestClient(test_app, raise_server_exceptions=True) as c:
            yield c


def _get_fake_etcd_from_store():
    import store
    return store._etcd


# ── Auth helpers ──────────────────────────────────────────────────────────────
@pytest.fixture
def admin_token(client):
    r = client.post("/auth/token", data={"username": "admin", "password": "adminpass"})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture
def user_token(client):
    r = client.post("/auth/token", data={"username": "user1", "password": "userpass"})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture
def admin_headers(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture
def user_headers(user_token):
    return {"Authorization": f"Bearer {user_token}"}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
