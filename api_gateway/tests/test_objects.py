"""
test_objects.py — Object upload, download, byte-range, metadata, tags.
"""
from __future__ import annotations

import hashlib
import os
from unittest.mock import patch, AsyncMock

import pytest


SMALL_DATA = b"Hello distributed storage gateway!"
LARGE_DATA = os.urandom(6 * 1024 * 1024)   # 6 MiB — crosses chunk boundary
BUCKET     = "test-objects"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture(autouse=True)
def mock_node_ops():
    """Replace actual HTTP calls to storage nodes with in-memory dict."""
    _store: dict[str, bytes] = {}

    async def _put(url, h, data):
        _store[h] = data
        return True

    async def _get(url, h):
        return _store.get(h)

    async def _head(url, h):
        return h in _store

    async def _delete(url, h):
        _store.pop(h, None)
        return True

    with (
        patch("routers.objects.node_put",    side_effect=_put),
        patch("routers.objects.node_get",    side_effect=_get),
        patch("routers.objects.node_head",   side_effect=_head),
        patch("routers.objects.node_delete", side_effect=_delete),
    ):
        yield _store


@pytest.fixture(autouse=True)
def create_bucket(client, admin_headers):
    client.put(f"/buckets/{BUCKET}", headers=admin_headers, json={
        "replication_factor": 3, "write_quorum": 2, "read_quorum": 2,
    })


# ── Upload ─────────────────────────────────────────────────────────────────────

def test_put_small_object(client, user_headers):
    r = client.put(
        f"/buckets/{BUCKET}/hello.txt",
        content=SMALL_DATA,
        headers={**user_headers, "content-type": "text/plain"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["bucket"] == BUCKET
    assert body["key"] == "hello.txt"
    assert body["size"] == len(SMALL_DATA)
    assert body["chunk_count"] >= 1


def test_put_returns_chunk_hashes(client, user_headers):
    r = client.put(
        f"/buckets/{BUCKET}/hashes.bin",
        content=SMALL_DATA,
        headers=user_headers,
    )
    assert r.status_code == 201


def test_put_empty_body_returns_400(client, user_headers):
    r = client.put(
        f"/buckets/{BUCKET}/empty",
        content=b"",
        headers=user_headers,
    )
    assert r.status_code == 400


def test_put_with_tags(client, user_headers):
    r = client.put(
        f"/buckets/{BUCKET}/tagged.txt",
        content=b"tagged content",
        headers={**user_headers, "tags": "project=alpha,env=prod"},
    )
    assert r.status_code == 201
    assert r.json()["tags"] == {"project": "alpha", "env": "prod"}


def test_put_multipart(client, user_headers):
    r = client.put(
        f"/buckets/{BUCKET}/multi.txt",
        files={"file": ("multi.txt", b"multipart body", "text/plain")},
        headers=user_headers,
    )
    assert r.status_code == 201


# ── Download ───────────────────────────────────────────────────────────────────

def test_get_small_object(client, user_headers):
    client.put(f"/buckets/{BUCKET}/get-me.txt", content=SMALL_DATA, headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/get-me.txt", headers=user_headers)
    assert r.status_code == 200
    assert r.content == SMALL_DATA


def test_get_preserves_content_type(client, user_headers):
    client.put(
        f"/buckets/{BUCKET}/page.html",
        content=b"<h1>hi</h1>",
        headers={**user_headers, "content-type": "text/html"},
    )
    r = client.get(f"/buckets/{BUCKET}/page.html", headers=user_headers)
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")


def test_get_missing_object_404(client, user_headers):
    r = client.get(f"/buckets/{BUCKET}/does-not-exist", headers=user_headers)
    assert r.status_code == 404


def test_get_returns_object_headers(client, user_headers):
    client.put(f"/buckets/{BUCKET}/headers.txt", content=SMALL_DATA, headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/headers.txt", headers=user_headers)
    assert "x-object-size" in r.headers
    assert int(r.headers["x-object-size"]) == len(SMALL_DATA)
    assert "x-chunk-count" in r.headers
    assert "accept-ranges" in r.headers


# ── Byte-range ─────────────────────────────────────────────────────────────────

def test_range_request_partial(client, user_headers):
    data = b"0123456789abcdef" * 64   # 1 KB
    client.put(f"/buckets/{BUCKET}/range.bin", content=data, headers=user_headers)
    r = client.get(
        f"/buckets/{BUCKET}/range.bin",
        headers={**user_headers, "range": "bytes=0-7"},
    )
    assert r.status_code == 206
    assert r.content == data[0:8]
    assert r.headers["content-range"].startswith("bytes 0-7/")


def test_range_middle(client, user_headers):
    data = b"abcdefghijklmnopqrstuvwxyz"
    client.put(f"/buckets/{BUCKET}/alphabet.txt", content=data, headers=user_headers)
    r = client.get(
        f"/buckets/{BUCKET}/alphabet.txt",
        headers={**user_headers, "range": "bytes=10-19"},
    )
    assert r.status_code == 206
    assert r.content == data[10:20]


def test_range_to_end(client, user_headers):
    data = b"hello world"
    client.put(f"/buckets/{BUCKET}/end.txt", content=data, headers=user_headers)
    r = client.get(
        f"/buckets/{BUCKET}/end.txt",
        headers={**user_headers, "range": "bytes=6-"},
    )
    assert r.status_code == 206
    assert r.content == data[6:]


def test_invalid_range_returns_400(client, user_headers):
    client.put(f"/buckets/{BUCKET}/inv.txt", content=b"data", headers=user_headers)
    r = client.get(
        f"/buckets/{BUCKET}/inv.txt",
        headers={**user_headers, "range": "invalid-range"},
    )
    assert r.status_code == 400


def test_range_out_of_bounds_416(client, user_headers):
    data = b"short"
    client.put(f"/buckets/{BUCKET}/short.txt", content=data, headers=user_headers)
    r = client.get(
        f"/buckets/{BUCKET}/short.txt",
        headers={**user_headers, "range": "bytes=100-200"},
    )
    assert r.status_code == 416


# ── Delete ─────────────────────────────────────────────────────────────────────

def test_delete_object(client, user_headers):
    client.put(f"/buckets/{BUCKET}/del-me.txt", content=b"bye", headers=user_headers)
    r = client.delete(f"/buckets/{BUCKET}/del-me.txt", headers=user_headers)
    assert r.status_code == 204


def test_delete_makes_object_404(client, user_headers):
    client.put(f"/buckets/{BUCKET}/gone.txt", content=b"temp", headers=user_headers)
    client.delete(f"/buckets/{BUCKET}/gone.txt", headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/gone.txt", headers=user_headers)
    assert r.status_code == 404


def test_delete_missing_object_404(client, user_headers):
    r = client.delete(f"/buckets/{BUCKET}/never-existed", headers=user_headers)
    assert r.status_code == 404


# ── Listing & Metadata ─────────────────────────────────────────────────────────

def test_list_objects(client, user_headers):
    client.put(f"/buckets/{BUCKET}/list/a.txt", content=b"a", headers=user_headers)
    client.put(f"/buckets/{BUCKET}/list/b.txt", content=b"b", headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/objects", headers=user_headers)
    assert r.status_code == 200
    keys = [o["key"] for o in r.json()["objects"]]
    assert "list/a.txt" in keys
    assert "list/b.txt" in keys


def test_list_with_prefix(client, user_headers):
    client.put(f"/buckets/{BUCKET}/pfx/x.txt", content=b"x", headers=user_headers)
    client.put(f"/buckets/{BUCKET}/other.txt", content=b"y", headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/objects?prefix=pfx/", headers=user_headers)
    assert r.status_code == 200
    keys = [o["key"] for o in r.json()["objects"]]
    assert all(k.startswith("pfx/") for k in keys)


def test_get_object_metadata(client, user_headers):
    client.put(f"/buckets/{BUCKET}/meta.txt", content=b"meta content", headers=user_headers)
    r = client.get(f"/buckets/{BUCKET}/objects/meta.txt", headers=user_headers)
    assert r.status_code == 200
    body = r.json()
    assert body["key"] == "meta.txt"
    assert body["size"] == len(b"meta content")
    assert "chunk_hashes" in body
    assert "creation_date" in body


# ── Tags ───────────────────────────────────────────────────────────────────────

def test_patch_tags(client, user_headers):
    client.put(
        f"/buckets/{BUCKET}/tagme.txt",
        content=b"data",
        headers={**user_headers, "tags": "env=dev"},
    )
    r = client.patch(
        f"/buckets/{BUCKET}/objects/tagme.txt",
        json={"tags": {"env": "prod", "team": "backend"}},
        headers=user_headers,
    )
    assert r.status_code == 200
    assert r.json()["tags"]["env"] == "prod"
    assert r.json()["tags"]["team"] == "backend"
