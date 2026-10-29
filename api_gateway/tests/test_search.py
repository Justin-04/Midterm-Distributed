"""
test_search.py — Advanced search and filtering tests.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

BUCKET = "search-bucket"


@pytest.fixture(autouse=True)
def mock_node_ops():
    _store: dict = {}

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
def seed_data(client, admin_headers, user_headers):
    """Pre-populate test objects for search tests."""
    client.put(f"/buckets/{BUCKET}", headers=admin_headers, json={})
    # Various objects with different sizes and tags
    client.put(f"/buckets/{BUCKET}/reports/q1.pdf",
               content=b"q1 report" * 100,
               headers={**user_headers, "tags": "type=report,quarter=q1"})
    client.put(f"/buckets/{BUCKET}/reports/q2.pdf",
               content=b"q2 report" * 200,
               headers={**user_headers, "tags": "type=report,quarter=q2"})
    client.put(f"/buckets/{BUCKET}/images/photo.jpg",
               content=b"img" * 50,
               headers={**user_headers, "tags": "type=image"})
    client.put(f"/buckets/{BUCKET}/docs/readme.md",
               content=b"readme" * 10,
               headers={**user_headers, "tags": "type=doc"})


def test_search_all(client, user_headers):
    r = client.get("/search", headers=user_headers)
    assert r.status_code == 200
    assert r.json()["total"] >= 4


def test_search_by_bucket(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}", headers=user_headers)
    assert r.status_code == 200
    assert r.json()["total"] >= 4
    for obj in r.json()["results"]:
        assert obj["bucket"] == BUCKET


def test_search_by_prefix(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&prefix=reports/", headers=user_headers)
    assert r.status_code == 200
    assert r.json()["total"] == 2
    for obj in r.json()["results"]:
        assert obj["key"].startswith("reports/")


def test_search_by_min_size(client, user_headers):
    # q2 report is 200*9=1800 bytes, q1 is 900 bytes
    r = client.get(f"/search?bucket={BUCKET}&min_size=1000", headers=user_headers)
    assert r.status_code == 200
    for obj in r.json()["results"]:
        assert obj["size"] >= 1000


def test_search_by_max_size(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&max_size=200", headers=user_headers)
    assert r.status_code == 200
    for obj in r.json()["results"]:
        assert obj["size"] <= 200


def test_search_by_size_range(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&min_size=100&max_size=1000",
                   headers=user_headers)
    assert r.status_code == 200
    for obj in r.json()["results"]:
        assert 100 <= obj["size"] <= 1000


def test_search_by_tag(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&type=report", headers=user_headers)
    assert r.status_code == 200
    for obj in r.json()["results"]:
        assert obj["tags"].get("type") == "report"


def test_search_multiple_tags(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&type=report&quarter=q1",
                   headers=user_headers)
    assert r.status_code == 200
    assert r.json()["total"] == 1
    assert r.json()["results"][0]["key"] == "reports/q1.pdf"


def test_search_pagination(client, user_headers):
    r1 = client.get(f"/search?bucket={BUCKET}&limit=2&offset=0", headers=user_headers)
    r2 = client.get(f"/search?bucket={BUCKET}&limit=2&offset=2", headers=user_headers)
    assert r1.status_code == 200
    assert r2.status_code == 200
    keys1 = {o["key"] for o in r1.json()["results"]}
    keys2 = {o["key"] for o in r2.json()["results"]}
    # Pages should not overlap
    assert keys1.isdisjoint(keys2)


def test_search_no_results(client, user_headers):
    r = client.get(f"/search?bucket={BUCKET}&prefix=nonexistent/", headers=user_headers)
    assert r.status_code == 200
    assert r.json()["total"] == 0
    assert r.json()["results"] == []


def test_search_requires_auth(client):
    r = client.get("/search")
    assert r.status_code == 401
