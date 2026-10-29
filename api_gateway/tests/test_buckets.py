"""
test_buckets.py — Bucket CRUD tests.
"""
import pytest


def test_create_bucket(client, admin_headers):
    r = client.put("/buckets/photos", headers=admin_headers, json={
        "encoding": "replication",
        "replication_factor": 3,
        "write_quorum": 2,
        "read_quorum": 2,
    })
    assert r.status_code == 201
    assert r.json()["config"]["bucket"] == "photos"


def test_create_bucket_quorum_violation(client, admin_headers):
    """W + R must be > N.  With N=3, W=1, R=1 → 1+1=2 ≤ 3 → should fail."""
    r = client.put("/buckets/bad", headers=admin_headers, json={
        "encoding": "replication",
        "replication_factor": 3,
        "write_quorum": 1,
        "read_quorum": 1,
    })
    assert r.status_code == 400, f"Expected 400, got {r.status_code}: {r.text}"


def test_get_bucket_config(client, admin_headers):
    client.put("/buckets/docs", headers=admin_headers, json={})
    r = client.get("/buckets/docs", headers=admin_headers)
    assert r.status_code == 200
    assert r.json()["bucket"] == "docs"


def test_get_nonexistent_bucket(client, admin_headers):
    r = client.get("/buckets/nonexistent-xyz", headers=admin_headers)
    assert r.status_code == 404


def test_list_buckets(client, admin_headers):
    client.put("/buckets/list-test-a", headers=admin_headers, json={})
    client.put("/buckets/list-test-b", headers=admin_headers, json={})
    r = client.get("/buckets", headers=admin_headers)
    assert r.status_code == 200
    buckets = [b["bucket"] for b in r.json()["buckets"]]
    assert "list-test-a" in buckets
    assert "list-test-b" in buckets


def test_delete_empty_bucket(client, admin_headers):
    client.put("/buckets/to-delete", headers=admin_headers, json={})
    r = client.delete("/buckets/to-delete", headers=admin_headers)
    assert r.status_code == 204


def test_delete_nonexistent_bucket(client, admin_headers):
    r = client.delete("/buckets/ghost-bucket", headers=admin_headers)
    assert r.status_code == 404


def test_delete_bucket_requires_admin(client, user_headers):
    r = client.delete("/buckets/any-bucket", headers=user_headers)
    assert r.status_code == 403


def test_erasure_bucket_config(client, admin_headers):
    r = client.put("/buckets/ec-bucket", headers=admin_headers, json={
        "encoding": "erasure",
        "replication_factor": 3,
        "write_quorum": 2,
        "read_quorum": 2,
        "erasure_k": 2,
        "erasure_m": 2,
    })
    assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
    assert r.json()["config"]["encoding"] == "erasure"
    assert r.json()["config"]["erasure_k"] == 2
