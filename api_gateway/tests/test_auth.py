"""
test_auth.py — JWT authentication tests.
"""
import pytest


def test_token_admin(client):
    r = client.post("/auth/token", data={"username": "admin", "password": "adminpass"})
    assert r.status_code == 200
    body = r.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"
    assert body["expires_in"] > 0


def test_token_user(client):
    r = client.post("/auth/token", data={"username": "user1", "password": "userpass"})
    assert r.status_code == 200


def test_token_wrong_password(client):
    r = client.post("/auth/token", data={"username": "admin", "password": "wrong"})
    assert r.status_code == 401


def test_token_unknown_user(client):
    r = client.post("/auth/token", data={"username": "ghost", "password": "x"})
    assert r.status_code == 401


def test_me_endpoint(client, admin_headers):
    r = client.get("/auth/me", headers=admin_headers)
    assert r.status_code == 200
    assert r.json()["username"] == "admin"
    assert r.json()["role"] == "admin"


def test_me_user_role(client, user_headers):
    r = client.get("/auth/me", headers=user_headers)
    assert r.status_code == 200
    assert r.json()["role"] == "user"


def test_protected_requires_token(client):
    r = client.get("/buckets")
    assert r.status_code == 401


def test_invalid_bearer(client):
    r = client.get("/buckets", headers={"Authorization": "Bearer totally.fake.token"})
    assert r.status_code == 401


def test_admin_only_endpoint_with_user_token(client, user_headers):
    """Bucket creation requires admin — user token should get 403."""
    r = client.put("/buckets/my-bucket", headers=user_headers, json={})
    assert r.status_code == 403
