"""
test_gateway.py

End-to-end integration tests for the S3-compatible API Gateway.

Requires all containers to be running:
    docker compose up --build -d

Override the gateway URL:
    GATEWAY_URL=http://localhost:9000 python test_gateway.py

Run:
    python test_gateway.py
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
import random
import string

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL: str = os.getenv("GATEWAY_URL", "http://localhost:9000").rstrip("/")
TIMEOUT:  int = 30

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
INFO = "\033[94mINFO\033[0m"

_results: list[tuple[str, bool, str]] = []


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

def run(name: str, fn) -> bool:
    try:
        fn()
        _results.append((name, True, ""))
        print(f"  [{PASS}] {name}")
        return True
    except AssertionError as exc:
        _results.append((name, False, str(exc)))
        print(f"  [{FAIL}] {name}")
        print(f"           {exc}")
        return False
    except Exception as exc:
        msg = f"{type(exc).__name__}: {exc}"
        _results.append((name, False, msg))
        print(f"  [{FAIL}] {name}")
        print(f"           {msg}")
        return False


def section(title: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def _rand_key(prefix: str = "obj") -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}-{suffix}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BUCKET     = "test-bucket"
SMALL_DATA = b"Hello, distributed storage!"
SMALL_KEY  = _rand_key("small")

MED_DATA   = os.urandom(6 * 1024 * 1024)   # 6 MiB — spans two 4 MiB chunks
MED_KEY    = _rand_key("medium")

# For deduplication test: two objects sharing identical content
DUP_DATA   = b"deduplicated content " * 1_000
DUP_KEY_A  = _rand_key("dup-a")
DUP_KEY_B  = _rand_key("dup-b")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health():
    r = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    body = r.json()
    assert body.get("status") == "ok"
    assert "nodes" in body
    assert body["nodes"] == 4, f"expected 4 nodes, got {body['nodes']}"
    print(f"    [{INFO}] chunking={body.get('chunking')!r}, nodes={body.get('nodes')}")


def test_ring_stats():
    r = requests.get(f"{BASE_URL}/ring/stats", timeout=TIMEOUT)
    assert r.status_code == 200
    body = r.json()
    assert body["physical_nodes"] == 4
    assert body["sample_size"] == 10_000
    dist = body["distribution"]
    assert len(dist) == 4

    print(f"\n    {'Node':<12} {'Chunks':>8} {'%':>7}")
    print(f"    {'─'*12} {'─'*8} {'─'*7}")
    for nid, info in sorted(dist.items()):
        print(f"    {nid:<12} {info['chunks']:>8} {info['pct']:>6.2f}%")


# ---------------------------------------------------------------------------
# PUT
# ---------------------------------------------------------------------------

def test_put_small_object():
    r = requests.put(
        f"{BASE_URL}/{BUCKET}/{SMALL_KEY}",
        data=SMALL_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 201, f"expected 201, got {r.status_code}: {r.text}"
    body = r.json()
    assert body["bucket"] == BUCKET
    assert body["key"]    == SMALL_KEY
    assert body["size"]   == len(SMALL_DATA)
    assert body["chunk_count"] >= 1
    assert isinstance(body["chunk_hashes"], list)
    assert len(body["chunk_hashes"]) == body["chunk_count"]
    print(f"    [{INFO}] {SMALL_KEY}: {body['chunk_count']} chunk(s)")


def test_put_medium_object():
    """6 MiB object should be split across at least 2 chunks."""
    r = requests.put(
        f"{BASE_URL}/{BUCKET}/{MED_KEY}",
        data=MED_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 201, f"expected 201, got {r.status_code}: {r.text}"
    body = r.json()
    assert body["size"] == len(MED_DATA)
    assert body["chunk_count"] >= 2, (
        f"6 MiB object should produce ≥2 chunks, got {body['chunk_count']}"
    )
    print(f"    [{INFO}] {MED_KEY}: {body['chunk_count']} chunks for {len(MED_DATA)//1024} KiB")


def test_put_empty_body_400():
    r = requests.put(
        f"{BASE_URL}/{BUCKET}/empty-object",
        data=b"",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


def test_put_response_has_all_fields():
    key = _rand_key("fields")
    r = requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=b"field check payload",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 201
    body = r.json()
    for field in ("bucket", "key", "size", "chunk_count", "chunk_hashes", "version", "created_at"):
        assert field in body, f"Missing field {field!r} in PUT response"


# ---------------------------------------------------------------------------
# GET
# ---------------------------------------------------------------------------

def test_get_small_object_roundtrip():
    r = requests.get(f"{BASE_URL}/{BUCKET}/{SMALL_KEY}", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    assert r.content == SMALL_DATA, (
        f"Data mismatch: got {r.content!r}, expected {SMALL_DATA!r}"
    )


def test_get_medium_object_integrity():
    """SHA-256 of reassembled object must match original."""
    r = requests.get(f"{BASE_URL}/{BUCKET}/{MED_KEY}", timeout=60)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    assert _sha256(r.content) == _sha256(MED_DATA), "Integrity check failed for 6 MiB object."
    assert r.content == MED_DATA, "Byte-level content mismatch for medium object."


def test_get_response_headers():
    r = requests.get(f"{BASE_URL}/{BUCKET}/{SMALL_KEY}", timeout=TIMEOUT)
    assert r.status_code == 200
    assert "x-object-size"    in r.headers, "Missing X-Object-Size header"
    assert "x-chunk-count"    in r.headers, "Missing X-Chunk-Count header"
    assert "x-object-version" in r.headers, "Missing X-Object-Version header"
    assert int(r.headers["x-object-size"]) == len(SMALL_DATA)


def test_get_missing_object_404():
    r = requests.get(f"{BASE_URL}/{BUCKET}/does-not-exist-xyz", timeout=TIMEOUT)
    assert r.status_code == 404, f"expected 404, got {r.status_code}"


def test_get_content_type_preserved():
    key = _rand_key("ct")
    requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=b'{"json": true}',
        headers={"Content-Type": "application/json"},
        timeout=TIMEOUT,
    )
    r = requests.get(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
    assert r.status_code == 200
    assert "application/json" in r.headers.get("content-type", ""), (
        f"Content-Type not preserved: {r.headers.get('content-type')}"
    )


# ---------------------------------------------------------------------------
# LIST
# ---------------------------------------------------------------------------

def test_list_bucket_contains_uploaded_objects():
    r = requests.get(f"{BASE_URL}/{BUCKET}", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    body = r.json()
    assert body["bucket"] == BUCKET
    assert "objects" in body
    assert "object_count" in body

    keys_in_bucket = {obj["key"] for obj in body["objects"]}
    assert SMALL_KEY in keys_in_bucket, f"{SMALL_KEY!r} not found in bucket listing"
    assert MED_KEY   in keys_in_bucket, f"{MED_KEY!r} not found in bucket listing"
    print(f"    [{INFO}] Bucket '{BUCKET}' contains {body['object_count']} object(s)")


def test_list_object_summary_fields():
    r = requests.get(f"{BASE_URL}/{BUCKET}", timeout=TIMEOUT)
    assert r.status_code == 200
    for obj in r.json()["objects"]:
        for field in ("bucket", "key", "size", "creation_date", "chunk_count"):
            assert field in obj, f"Missing field {field!r} in listing entry"


def test_list_empty_bucket():
    r = requests.get(f"{BASE_URL}/nonexistent-bucket-xyz", timeout=TIMEOUT)
    assert r.status_code == 200
    body = r.json()
    assert body["object_count"] == 0
    assert body["objects"] == []


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def test_deduplication_same_content_two_objects():
    """
    Uploading the same content under two different keys must succeed for both,
    and both must return the identical byte stream on GET.
    """
    for key in (DUP_KEY_A, DUP_KEY_B):
        r = requests.put(
            f"{BASE_URL}/{BUCKET}/{key}",
            data=DUP_DATA,
            headers={"Content-Type": "application/octet-stream"},
            timeout=TIMEOUT,
        )
        assert r.status_code == 201, f"PUT {key}: {r.status_code}"

    for key in (DUP_KEY_A, DUP_KEY_B):
        r = requests.get(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
        assert r.status_code == 200
        assert r.content == DUP_DATA, f"Content mismatch for {key}"

    # Both objects should report the same chunk hashes
    r_a = requests.put(
        f"{BASE_URL}/{BUCKET}/{DUP_KEY_A}",
        data=DUP_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r_b = requests.put(
        f"{BASE_URL}/{BUCKET}/{DUP_KEY_B}",
        data=DUP_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert sorted(r_a.json()["chunk_hashes"]) == sorted(r_b.json()["chunk_hashes"]), (
        "Duplicate objects should reference identical chunk hashes"
    )
    print(f"    [{INFO}] Both objects share {len(r_a.json()['chunk_hashes'])} chunk(s)")


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

def test_delete_object_returns_204():
    key = _rand_key("del")
    requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=b"to be deleted",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.delete(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
    assert r.status_code == 204, f"expected 204, got {r.status_code}"


def test_delete_makes_object_unavailable():
    key = _rand_key("gone")
    requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=b"delete me",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    requests.delete(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)

    r = requests.get(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
    assert r.status_code == 404, f"After DELETE: expected 404, got {r.status_code}"


def test_delete_removes_from_listing():
    key = _rand_key("list-del")
    requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=b"listed then deleted",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    listing_before = {
        obj["key"] for obj in
        requests.get(f"{BASE_URL}/{BUCKET}", timeout=TIMEOUT).json()["objects"]
    }
    assert key in listing_before, "Object not in listing before DELETE"

    requests.delete(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)

    listing_after = {
        obj["key"] for obj in
        requests.get(f"{BASE_URL}/{BUCKET}", timeout=TIMEOUT).json()["objects"]
    }
    assert key not in listing_after, "Object still in listing after DELETE"


def test_delete_missing_object_404():
    r = requests.delete(f"{BASE_URL}/{BUCKET}/never-existed-abc", timeout=TIMEOUT)
    assert r.status_code == 404, f"expected 404, got {r.status_code}"


def test_delete_shared_chunks_not_gc_until_zero_refcount():
    """
    When two objects share chunks (deduplication), deleting the first should
    NOT remove the chunks from storage — the second must still be readable.
    """
    # Both keys were uploaded in the deduplication test above.
    requests.delete(f"{BASE_URL}/{BUCKET}/{DUP_KEY_A}", timeout=TIMEOUT)

    r = requests.get(f"{BASE_URL}/{BUCKET}/{DUP_KEY_B}", timeout=TIMEOUT)
    assert r.status_code == 200, (
        f"DUP_KEY_B should still be readable after DUP_KEY_A deleted, got {r.status_code}"
    )
    assert r.content == DUP_DATA, "Content mismatch after partial delete"

    # Cleanup
    requests.delete(f"{BASE_URL}/{BUCKET}/{DUP_KEY_B}", timeout=TIMEOUT)


# ---------------------------------------------------------------------------
# Nested key paths
# ---------------------------------------------------------------------------

def test_nested_key_path():
    """Keys may contain slashes (S3-style folder prefix simulation)."""
    key  = "folder/subfolder/nested-file.bin"
    data = b"nested path test"
    r_put = requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r_put.status_code == 201, f"PUT nested key: {r_put.status_code}"

    r_get = requests.get(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
    assert r_get.status_code == 200
    assert r_get.content == data

    requests.delete(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)


# ---------------------------------------------------------------------------
# Binary data
# ---------------------------------------------------------------------------

def test_binary_object_round_trip():
    """All 256 byte values survive the chunking + reassembly pipeline."""
    key  = _rand_key("bin")
    data = bytes(range(256)) * 256   # 64 KB
    r_put = requests.put(
        f"{BASE_URL}/{BUCKET}/{key}",
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r_put.status_code == 201

    r_get = requests.get(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)
    assert r_get.status_code == 200
    assert r_get.content == data, "Binary round-trip mismatch"

    requests.delete(f"{BASE_URL}/{BUCKET}/{key}", timeout=TIMEOUT)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"\n Gateway End-to-End Integration Tests")
    print(f" Target : {BASE_URL}")
    print(f" Time   : {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Pre-check
    print("\n[Pre-check] Verifying gateway is reachable …")
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"  [{FAIL}] Gateway returned {r.status_code} — aborting.")
            return 1
        body = r.json()
        print(f"  [{PASS}] Gateway up — nodes={body.get('nodes')}, chunking={body.get('chunking')!r}")
    except requests.ConnectionError:
        print(
            f"  [{FAIL}] Cannot connect to {BASE_URL}.\n"
            "  Make sure all containers are running: docker compose up --build -d"
        )
        return 1

    section("Health & Ring Stats")
    run("GET /health",                                   test_health)
    run("GET /ring/stats — load distribution table",     test_ring_stats)

    section("PUT /{bucket}/{key}")
    run("PUT small object → 201",                        test_put_small_object)
    run("PUT medium object (6 MiB, ≥2 chunks) → 201",   test_put_medium_object)
    run("PUT empty body → 400",                          test_put_empty_body_400)
    run("PUT response contains all expected fields",     test_put_response_has_all_fields)

    section("GET /{bucket}/{key}")
    run("GET small object — exact byte match",           test_get_small_object_roundtrip)
    run("GET medium object — SHA-256 integrity check",   test_get_medium_object_integrity)
    run("GET response headers present",                  test_get_response_headers)
    run("GET missing object → 404",                      test_get_missing_object_404)
    run("GET preserves content-type",                    test_get_content_type_preserved)

    section("LIST /{bucket}")
    run("LIST contains uploaded objects",                test_list_bucket_contains_uploaded_objects)
    run("LIST entries have required fields",             test_list_object_summary_fields)
    run("LIST empty bucket returns 0 objects",           test_list_empty_bucket)

    section("Deduplication")
    run("Same content, two keys → shared chunk hashes", test_deduplication_same_content_two_objects)

    section("DELETE /{bucket}/{key}")
    run("DELETE existing object → 204",                  test_delete_object_returns_204)
    run("DELETE makes object unavailable (GET → 404)",   test_delete_makes_object_unavailable)
    run("DELETE removes object from bucket listing",     test_delete_removes_from_listing)
    run("DELETE missing object → 404",                   test_delete_missing_object_404)
    run("DELETE respects refcount (shared chunk stays)", test_delete_shared_chunks_not_gc_until_zero_refcount)

    section("Advanced")
    run("Nested key path (folder/subfolder/file)",       test_nested_key_path)
    run("Binary round-trip (all 256 byte values)",       test_binary_object_round_trip)

    # Summary
    total  = len(_results)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = total - passed
    print(f"\n{'═'*60}")
    print(f"  Results: {passed}/{total} passed", end="")
    print(f"  ({failed} failed)" if failed else "")
    print(f"{'═'*60}\n")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
