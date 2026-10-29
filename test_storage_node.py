"""
test_storage_node.py

Integration test script for the Storage Node Service.

Runs against a live node (default: http://localhost:8101).
Override the target with the NODE_URL environment variable:

    NODE_URL=http://localhost:8102 python test_storage_node.py

Requirements:
    pip install requests

No external test framework needed — the script exits with code 0 on full
pass, or code 1 if any test fails.
"""

import hashlib
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL: str = os.getenv("NODE_URL", "http://localhost:8101").rstrip("/")
TIMEOUT: int = 10  # seconds per request

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
INFO = "\033[94mINFO\033[0m"

_results: list[tuple[str, bool, str]] = []


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(name: str, fn) -> bool:
    """Execute *fn*, record pass/fail, print result."""
    try:
        fn()
        _results.append((name, True, ""))
        print(f"  [{PASS}] {name}")
        return True
    except AssertionError as exc:
        msg = str(exc)
        _results.append((name, False, msg))
        print(f"  [{FAIL}] {name}")
        print(f"           {msg}")
        return False
    except Exception as exc:  # pylint: disable=broad-except
        msg = f"{type(exc).__name__}: {exc}"
        _results.append((name, False, msg))
        print(f"  [{FAIL}] {name}")
        print(f"           {msg}")
        return False


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def summary() -> None:
    total = len(_results)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = total - passed
    print(f"\n{'═' * 60}")
    print(f"  Results: {passed}/{total} passed", end="")
    if failed:
        print(f"  ({failed} failed)")
    else:
        print()
    print(f"{'═' * 60}\n")


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

# ── Shared state ────────────────────────────────────────────────────────────
CHUNK_DATA = b"hello distributed storage"
CHUNK_HASH = sha256(CHUNK_DATA)

CHUNK_DATA_2 = b"second chunk payload"
CHUNK_HASH_2 = sha256(CHUNK_DATA_2)

WRONG_HASH = "a" * 64   # valid format, wrong content
BAD_HASH   = "xyz"      # invalid format entirely


# ── Health ───────────────────────────────────────────────────────────────────

def test_health_ok():
    r = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    body = r.json()
    assert "status" in body, "missing 'status' field"
    assert body["status"] == "ok", f"expected status=ok, got {body['status']}"
    assert "node_id" in body, "missing 'node_id' field"


# ── PUT ──────────────────────────────────────────────────────────────────────

def test_put_returns_201():
    r = requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 201, f"expected 201, got {r.status_code}: {r.text}"


def test_put_response_body():
    """PUT response JSON must include hash and size_bytes."""
    r = requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 201
    body = r.json()
    assert body.get("hash") == CHUNK_HASH, f"hash mismatch in response: {body}"
    assert body.get("size_bytes") == len(CHUNK_DATA), f"size mismatch: {body}"


def test_put_idempotent():
    """Storing the same chunk twice must still return 201."""
    for _ in range(2):
        r = requests.put(
            f"{BASE_URL}/chunks/{CHUNK_HASH}",
            data=CHUNK_DATA,
            headers={"Content-Type": "application/octet-stream"},
            timeout=TIMEOUT,
        )
        assert r.status_code == 201, f"expected 201, got {r.status_code}"


def test_put_hash_mismatch_returns_400():
    """Sending data whose hash doesn't match the URL hash → 400."""
    r = requests.put(
        f"{BASE_URL}/chunks/{WRONG_HASH}",
        data=CHUNK_DATA,          # hash of this != WRONG_HASH
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 400, f"expected 400, got {r.status_code}: {r.text}"


def test_put_invalid_hash_format_returns_400():
    """A non-hex or short hash in the URL path → 400."""
    r = requests.put(
        f"{BASE_URL}/chunks/{BAD_HASH}",
        data=b"anything",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


def test_put_empty_body_returns_400():
    """Sending an empty body → 400 (nothing to store)."""
    r = requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=b"",
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


# ── HEAD ─────────────────────────────────────────────────────────────────────

def test_head_existing_returns_200():
    # Ensure the chunk is stored first.
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.head(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"


def test_head_missing_returns_404():
    missing_hash = sha256(b"this chunk was never stored")
    r = requests.head(f"{BASE_URL}/chunks/{missing_hash}", timeout=TIMEOUT)
    assert r.status_code == 404, f"expected 404, got {r.status_code}"


def test_head_no_body():
    """HEAD must not return a response body."""
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.head(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200
    assert len(r.content) == 0, f"HEAD returned a body: {r.content!r}"


def test_head_invalid_hash_returns_400():
    r = requests.head(f"{BASE_URL}/chunks/{BAD_HASH}", timeout=TIMEOUT)
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


# ── GET ──────────────────────────────────────────────────────────────────────

def test_get_returns_correct_data():
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.get(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    assert r.content == CHUNK_DATA, (
        f"data mismatch: expected {CHUNK_DATA!r}, got {r.content!r}"
    )


def test_get_content_type_octet_stream():
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.get(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200
    assert "application/octet-stream" in r.headers.get("content-type", ""), (
        f"unexpected content-type: {r.headers.get('content-type')}"
    )


def test_get_hash_header_present():
    """Response should include X-Content-Hash header matching the requested hash."""
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.get(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200
    assert r.headers.get("x-content-hash") == CHUNK_HASH, (
        f"X-Content-Hash header missing or wrong: {r.headers.get('x-content-hash')}"
    )


def test_get_missing_returns_404():
    missing_hash = sha256(b"never stored chunk abc123")
    r = requests.get(f"{BASE_URL}/chunks/{missing_hash}", timeout=TIMEOUT)
    assert r.status_code == 404, f"expected 404, got {r.status_code}"


def test_get_invalid_hash_returns_400():
    r = requests.get(f"{BASE_URL}/chunks/{BAD_HASH}", timeout=TIMEOUT)
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


def test_get_data_integrity():
    """Re-hash the returned bytes to confirm end-to-end integrity."""
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH}",
        data=CHUNK_DATA,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.get(f"{BASE_URL}/chunks/{CHUNK_HASH}", timeout=TIMEOUT)
    assert r.status_code == 200
    returned_hash = sha256(r.content)
    assert returned_hash == CHUNK_HASH, (
        f"integrity check failed: sha256(response)={returned_hash}, expected={CHUNK_HASH}"
    )


# ── DELETE ───────────────────────────────────────────────────────────────────

def test_delete_existing_returns_204():
    # Store a fresh chunk specifically for this test.
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH_2}",
        data=CHUNK_DATA_2,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r = requests.delete(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)
    assert r.status_code == 204, f"expected 204, got {r.status_code}: {r.text}"


def test_delete_makes_chunk_unavailable():
    """After DELETE, GET and HEAD must both return 404."""
    # Store then delete.
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH_2}",
        data=CHUNK_DATA_2,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    requests.delete(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)

    r_get = requests.get(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)
    assert r_get.status_code == 404, (
        f"GET after DELETE: expected 404, got {r_get.status_code}"
    )

    r_head = requests.head(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)
    assert r_head.status_code == 404, (
        f"HEAD after DELETE: expected 404, got {r_head.status_code}"
    )


def test_delete_missing_returns_404():
    missing_hash = sha256(b"chunk that does not exist at all")
    r = requests.delete(f"{BASE_URL}/chunks/{missing_hash}", timeout=TIMEOUT)
    assert r.status_code == 404, f"expected 404, got {r.status_code}"


def test_delete_invalid_hash_returns_400():
    r = requests.delete(f"{BASE_URL}/chunks/{BAD_HASH}", timeout=TIMEOUT)
    assert r.status_code == 400, f"expected 400, got {r.status_code}"


def test_delete_idempotent_second_call_is_404():
    """Deleting the same chunk twice: first call 204, second call 404."""
    requests.put(
        f"{BASE_URL}/chunks/{CHUNK_HASH_2}",
        data=CHUNK_DATA_2,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    r1 = requests.delete(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)
    assert r1.status_code == 204, f"first DELETE: expected 204, got {r1.status_code}"

    r2 = requests.delete(f"{BASE_URL}/chunks/{CHUNK_HASH_2}", timeout=TIMEOUT)
    assert r2.status_code == 404, f"second DELETE: expected 404, got {r2.status_code}"


# ── Binary data ──────────────────────────────────────────────────────────────

def test_binary_chunk_round_trip():
    """Store and retrieve non-text binary data to verify no encoding issues."""
    binary_data = bytes(range(256)) * 64   # 16 KB of every possible byte value
    binary_hash = sha256(binary_data)

    r_put = requests.put(
        f"{BASE_URL}/chunks/{binary_hash}",
        data=binary_data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=TIMEOUT,
    )
    assert r_put.status_code == 201, f"PUT binary: {r_put.status_code}"

    r_get = requests.get(f"{BASE_URL}/chunks/{binary_hash}", timeout=TIMEOUT)
    assert r_get.status_code == 200
    assert r_get.content == binary_data, "binary round-trip data mismatch"

    # Cleanup.
    requests.delete(f"{BASE_URL}/chunks/{binary_hash}", timeout=TIMEOUT)


def test_large_chunk_round_trip():
    """Store and retrieve a 1 MB chunk."""
    large_data = os.urandom(1024 * 1024)   # 1 MB of random bytes
    large_hash = sha256(large_data)

    r_put = requests.put(
        f"{BASE_URL}/chunks/{large_hash}",
        data=large_data,
        headers={"Content-Type": "application/octet-stream"},
        timeout=30,
    )
    assert r_put.status_code == 201, f"PUT large: {r_put.status_code}"

    r_get = requests.get(f"{BASE_URL}/chunks/{large_hash}", timeout=30)
    assert r_get.status_code == 200
    assert sha256(r_get.content) == large_hash, "large chunk integrity check failed"

    # Cleanup.
    requests.delete(f"{BASE_URL}/chunks/{large_hash}", timeout=TIMEOUT)


# ── Multi-node smoke test ────────────────────────────────────────────────────

def test_all_nodes_reachable():
    """
    Verify all 4 storage nodes respond to /health.
    Each node runs on a different host port (8101–8104).
    """
    ports = [8101, 8102, 8103, 8104]
    for port in ports:
        url = f"http://localhost:{port}/health"
        try:
            r = requests.get(url, timeout=TIMEOUT)
            assert r.status_code == 200, f"node:{port} → {r.status_code}"
            body = r.json()
            assert body.get("status") == "ok", f"node:{port} status={body}"
            print(f"    [{INFO}] node:{port} node_id={body.get('node_id')!r}")
        except requests.ConnectionError:
            raise AssertionError(
                f"Could not reach node on port {port}. "
                "Is docker compose up and running?"
            )


def test_stats_endpoint():
    """GET /stats must return node_id, chunk_count, free_space_bytes, free_space_gb."""
    r = requests.get(f"{BASE_URL}/stats", timeout=TIMEOUT)
    assert r.status_code == 200, f"expected 200, got {r.status_code}"
    body = r.json()
    for field in ("node_id", "chunk_count", "free_space_bytes", "free_space_gb", "storage_dir"):
        assert field in body, f"missing field {field!r} in /stats response"
    assert isinstance(body["chunk_count"], int),       "chunk_count must be an int"
    assert isinstance(body["free_space_bytes"], int),  "free_space_bytes must be an int"
    assert isinstance(body["free_space_gb"], float),   "free_space_gb must be a float"
    assert body["free_space_bytes"] > 0,               "free_space_bytes should be > 0"


def print_cluster_stats():
    """Print a live stats table for all 4 nodes — not a pass/fail test."""
    ports = [8101, 8102, 8103, 8104]
    rows = []
    for port in ports:
        try:
            r = requests.get(f"http://localhost:{port}/stats", timeout=TIMEOUT)
            if r.status_code == 200:
                rows.append(r.json())
            else:
                rows.append({"node_id": f"port:{port}", "chunk_count": "ERR",
                              "free_space_gb": "ERR", "free_space_bytes": "ERR"})
        except requests.ConnectionError:
            rows.append({"node_id": f"port:{port}", "chunk_count": "DOWN",
                         "free_space_gb": "DOWN", "free_space_bytes": "DOWN"})

    col_w = [10, 8, 14, 16]
    header = (
        f"  {'node_id':<{col_w[0]}}  {'chunks':<{col_w[1]}}"
        f"  {'free (GiB)':<{col_w[2]}}  {'free (bytes)':<{col_w[3]}}"
    )
    sep = "  " + "─" * (sum(col_w) + 3 * 2)
    print(sep)
    print(header)
    print(sep)
    for row in rows:
        print(
            f"  {str(row.get('node_id','?')):<{col_w[0]}}"
            f"  {str(row.get('chunk_count','?')):<{col_w[1]}}"
            f"  {str(row.get('free_space_gb','?')):<{col_w[2]}}"
            f"  {str(row.get('free_space_bytes','?')):<{col_w[3]}}"
        )
    print(sep)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"\n Storage Node Integration Tests")
    print(f" Target: {BASE_URL}")
    print(f" Time:   {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # ── Connectivity pre-check ──────────────────────────────────────────────
    print("\n[Pre-check] Verifying node is reachable …")
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"  [{FAIL}] Node returned {r.status_code} on /health — aborting.")
            return 1
        print(f"  [{PASS}] Node is up (node_id={r.json().get('node_id')!r})")
    except requests.ConnectionError:
        print(
            f"  [{FAIL}] Cannot connect to {BASE_URL}.\n"
            "  Make sure the containers are running: docker compose up --build"
        )
        return 1

    # ── Test suites ─────────────────────────────────────────────────────────
    section("Health Endpoint")
    run("GET /health returns 200 with status=ok", test_health_ok)

    section("PUT /chunks/{hash}")
    run("PUT valid chunk → 201",                       test_put_returns_201)
    run("PUT response body contains hash + size",      test_put_response_body)
    run("PUT same chunk twice → 201 (idempotent)",     test_put_idempotent)
    run("PUT hash mismatch → 400",                     test_put_hash_mismatch_returns_400)
    run("PUT invalid hash format → 400",               test_put_invalid_hash_format_returns_400)
    run("PUT empty body → 400",                        test_put_empty_body_returns_400)

    section("HEAD /chunks/{hash}")
    run("HEAD existing chunk → 200",                   test_head_existing_returns_200)
    run("HEAD missing chunk → 404",                    test_head_missing_returns_404)
    run("HEAD returns no body",                        test_head_no_body)
    run("HEAD invalid hash format → 400",              test_head_invalid_hash_returns_400)

    section("GET /chunks/{hash}")
    run("GET returns correct binary data",             test_get_returns_correct_data)
    run("GET content-type is octet-stream",            test_get_content_type_octet_stream)
    run("GET includes X-Content-Hash header",          test_get_hash_header_present)
    run("GET missing chunk → 404",                     test_get_missing_returns_404)
    run("GET invalid hash format → 400",               test_get_invalid_hash_returns_400)
    run("GET end-to-end SHA-256 integrity",            test_get_data_integrity)

    section("DELETE /chunks/{hash}")
    run("DELETE existing chunk → 204",                 test_delete_existing_returns_204)
    run("DELETE makes chunk unreachable (404)",        test_delete_makes_chunk_unavailable)
    run("DELETE missing chunk → 404",                  test_delete_missing_returns_404)
    run("DELETE invalid hash format → 400",            test_delete_invalid_hash_returns_400)
    run("DELETE second call → 404",                    test_delete_idempotent_second_call_is_404)

    section("Binary & Large Data")
    run("Binary round-trip (all 256 byte values)",     test_binary_chunk_round_trip)
    run("Large chunk round-trip (1 MB)",               test_large_chunk_round_trip)

    section("Multi-Node Smoke Test")
    run("All 4 nodes reachable on ports 8101–8104",    test_all_nodes_reachable)
    run("GET /stats returns all fields",               test_stats_endpoint)

    section("Live Cluster Stats")
    print_cluster_stats()

    # ── Summary ─────────────────────────────────────────────────────────────
    summary()

    failed = sum(1 for _, ok, _ in _results if not ok)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
