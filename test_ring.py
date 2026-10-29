"""
test_ring.py

Standalone tests for the Consistent Hash Ring (ring.py).

Tests
-----
1.  Basic node addition / lookup
2.  Load distribution uniformity across N nodes
3.  Node addition migration — verify ~1/N keys migrate
4.  Node removal migration  — verify ~1/(N-1) keys migrate back
5.  Replication (get_nodes returns distinct nodes)
6.  Ring wraps around (clockwise successor works at ring boundary)
7.  Empty ring raises RuntimeError
8.  Re-adding a node (URL update) works correctly
9.  Removing a non-existent node raises KeyError
10. simulate_migration() utility consistency checks

Run:
    python test_ring.py
"""

import sys
import math

# Ensure the gateway directory is on the path
sys.path.insert(0, "gateway")

from ring import (
    ConsistentHashRing,
    simulate_migration,
    load_distribution,
    _generate_sample_hashes,
)

# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

_results: list[tuple[str, bool, str]] = []


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
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

NODES = {
    "node-1": "http://storage_node1:8000",
    "node-2": "http://storage_node2:8000",
    "node-3": "http://storage_node3:8000",
    "node-4": "http://storage_node4:8000",
}
SAMPLE_SIZE  = 100_000
VNODE_COUNT  = 150
TOLERANCE    = 0.05  # ±5 % acceptable deviation from theoretical expectation


def _make_ring(nodes: dict = NODES, vnodes: int = VNODE_COUNT) -> ConsistentHashRing:
    r = ConsistentHashRing(vnode_count=vnodes)
    for nid, url in nodes.items():
        r.add_node(nid, url)
    return r


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_basic_lookup():
    """A chunk hash should always resolve to the same node (determinism)."""
    r = _make_ring()
    sample = _generate_sample_hashes(100)
    for h in sample:
        n1 = r.get_node(h)
        n2 = r.get_node(h)
        assert n1.node_id == n2.node_id, f"Non-deterministic lookup for {h[:12]}"
        assert n1.node_id in NODES, f"Resolved to unknown node {n1.node_id!r}"


def test_all_nodes_receive_load():
    """Every registered node should own at least some keys."""
    r = _make_ring()
    sample = _generate_sample_hashes(SAMPLE_SIZE)
    dist = load_distribution(r, sample)
    for nid in NODES:
        assert dist[nid] > 0, f"Node {nid!r} received zero keys — ring is degenerate."


def test_load_balance_uniformity():
    """
    Each node should own approximately 1/N of the key space.
    Allow ±TOLERANCE relative deviation from the expected share.
    """
    r = _make_ring()
    n = len(NODES)
    expected = SAMPLE_SIZE / n
    sample = _generate_sample_hashes(SAMPLE_SIZE)
    dist = load_distribution(r, sample)

    for nid, count in dist.items():
        deviation = abs(count - expected) / expected
        assert deviation <= TOLERANCE, (
            f"Node {nid!r} owns {count} keys ({100*count/SAMPLE_SIZE:.1f}%), "
            f"expected ~{expected:.0f} (±{TOLERANCE*100:.0f}%). "
            f"Increase vnode_count to improve balance."
        )

    # Print distribution table for visibility
    print()
    print(f"    {'Node':<12} {'Keys':>8} {'%':>7}")
    print(f"    {'─'*12} {'─'*8} {'─'*7}")
    for nid, count in sorted(dist.items()):
        print(f"    {nid:<12} {count:>8,} {100*count/SAMPLE_SIZE:>6.2f}%")


def test_node_addition_migration():
    """
    Adding one node to an N-node ring should migrate approximately 1/(N+1)
    of the keys to the new node, leaving the rest undisturbed.
    """
    old_ring = _make_ring()
    new_ring = _make_ring()
    new_node_id = "node-5"
    new_ring.add_node(new_node_id, "http://storage_node5:8000")

    sample = _generate_sample_hashes(SAMPLE_SIZE)
    stats = simulate_migration(old_ring, new_ring, sample)

    n_old = len(NODES)
    expected_pct = 100 / (n_old + 1)           # ~20 % for 4→5 nodes
    actual_pct   = stats["pct_migrated"]

    print(
        f"\n    Node addition  ({n_old} → {n_old+1} nodes): "
        f"migrated={actual_pct:.2f}%  expected≈{expected_pct:.2f}%"
    )

    # Allow ±2× tolerance around theoretical expectation
    assert actual_pct <= expected_pct * (1 + TOLERANCE * 4), (
        f"Too many keys migrated on addition: {actual_pct:.2f}% > {expected_pct*(1+TOLERANCE*4):.2f}%"
    )
    assert actual_pct >= expected_pct * (1 - TOLERANCE * 4), (
        f"Too few keys migrated on addition: {actual_pct:.2f}% < {expected_pct*(1-TOLERANCE*4):.2f}%"
    )


def test_node_removal_migration():
    """
    Removing one node from an N-node ring should migrate approximately
    1/N of the keys to surviving nodes, leaving the rest undisturbed.
    """
    old_ring = _make_ring()
    new_ring = _make_ring()
    removed  = "node-4"
    new_ring.remove_node(removed)

    sample = _generate_sample_hashes(SAMPLE_SIZE)
    stats = simulate_migration(old_ring, new_ring, sample)

    n_old = len(NODES)
    expected_pct = 100 / n_old                  # ~25 % for 4→3 nodes
    actual_pct   = stats["pct_migrated"]

    print(
        f"\n    Node removal   ({n_old} → {n_old-1} nodes): "
        f"migrated={actual_pct:.2f}%  expected≈{expected_pct:.2f}%"
    )

    assert actual_pct <= expected_pct * (1 + TOLERANCE * 4), (
        f"Too many keys migrated on removal: {actual_pct:.2f}%"
    )
    assert actual_pct >= expected_pct * (1 - TOLERANCE * 4), (
        f"Too few keys migrated on removal: {actual_pct:.2f}%"
    )


def test_replication_get_nodes_distinct():
    """get_nodes(hash, n) must return n distinct physical nodes."""
    r = _make_ring()
    sample = _generate_sample_hashes(1_000)
    for h in sample:
        replicas = r.get_nodes(h, 3)
        ids = [n.node_id for n in replicas]
        assert len(ids) == len(set(ids)), (
            f"Duplicate nodes in replica list for {h[:12]}: {ids}"
        )
        assert len(ids) == 3, f"Expected 3 replicas, got {len(ids)}"


def test_replication_capped_at_node_count():
    """Requesting more replicas than nodes returns at most node_count."""
    r = _make_ring({"node-1": "http://n1:8000", "node-2": "http://n2:8000"})
    replicas = r.get_nodes(_generate_sample_hashes(1)[0], count=10)
    assert len(replicas) == 2, f"Expected 2 (capped), got {len(replicas)}"


def test_single_node_ring():
    """A ring with one node should always return that node."""
    r = ConsistentHashRing(vnode_count=50)
    r.add_node("solo", "http://solo:8000")
    sample = _generate_sample_hashes(500)
    for h in sample:
        assert r.get_node(h).node_id == "solo"


def test_empty_ring_raises():
    """get_node on an empty ring must raise RuntimeError."""
    r = ConsistentHashRing()
    try:
        r.get_node("a" * 64)
        assert False, "Should have raised RuntimeError"
    except RuntimeError:
        pass


def test_remove_nonexistent_raises():
    """Removing a node that was never added must raise KeyError."""
    r = _make_ring()
    try:
        r.remove_node("ghost-node")
        assert False, "Should have raised KeyError"
    except KeyError:
        pass


def test_readd_node_updates_url():
    """Re-adding an existing node with a new URL should update correctly."""
    r = _make_ring()
    new_url = "http://new-address:9999"
    r.add_node("node-1", new_url)
    info = r.get_nodes(_generate_sample_hashes(5000)[0], 4)
    # The URL for node-1 should now be the updated one
    for n in info:
        if n.node_id == "node-1":
            assert n.url == new_url, f"URL not updated: {n.url!r}"


def test_simulate_migration_totals():
    """simulate_migration must report migrated + stayed == total."""
    old_ring = _make_ring()
    new_ring = _make_ring()
    new_ring.add_node("node-5", "http://node5:8000")
    sample = _generate_sample_hashes(10_000)
    stats = simulate_migration(old_ring, new_ring, sample)
    assert stats["total"] == len(sample)
    assert stats["migrated"] + stats["stayed"] == stats["total"]
    assert abs(stats["pct_migrated"] + stats["pct_stayed"] - 100.0) < 0.01


def test_consistent_hashing_stability():
    """
    Keys not owned by a removed node must stay on the same node (the whole
    point of consistent hashing).
    """
    old_ring = _make_ring()
    new_ring = _make_ring()
    removed  = "node-3"
    new_ring.remove_node(removed)

    sample = _generate_sample_hashes(50_000)
    stable = failed = 0

    for h in sample:
        old_node = old_ring.get_node(h).node_id
        new_node = new_ring.get_node(h).node_id
        if old_node != removed:
            if old_node == new_node:
                stable += 1
            else:
                failed += 1

    total_non_removed = stable + failed
    if total_non_removed > 0:
        pct_disrupted = 100 * failed / total_non_removed
        print(
            f"\n    Stability check: {pct_disrupted:.2f}% of non-removed-node "
            f"keys changed owner (should be 0%)"
        )
        assert failed == 0, (
            f"{failed}/{total_non_removed} keys that were NOT on {removed!r} "
            f"still changed owner — consistent hashing invariant violated!"
        )


def test_high_vnode_count_improves_balance():
    """
    A ring with 300 vnodes per node should be at least as balanced
    as one with 50 vnodes per node.
    """
    def _imbalance(ring: ConsistentHashRing) -> float:
        sample = _generate_sample_hashes(50_000)
        dist = load_distribution(ring, sample)
        counts = list(dist.values())
        mean = sum(counts) / len(counts)
        return max(abs(c - mean) / mean for c in counts)

    low_ring  = _make_ring(vnodes=50)
    high_ring = _make_ring(vnodes=300)

    low_imb  = _imbalance(low_ring)
    high_imb = _imbalance(high_ring)

    print(f"\n    Imbalance — 50  vnodes: {low_imb*100:.2f}%")
    print(f"    Imbalance — 300 vnodes: {high_imb*100:.2f}%")

    assert high_imb <= low_imb + 0.02, (
        "Higher vnode count did not improve balance "
        f"(low={low_imb:.4f}, high={high_imb:.4f})"
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main() -> int:
    print("\n Consistent Hash Ring Tests")
    print(f" Sample size : {SAMPLE_SIZE:,}")
    print(f" Vnode count : {VNODE_COUNT} per node")
    print(f" Nodes       : {list(NODES)}")

    section("Core Lookup")
    run("Deterministic lookup",                      test_basic_lookup)
    run("All nodes receive load",                    test_all_nodes_receive_load)
    run("Load balance uniformity (±5%)",             test_load_balance_uniformity)

    section("Topology Changes & Migration")
    run("Node addition migration (≈1/(N+1) keys)",  test_node_addition_migration)
    run("Node removal migration (≈1/N keys)",        test_node_removal_migration)
    run("Consistent hashing stability invariant",    test_consistent_hashing_stability)

    section("Replication")
    run("get_nodes returns distinct nodes",          test_replication_get_nodes_distinct)
    run("Replica count capped at node_count",        test_replication_capped_at_node_count)

    section("Edge Cases")
    run("Single-node ring",                          test_single_node_ring)
    run("Empty ring raises RuntimeError",            test_empty_ring_raises)
    run("Remove non-existent node raises KeyError",  test_remove_nonexistent_raises)
    run("Re-adding node updates URL",                test_readd_node_updates_url)

    section("Utilities")
    run("simulate_migration totals add up",          test_simulate_migration_totals)
    run("Higher vnode count improves balance",       test_high_vnode_count_improves_balance)

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
