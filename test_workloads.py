"""
test_workloads.py

Two distinct workload benchmarks for the distributed storage gateway:

  Workload A — Many small files
      Simulates a backup/sync workload: 200 files of 4–64 KB each.
      Measures upload throughput, download round-trip, and listing latency.

  Workload B — Few large files
      Simulates a media/archive workload: 5 files of 16–64 MB each.
      Measures upload throughput, download integrity, and per-chunk behaviour.

Usage:
    python test_workloads.py              # run both
    python test_workloads.py --workload a # run only A
    python test_workloads.py --workload b # run only B

Environment:
    GATEWAY_URL   (default: http://localhost:9000)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import string
import sys
import time
from dataclasses import dataclass, field

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL: str = os.getenv("GATEWAY_URL", "http://localhost:9000").rstrip("/")
TIMEOUT: int = 120

# Workload A: many small files
A_FILE_COUNT:  int = 200
A_MIN_SIZE_KB:  int = 4
A_MAX_SIZE_KB:  int = 64
A_BUCKET:       str = "workload-a"

# Workload B: few large files
B_FILE_COUNT:  int = 5
B_MIN_SIZE_MB:  int = 16
B_MAX_SIZE_MB:  int = 64
B_BUCKET:       str = "workload-b"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------

CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def _fmt_ms(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    return f"{seconds:.2f} s"


def _rand_key(prefix: str) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}-{suffix}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class UploadResult:
    key:        str
    size:       int
    duration:   float
    chunk_count: int
    success:    bool
    error:      str = ""


@dataclass
class DownloadResult:
    key:     str
    size:    int
    duration: float
    success: bool
    error:   str = ""


@dataclass
class WorkloadReport:
    name:         str
    file_count:   int
    total_bytes:  int
    uploads:       list[UploadResult] = field(default_factory=list)
    downloads:     list[DownloadResult] = field(default_factory=list)
    list_duration: float = 0.0
    list_count:    int = 0
    delete_duration: float = 0.0
    delete_count:  int = 0


# ---------------------------------------------------------------------------
# Workload A — Many small files
# ---------------------------------------------------------------------------

def run_workload_a() -> WorkloadReport:
    """
    Upload N small files, download them all back, list the bucket,
    then delete everything.  Measures end-to-end throughput.
    """
    report = WorkloadReport(name="A (many small files)", file_count=A_FILE_COUNT, total_bytes=0)

    print(f"\n{BOLD}{CYAN}{'═' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  Workload A — Many Small Files{RESET}")
    print(f"  {A_FILE_COUNT} files, {_fmt_bytes(A_MIN_SIZE_KB * 1024)} – {_fmt_bytes(A_MAX_SIZE_KB * 1024)} each")
    print(f"{BOLD}{CYAN}{'═' * 60}{RESET}")

    # ── Generate payloads ────────────────────────────────────────────────
    payloads: list[tuple[str, bytes]] = []
    for i in range(A_FILE_COUNT):
        size_kb = random.randint(A_MIN_SIZE_KB, A_MAX_SIZE_KB)
        data = os.urandom(size_kb * 1024)
        key = _rand_key(f"small/{i:04d}")
        payloads.append((key, data))
        report.total_bytes += len(data)

    print(f"\n  Total data: {_fmt_bytes(report.total_bytes)} across {A_FILE_COUNT} files")

    # ── Upload ────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 1 — Upload{RESET}")
    print(f"  {'─' * 56}")
    t0 = time.perf_counter()
    for idx, (key, data) in enumerate(payloads):
        t_put = time.perf_counter()
        try:
            r = requests.put(
                f"{BASE_URL}/{A_BUCKET}/{key}",
                data=data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=TIMEOUT,
            )
            dur = time.perf_counter() - t_put
            ok = r.status_code == 201
            chunks = r.json().get("chunk_count", 0) if ok else 0
            err = "" if ok else f"HTTP {r.status_code}: {r.text[:100]}"
            report.uploads.append(UploadResult(key, len(data), dur, chunks, ok, err))
        except Exception as exc:
            dur = time.perf_counter() - t_put
            report.uploads.append(UploadResult(key, len(data), dur, 0, False, str(exc)))

        if (idx + 1) % 50 == 0 or idx == 0:
            print(f"    [{idx + 1:>3}/{A_FILE_COUNT}] uploaded …")
    upload_total = time.perf_counter() - t0
    up_ok = sum(1 for u in report.uploads if u.success)

    print(f"  {'─' * 56}")
    print(f"  Uploaded: {up_ok}/{A_FILE_COUNT} in {_fmt_ms(upload_total)}")
    print(f"  Throughput: {_fmt_bytes(int(report.total_bytes / upload_total)) if upload_total > 0 else 'N/A'}/s")

    # ── Download ──────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 2 — Download & Verify{RESET}")
    print(f"  {'─' * 56}")
    t0 = time.perf_counter()
    mismatches = 0
    for idx, (key, original) in enumerate(payloads):
        t_get = time.perf_counter()
        try:
            r = requests.get(f"{BASE_URL}/{A_BUCKET}/{key}", timeout=TIMEOUT)
            dur = time.perf_counter() - t_get
            ok = r.status_code == 200 and r.content == original
            if r.status_code == 200 and r.content != original:
                mismatches += 1
            report.downloads.append(DownloadResult(key, len(r.content if r.status_code == 200 else b""), dur, ok,
                                                    "" if ok else f"HTTP {r.status_code}"))
        except Exception as exc:
            t_dur = time.perf_counter() - t_get
            report.downloads.append(DownloadResult(key, 0, t_dur, False, str(exc)))

        if (idx + 1) % 50 == 0 or idx == 0:
            print(f"    [{idx + 1:>3}/{A_FILE_COUNT}] downloaded & verified …")
    dl_total = time.perf_counter() - t0
    dl_ok = sum(1 for d in report.downloads if d.success)

    print(f"  {'─' * 56}")
    print(f"  Downloaded: {dl_ok}/{A_FILE_COUNT} in {_fmt_ms(dl_total)}")
    print(f"  Throughput: {_fmt_bytes(int(report.total_bytes / dl_total)) if dl_total > 0 else 'N/A'}/s")
    if mismatches:
        print(f"  {RED}Integrity mismatches: {mismatches}{RESET}")

    # ── List ──────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 3 — List Bucket{RESET}")
    t0 = time.perf_counter()
    try:
        r = requests.get(f"{BASE_URL}/{A_BUCKET}", timeout=TIMEOUT)
        report.list_duration = time.perf_counter() - t0
        report.list_count = r.json().get("object_count", 0) if r.status_code == 200 else 0
        ok = r.status_code == 200
    except Exception:
        report.list_duration = time.perf_counter() - t0
        ok = False
    print(f"  Listed {report.list_count} objects in {_fmt_ms(report.list_duration)}  "
          f"{'✓' if ok else '✗'}")

    # ── Delete ────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 4 — Delete All{RESET}")
    t0 = time.perf_counter()
    del_ok = 0
    for key, _ in payloads:
        try:
            r = requests.delete(f"{BASE_URL}/{A_BUCKET}/{key}", timeout=TIMEOUT)
            if r.status_code == 204:
                del_ok += 1
        except Exception:
            pass
    report.delete_duration = time.perf_counter() - t0
    report.delete_count = del_ok
    print(f"  Deleted {del_ok}/{A_FILE_COUNT} in {_fmt_ms(report.delete_duration)}")

    # ── Summary ──────────────────────────────────────────────────────────
    _print_summary(report, upload_total, dl_total)

    return report


# ---------------------------------------------------------------------------
# Workload B — Few large files
# ---------------------------------------------------------------------------

def run_workload_b() -> WorkloadReport:
    """
    Upload N large files (16–64 MB), download and verify integrity,
    list the bucket, then delete everything.
    """
    report = WorkloadReport(name="B (few large files)", file_count=B_FILE_COUNT, total_bytes=0)

    print(f"\n{BOLD}{CYAN}{'═' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  Workload B — Few Large Files{RESET}")
    print(f"  {B_FILE_COUNT} files, {B_MIN_SIZE_MB} – {B_MAX_SIZE_MB} MB each")
    print(f"{BOLD}{CYAN}{'═' * 60}{RESET}")

    # ── Generate payloads ────────────────────────────────────────────────
    payloads: list[tuple[str, bytes]] = []
    for i in range(B_FILE_COUNT):
        size_mb = random.randint(B_MIN_SIZE_MB, B_MAX_SIZE_MB)
        data = os.urandom(size_mb * 1024 * 1024)
        key = _rand_key(f"large/{i:04d}")
        payloads.append((key, data))
        report.total_bytes += len(data)

    print(f"\n  Total data: {_fmt_bytes(report.total_bytes)} across {B_FILE_COUNT} files")

    # ── Upload ────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 1 — Upload{RESET}")
    print(f"  {'─' * 56}")
    t0 = time.perf_counter()
    for idx, (key, data) in enumerate(payloads):
        t_put = time.perf_counter()
        try:
            r = requests.put(
                f"{BASE_URL}/{B_BUCKET}/{key}",
                data=data,
                headers={"Content-Type": "application/octet-stream"},
                timeout=TIMEOUT,
            )
            dur = time.perf_counter() - t_put
            ok = r.status_code == 201
            body = r.json() if ok else {}
            chunks = body.get("chunk_count", 0)
            err = "" if ok else f"HTTP {r.status_code}: {r.text[:100]}"
            report.uploads.append(UploadResult(key, len(data), dur, chunks, ok, err))
            print(f"    [{idx + 1}/{B_FILE_COUNT}] {key}  "
                  f"{_fmt_bytes(len(data))} → {chunks} chunks  "
                  f"{_fmt_ms(dur)}  {'✓' if ok else '✗ ' + err}")
        except Exception as exc:
            dur = time.perf_counter() - t_put
            report.uploads.append(UploadResult(key, len(data), dur, 0, False, str(exc)))
            print(f"    [{idx + 1}/{B_FILE_COUNT}] {key}  FAILED: {exc}")
    upload_total = time.perf_counter() - t0
    up_ok = sum(1 for u in report.uploads if u.success)

    print(f"  {'─' * 56}")
    print(f"  Uploaded: {up_ok}/{B_FILE_COUNT} in {_fmt_ms(upload_total)}")
    if upload_total > 0:
        print(f"  Throughput: {_fmt_bytes(int(report.total_bytes / upload_total))}/s")

    # ── Download & Verify ─────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 2 — Download & SHA-256 Verify{RESET}")
    print(f"  {'─' * 56}")
    t0 = time.perf_counter()
    mismatches = 0
    for idx, (key, original) in enumerate(payloads):
        t_get = time.perf_counter()
        try:
            r = requests.get(f"{BASE_URL}/{B_BUCKET}/{key}", timeout=TIMEOUT)
            dur = time.perf_counter() - t_get
            ok = r.status_code == 200
            integrity_ok = False
            if ok:
                integrity_ok = _sha256(r.content) == _sha256(original)
                if not integrity_ok:
                    mismatches += 1
            report.downloads.append(
                DownloadResult(key, len(r.content) if ok else 0, dur, ok and integrity_ok,
                                "" if ok and integrity_ok else f"HTTP {r.status_code}" + (" — integrity FAIL" if ok and not integrity_ok else ""))
            )
            print(f"    [{idx + 1}/{B_FILE_COUNT}] {key}  "
                  f"{_fmt_bytes(len(r.content) if ok else 0)}  "
                  f"{_fmt_ms(dur)}  "
                  f"{'✓' if ok and integrity_ok else '✗'}")
        except Exception as exc:
            t_dur = time.perf_counter() - t_get
            report.downloads.append(DownloadResult(key, 0, t_dur, False, str(exc)))
            print(f"    [{idx + 1}/{B_FILE_COUNT}] {key}  FAILED: {exc}")
    dl_total = time.perf_counter() - t0
    dl_ok = sum(1 for d in report.downloads if d.success)

    print(f"  {'─' * 56}")
    print(f"  Downloaded: {dl_ok}/{B_FILE_COUNT} in {_fmt_ms(dl_total)}")
    if dl_total > 0:
        print(f"  Throughput: {_fmt_bytes(int(report.total_bytes / dl_total))}/s")
    if mismatches:
        print(f"  {RED}Integrity mismatches: {mismatches}{RESET}")
    else:
        print(f"  {GREEN}All SHA-256 integrity checks passed.{RESET}")

    # ── List ──────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 3 — List Bucket{RESET}")
    t0 = time.perf_counter()
    try:
        r = requests.get(f"{BASE_URL}/{B_BUCKET}", timeout=TIMEOUT)
        report.list_duration = time.perf_counter() - t0
        report.list_count = r.json().get("object_count", 0) if r.status_code == 200 else 0
        ok = r.status_code == 200
    except Exception:
        report.list_duration = time.perf_counter() - t0
        ok = False
    print(f"  Listed {report.list_count} objects in {_fmt_ms(report.list_duration)}  "
          f"{'✓' if ok else '✗'}")

    # ── Delete ────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Phase 4 — Delete All{RESET}")
    t0 = time.perf_counter()
    del_ok = 0
    for key, _ in payloads:
        try:
            r = requests.delete(f"{BASE_URL}/{B_BUCKET}/{key}", timeout=TIMEOUT)
            if r.status_code == 204:
                del_ok += 1
        except Exception:
            pass
    report.delete_duration = time.perf_counter() - t0
    report.delete_count = del_ok
    print(f"  Deleted {del_ok}/{B_FILE_COUNT} in {_fmt_ms(report.delete_duration)}")

    # ── Summary ──────────────────────────────────────────────────────────
    _print_summary(report, upload_total, dl_total)

    return report


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(report: WorkloadReport, upload_total: float, dl_total: float) -> None:
    print(f"\n  {BOLD}{'─' * 56}{RESET}")
    print(f"  {BOLD}Summary — Workload {report.name}{RESET}")
    print(f"  {BOLD}{'─' * 56}{RESET}")
    print(f"    Files            : {report.file_count}")
    print(f"    Total data       : {_fmt_bytes(report.total_bytes)}")

    up_ok = sum(1 for u in report.uploads if u.success)
    up_bytes = sum(u.size for u in report.uploads if u.success)
    avg_up = sum(u.duration for u in report.uploads) / max(len(report.uploads), 1)
    total_chunks = sum(u.chunk_count for u in report.uploads if u.success)
    print(f"    Uploads          : {up_ok}/{report.file_count} ok")
    print(f"    Upload time      : {_fmt_ms(upload_total)}")
    print(f"    Avg upload/file  : {_fmt_ms(avg_up)}")
    print(f"    Upload throughput: {_fmt_bytes(int(up_bytes / upload_total)) if upload_total > 0 else 'N/A'}/s")
    print(f"    Total chunks     : {total_chunks}")
    if report.uploads:
        avg_chunks = total_chunks / max(up_ok, 1)
        print(f"    Avg chunks/file  : {avg_chunks:.1f}")

    dl_ok = sum(1 for d in report.downloads if d.success)
    dl_bytes = sum(d.size for d in report.downloads if d.success)
    avg_dl = sum(d.duration for d in report.downloads) / max(len(report.downloads), 1)
    print(f"    Downloads        : {dl_ok}/{report.file_count} ok")
    print(f"    Download time    : {_fmt_ms(dl_total)}")
    print(f"    Avg download/file: {_fmt_ms(avg_dl)}")
    print(f"    Download thruput : {_fmt_bytes(int(dl_bytes / dl_total)) if dl_total > 0 else 'N/A'}/s")

    print(f"    List bucket      : {report.list_count} objects in {_fmt_ms(report.list_duration)}")
    print(f"    Delete           : {report.delete_count}/{report.file_count} in {_fmt_ms(report.delete_duration)}")

    # Per-file size distribution
    if report.uploads:
        sizes = [u.size for u in report.uploads]
        print(f"    File size range  : {_fmt_bytes(min(sizes))} – {_fmt_bytes(max(sizes))}")

    print(f"  {BOLD}{'─' * 56}{RESET}")


# ---------------------------------------------------------------------------
# Combined comparison
# ---------------------------------------------------------------------------

def _print_comparison(a: WorkloadReport | None, b: WorkloadReport | None) -> None:
    if not a or not b:
        return
    print(f"\n{BOLD}{CYAN}{'═' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  Workload Comparison{RESET}")
    print(f"{BOLD}{CYAN}{'═' * 60}{RESET}")
    print(f"  {'Metric':<24} {'Workload A':>16} {'Workload B':>16}")
    print(f"  {'─' * 56}")
    print(f"  {'Files':<24} {a.file_count:>16} {b.file_count:>16}")
    print(f"  {'Total data':<24} {_fmt_bytes(a.total_bytes):>16} {_fmt_bytes(b.total_bytes):>16}")

    a_up = sum(u.duration for u in a.uploads)
    b_up = sum(u.duration for u in b.uploads)
    print(f"  {'Upload time':<24} {_fmt_ms(a_up):>16} {_fmt_ms(b_up):>16}")

    a_tp = _fmt_bytes(int(a.total_bytes / a_up)) if a_up > 0 else "N/A"
    b_tp = _fmt_bytes(int(b.total_bytes / b_up)) if b_up > 0 else "N/A"
    print(f"  {'Upload throughput':<24} {a_tp + '/s':>16} {b_tp + '/s':>16}")

    a_dl = sum(d.duration for d in a.downloads)
    b_dl = sum(d.duration for d in b.downloads)
    print(f"  {'Download time':<24} {_fmt_ms(a_dl):>16} {_fmt_ms(b_dl):>16}")

    a_dtp = _fmt_bytes(int(a.total_bytes / a_dl)) if a_dl > 0 else "N/A"
    b_dtp = _fmt_bytes(int(b.total_bytes / b_dl)) if b_dl > 0 else "N/A"
    print(f"  {'Download throughput':<24} {a_dtp + '/s':>16} {b_dtp + '/s':>16}")

    a_chunks = sum(u.chunk_count for u in a.uploads if u.success)
    b_chunks = sum(u.chunk_count for u in b.uploads if u.success)
    print(f"  {'Total chunks':<24} {a_chunks:>16} {b_chunks:>16}")

    a_avg_c = a_chunks / max(sum(1 for u in a.uploads if u.success), 1)
    b_avg_c = b_chunks / max(sum(1 for u in b.uploads if u.success), 1)
    print(f"  {'Avg chunks/file':<24} {a_avg_c:>16.1f} {b_avg_c:>16.1f}")

    print(f"  {'─' * 56}")
    print(f"  Workload A stresses:  metadata I/O, many small chunk lookups,")
    print(f"                         high request count, listing performance.")
    print(f"  Workload B stresses:  large chunk count per file, transfer time,")
    print(f"                         memory, streaming reassembly, integrity.")
    print(f"{BOLD}{CYAN}{'═' * 60}{RESET}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Run storage workload benchmarks.")
    parser.add_argument(
        "--workload", "-w",
        choices=["a", "b", "both"],
        default="both",
        help="Which workload to run (default: both).",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}Storage Workload Benchmark{RESET}")
    print(f"  Gateway: {BASE_URL}")
    print(f"  Time:    {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Pre-check
    try:
        r = requests.get(f"{BASE_URL}/health", timeout=10)
        if r.status_code != 200:
            print(f"\n  {RED}Gateway not healthy (HTTP {r.status_code}).{RESET}")
            return 1
        print(f"  Status:  {GREEN}healthy{RESET} ({r.json()})")
    except requests.ConnectionError:
        print(f"\n  {RED}Cannot reach {BASE_URL} — is docker compose up?{RESET}")
        return 1

    report_a: WorkloadReport | None = None
    report_b: WorkloadReport | None = None

    if args.workload in ("a", "both"):
        report_a = run_workload_a()

    if args.workload in ("b", "both"):
        report_b = run_workload_b()

    if args.workload == "both":
        _print_comparison(report_a, report_b)

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
