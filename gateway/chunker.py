"""
chunker.py

Modular chunking engine supporting two strategies:

1. FixedSizeChunker  — splits a byte stream into uniform blocks of a
                       configurable size (default 4 MiB).

2. FastCDCChunker    — Content-Defined Chunking using a gear-hash rolling
                       window (FastCDC algorithm).  Chunk boundaries are
                       determined by content patterns so identical byte
                       sequences produce identical chunks across different
                       files / upload sessions (enabling cross-file
                       deduplication).

Both chunkers:
  - Accept raw bytes or a file-like object.
  - Yield Chunk dataclass instances (index, data, sha256 hash, size).
  - Are fully synchronous (call from a thread-pool executor if needed in an
    async context).

References
----------
FastCDC paper: Wen Xia et al., "FastCDC: a Fast and Efficient Content-Defined
Chunking Approach for Data Deduplication" (USENIX ATC 2016).
"""

from __future__ import annotations

import hashlib
import io
import os
import struct
from dataclasses import dataclass, field
from typing import Generator, Iterable, Union

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Chunk:
    """A single content chunk produced by a chunker."""
    index:  int          # 0-based position in the original stream
    data:   bytes        # raw payload
    hash:   str          # hex SHA-256 of data
    size:   int          # len(data)  — kept separate for cheap access

    def __repr__(self) -> str:
        return (
            f"Chunk(index={self.index}, size={self.size}, "
            f"hash={self.hash[:12]}…)"
        )


# Accepted input types for chunkers
BytesLike = Union[bytes, bytearray, memoryview, io.RawIOBase, io.BufferedIOBase]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _to_stream(source: BytesLike) -> io.BufferedIOBase:
    """Wrap plain bytes in a BytesIO so all code paths use a stream."""
    if isinstance(source, (bytes, bytearray, memoryview)):
        return io.BytesIO(bytes(source))
    return source  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Fixed-Size Chunker
# ---------------------------------------------------------------------------

# Default chunk size: 4 MiB
DEFAULT_FIXED_SIZE: int = 4 * 1024 * 1024


class FixedSizeChunker:
    """
    Split a byte stream into fixed-size blocks.

    Parameters
    ----------
    chunk_size:
        Number of bytes per chunk (last chunk may be smaller).
    """

    def __init__(self, chunk_size: int = DEFAULT_FIXED_SIZE) -> None:
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
        self.chunk_size = chunk_size

    def chunks(self, source: BytesLike) -> Generator[Chunk, None, None]:
        """
        Yield :class:`Chunk` objects by reading *chunk_size* bytes at a time.

        Parameters
        ----------
        source:
            Raw bytes, bytearray, memoryview, or a readable binary stream.
        """
        stream = _to_stream(source)
        index = 0
        while True:
            data = stream.read(self.chunk_size)
            if not data:
                break
            yield Chunk(
                index=index,
                data=data,
                hash=_sha256(data),
                size=len(data),
            )
            index += 1


# ---------------------------------------------------------------------------
# FastCDC Chunker (gear-hash rolling window)
# ---------------------------------------------------------------------------

# Default target / min / max chunk sizes (FastCDC recommended ratios: 0.5x / 1x / 2x)
CDC_MIN_SIZE:    int = 2 * 1024 * 1024   # 2 MiB
CDC_AVG_SIZE:    int = 4 * 1024 * 1024   # 4 MiB  (target)
CDC_MAX_SIZE:    int = 8 * 1024 * 1024   # 8 MiB

# Gear hash lookup table — 256 random 64-bit values (one per byte value).
# Generated once at import time with a fixed seed for determinism.
_GEAR_TABLE: list[int] = []


def _build_gear_table() -> None:
    """Populate _GEAR_TABLE with 256 deterministic pseudo-random 64-bit ints."""
    import random
    rng = random.Random(0xDEADBEEF_CAFEBABE)
    _GEAR_TABLE.extend(rng.getrandbits(64) for _ in range(256))


_build_gear_table()


class FastCDCChunker:
    """
    Content-Defined Chunking using the FastCDC gear-hash algorithm.

    The rolling hash is computed over a sliding window of bytes. A chunk
    boundary is declared when:
      - The hash satisfies a bitmask condition (avg-size target), AND
      - The current position is >= min_size (hard minimum enforced), OR
      - The current position reaches max_size (hard maximum cap).

    This produces variable-length chunks that align to content boundaries,
    enabling cross-file deduplication when the same byte sequence appears
    in different files.

    Parameters
    ----------
    min_size:  Minimum chunk size in bytes.
    avg_size:  Target average chunk size (controls the bitmask).
    max_size:  Maximum chunk size cap.
    """

    def __init__(
        self,
        min_size: int = CDC_MIN_SIZE,
        avg_size: int = CDC_AVG_SIZE,
        max_size: int = CDC_MAX_SIZE,
    ) -> None:
        if not (min_size < avg_size < max_size):
            raise ValueError(
                f"Require min_size < avg_size < max_size, "
                f"got {min_size} / {avg_size} / {max_size}"
            )
        self.min_size = min_size
        self.avg_size = avg_size
        self.max_size = max_size

        # Derive the bitmask from avg_size.
        # bits = log2(avg_size); mask = 2^bits - 1
        bits = (avg_size - 1).bit_length()
        self._mask_s: int = (1 << bits) - 1          # normal mask
        self._mask_l: int = (1 << max(bits - 1, 1)) - 1  # relaxed mask (FastCDC)

    # ------------------------------------------------------------------
    def chunks(self, source: BytesLike) -> Generator[Chunk, None, None]:
        """
        Yield variable-length :class:`Chunk` objects.

        Parameters
        ----------
        source:
            Raw bytes, bytearray, memoryview, or a readable binary stream.
        """
        data: bytes
        if isinstance(source, (bytes, bytearray, memoryview)):
            data = bytes(source)
        else:
            data = source.read()  # type: ignore[union-attr]

        total = len(data)
        start = 0
        index = 0

        while start < total:
            end = self._find_boundary(data, start, total)
            chunk_data = data[start:end]
            yield Chunk(
                index=index,
                data=chunk_data,
                hash=_sha256(chunk_data),
                size=len(chunk_data),
            )
            index += 1
            start = end

    # ------------------------------------------------------------------
    def _find_boundary(self, data: bytes, start: int, total: int) -> int:
        """
        Return the end index (exclusive) of the next chunk starting at *start*.

        Uses two masks:
          - mask_s (stricter) for the first half of the target window
          - mask_l (looser)   for the second half — increases cut probability
            as we approach avg_size, reducing variance.
        """
        min_end = min(start + self.min_size, total)
        max_end = min(start + self.max_size, total)
        norm_end = min(start + self.avg_size, total)

        gear: int = 0
        mask_s = self._mask_s
        mask_l = self._mask_l
        table = _GEAR_TABLE

        # Zone 1: min_size → avg_size  (stricter mask)
        for i in range(min_end, norm_end):
            gear = ((gear << 1) + table[data[i]]) & 0xFFFFFFFFFFFFFFFF
            if not (gear & mask_s):
                return i + 1

        # Zone 2: avg_size → max_size  (looser mask)
        for i in range(norm_end, max_end):
            gear = ((gear << 1) + table[data[i]]) & 0xFFFFFFFFFFFFFFFF
            if not (gear & mask_l):
                return i + 1

        return max_end  # hard cap


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def make_chunker(
    mode: str = "fixed",
    chunk_size: int = DEFAULT_FIXED_SIZE,
    min_size: int = CDC_MIN_SIZE,
    avg_size: int = CDC_AVG_SIZE,
    max_size: int = CDC_MAX_SIZE,
) -> FixedSizeChunker | FastCDCChunker:
    """
    Factory function to create a chunker by name.

    Parameters
    ----------
    mode:       ``"fixed"`` or ``"cdc"`` / ``"fastcdc"``.
    chunk_size: Used only for fixed-size mode.
    min_size, avg_size, max_size: Used only for CDC mode.
    """
    mode = mode.lower()
    if mode == "fixed":
        return FixedSizeChunker(chunk_size=chunk_size)
    if mode in ("cdc", "fastcdc"):
        return FastCDCChunker(min_size=min_size, avg_size=avg_size, max_size=max_size)
    raise ValueError(f"Unknown chunking mode: {mode!r}. Choose 'fixed' or 'cdc'.")
