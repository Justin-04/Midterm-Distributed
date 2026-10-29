"""
erasure.py

Reed-Solomon erasure coding (k data + m parity) using the ``zfec`` library.

For each chunk produced by the chunker, the encoder splits it into *k*
equal-sized data fragments and computes *m* parity fragments.  Each of the
k+m shards is then distributed to a distinct storage node via the consistent
hash ring.

During retrieval, any *k* out of *k+m* shards suffice to reconstruct the
original chunk — the system tolerates up to *m* node failures or corruptions
without data loss.

Key characteristics
-------------------
- ``zfec`` operates over GF(2^8) with a Vandermonde generator matrix.
- Data is zero-padded to be divisible by *k* before encoding; the padding
  length is stored in metadata so it can be stripped on reconstruction.
- Each shard's SHA-256 is computed independently and used as its storage key
  (content-addressed), so existing storage node infrastructure is reused
  without modification.

Public API
----------
- ErasureConfig  : dataclass holding k, m
- encode_chunk(data, k, m) → (shards, pad_len)
- decode_chunk(shards, indices, k, m, original_size, pad_len) → bytes
- shard_key(shard_hash) → str   (convenience wrapper)
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Sequence

logger = logging.getLogger(__name__)

# Lazy import so the module can be imported even if zfec is not installed
# (useful for ring-only test environments).
_zfec = None


def _get_zfec():
    global _zfec
    if _zfec is None:
        try:
            from zfec import Encoder, Decoder  # type: ignore
            _zfec = (Encoder, Decoder)
        except ImportError:
            raise ImportError(
                "zfec is required for erasure coding. Install with: pip install zfec"
            )
    return _zfec


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ErasureConfig:
    """Configuration for Reed-Solomon erasure coding."""
    k: int   # data fragments
    m: int   # parity fragments

    @property
    def total_shards(self) -> int:
        return self.k + self.m

    @property
    def fault_tolerance(self) -> int:
        """Max number of shards that can be lost while still recovering."""
        return self.m

    def __repr__(self) -> str:
        return f"ErasureConfig(k={self.k}, m={self.m}, ft={self.fault_tolerance})"


@dataclass(frozen=True)
class Shard:
    """A single erasure-coded shard."""
    index: int        # 0 .. k+m-1
    data:  bytes       # shard payload
    hash:  str         # SHA-256 hex of shard data (used as storage key)
    size:  int         # len(data)


# ---------------------------------------------------------------------------
# Encode
# ---------------------------------------------------------------------------

def encode_chunk(data: bytes, k: int, m: int) -> tuple[list[Shard], int]:
    """
    Split *data* into k+m shards using Reed-Solomon.

    Parameters
    ----------
    data : raw chunk bytes
    k    : number of data fragments
    m    : number of parity fragments

    Returns
    -------
    (shards, pad_len)
        shards   : list of Shard objects (index 0..k+m-1)
        pad_len  : number of zero-padding bytes added (for later removal)
    """
    Encoder, _ = _get_zfec()

    # Pad data to be divisible by k.
    pad_len = (k - len(data) % k) % k
    padded = data + b'\x00' * pad_len

    enc = Encoder(k=k, m=m)
    raw_shares = enc.encode(padded)   # list of k+m bytes objects

    shards: list[Shard] = []
    for i, shard_data in enumerate(raw_shares):
        shard_hash = hashlib.sha256(shard_data).hexdigest()
        shards.append(Shard(
            index=i,
            data=shard_data,
            hash=shard_hash,
            size=len(shard_data),
        ))

    logger.debug(
        "Erasure encode: %d bytes → %d shards (k=%d, m=%d, pad=%d)",
        len(data), len(shards), k, m, pad_len,
    )
    return shards, pad_len


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------

def decode_chunk(
    shards_data: list[bytes],
    shard_indices: list[int],
    k: int,
    m: int,
    original_size: int,
    pad_len: int,
) -> bytes:
    """
    Reconstruct the original chunk from *k* or more shards.

    Parameters
    ----------
    shards_data    : list of shard payloads (at least k items)
    shard_indices  : corresponding shard indices (0..k+m-1)
    k, m           : erasure parameters
    original_size  : original chunk size in bytes (before padding)
    pad_len        : padding bytes added during encoding

    Returns
    -------
    The reconstructed chunk bytes (exactly *original_size* bytes).

    Raises
    ------
    ValueError
        If fewer than k shards are provided.
    """
    _, Decoder = _get_zfec()

    if len(shards_data) < k:
        raise ValueError(
            f"Need at least {k} shards to decode, got {len(shards_data)}."
        )

    # Use only the first k valid shards.
    sh_data = shards_data[:k]
    sh_idx  = shard_indices[:k]

    dec = Decoder(k=k, m=m)
    recovered = dec.decode(sh_data, sh_idx)

    # Strip padding.
    result = recovered[:original_size]

    logger.debug(
        "Erasure decode: %d shards → %d bytes (k=%d, m=%d, pad=%d)",
        len(sh_data), len(result), k, m, pad_len,
    )
    return result


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def verify_shard(shard_data: bytes, expected_hash: str) -> bool:
    """Return True if SHA-256(shard_data) == expected_hash."""
    return hashlib.sha256(shard_data).hexdigest() == expected_hash.lower()
