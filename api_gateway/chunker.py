"""
chunker.py — Fixed-size chunker (self-contained, no dep on gateway/chunker.py).
"""
from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass
from typing import Generator, Union

BytesLike = Union[bytes, bytearray, io.BytesIO]

DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024   # 4 MiB


@dataclass(frozen=True)
class Chunk:
    index: int
    data:  bytes
    hash:  str
    size:  int


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixed_chunks(source: BytesLike, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Generator[Chunk, None, None]:
    if isinstance(source, (bytes, bytearray)):
        source = io.BytesIO(bytes(source))
    idx = 0
    while True:
        data = source.read(chunk_size)
        if not data:
            break
        yield Chunk(index=idx, data=data, hash=sha256(data), size=len(data))
        idx += 1
