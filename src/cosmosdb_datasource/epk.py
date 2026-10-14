"""Effective-partition-key (EPK) range arithmetic.

Cosmos DB routes every document by its EPK, an upper-case hex string. Physical partitions and
feed ranges are half-open intervals ``[min, max)`` over those strings compared lexicographically,
with the sentinels ``""`` (minimum) and ``"FF"`` (exclusive maximum). See
``azure/cosmos/_routing/routing_range.py`` (``Range``, ``Range.contains``).

Correctness of sub-splitting does not depend on balance: any strictly increasing sequence of
boundaries ``lo < p1 < ... < pk < hi`` partitions ``[lo, hi)`` into disjoint, gap-free half-open
intervals under any total order, and the service applies exactly that order. The helpers below
therefore only use the hash distribution to *choose* balanced boundaries, and always re-check the
lexicographic invariants before returning them.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable, Sequence
from functools import lru_cache
from itertools import pairwise

MIN_EPK = ""
MAX_EPK = "FF"
_HEX = frozenset("0123456789ABCDEF")
_MIN_WIDTH = 32  # hex digits used for interpolation (128 bits, the width of a V2 hash)
_MAX_WIDTH = 256
_V1_GRID_SIZE = 4096


def normalize_epk(value: str) -> str:
    value = value.upper()
    if any(c not in _HEX for c in value):
        raise ValueError(f"Not an effective partition key hex string: {value!r}")
    return value


def range_key(lo: str, hi: str) -> str:
    """Return a stable, human-readable identifier of an EPK range (used as streaming offset key)."""
    return f"{lo}-{hi}"


def parse_range_key(key: str) -> tuple[str, str]:
    lo, sep, hi = key.partition("-")
    if not sep:
        raise ValueError(f"Invalid feed range key {key!r}")
    return normalize_epk(lo), normalize_epk(hi)


def _to_int(value: str, width: int) -> int:
    return int(value.ljust(width, "0"), 16) if value else 0


def _effective_bounds(lo: str, hi: str, kind: str, version: int) -> tuple[str, str]:
    """Return the bounds used to compute *balanced* boundaries (never used for correctness).

    V2 and MultiHash hashes clear the two most significant bits of the first byte
    (``hash_bytes[0] &= 0x3F`` in ``azure/cosmos/partition_key.py``), so real EPKs are < "40"
    even though the last physical partition ends at the sentinel "FF".
    """
    if (kind.lower() == "multihash" or version >= 2) and lo < "40" < hi:
        return lo, "40"
    return lo, hi


def _numeric_points(lo: str, hi: str, n: int, eff_lo: str, eff_hi: str) -> list[str]:
    width = max(_MIN_WIDTH, len(lo), len(hi), len(eff_lo), len(eff_hi))
    width += width % 2
    while True:
        a, b = _to_int(eff_lo, width), _to_int(eff_hi, width)
        if b - a >= 2 * n:
            break
        if width >= _MAX_WIDTH:
            return []
        width += 2
    points = [format(a + (b - a) * i // n, f"0{width}X") for i in range(1, n)]
    return _filter_increasing(lo, hi, points)


def _filter_increasing(lo: str, hi: str, points: Iterable[str]) -> list[str]:
    out: list[str] = []
    prev = lo
    for p in points:
        if prev < p < hi:
            out.append(p)
            prev = p
    return out


@lru_cache(maxsize=1)
def _v1_grid() -> tuple[str, ...]:
    """Return the EPK prefixes of evenly spaced 32-bit hash values for hash-V1 containers.

    A V1 EPK is the binary encoding of ``[float(murmur3_32(pk)), *pk_components]``, so documents
    are ordered by hash value and ``encode([float(h)])`` is a lower bound for every document
    whose hash is >= h. The encoding is far from uniform over the hex space, which is why plain
    numeric interpolation would be badly skewed for V1.
    """
    try:
        from azure.cosmos.partition_key import _to_hex_encoded_binary_string_v1
    except ImportError:  # pragma: no cover - private SDK helper moved; fall back to interpolation
        return ()
    grid = []
    for k in range(1, _V1_GRID_SIZE):
        h = (k * (1 << 32)) // _V1_GRID_SIZE
        grid.append(_to_hex_encoded_binary_string_v1([float(h)]).upper())
    return tuple(sorted(set(grid)))


def _grid_points(lo: str, hi: str, n: int, grid: Sequence[str]) -> list[str]:
    inside = [g for g in grid if lo < g < hi]
    if len(inside) < n - 1:
        return []
    picks = [inside[(i * len(inside)) // n] for i in range(1, n)]
    return _filter_increasing(lo, hi, picks)


def split_range(lo: str, hi: str, n: int, *, kind: str = "Hash", version: int = 2) -> list[tuple[str, str]]:
    """Split ``[lo, hi)`` into up to ``n`` contiguous, non-overlapping sub-ranges.

    Fewer than ``n`` ranges are returned only when the interval is too narrow to split.
    """
    lo, hi = normalize_epk(lo), normalize_epk(hi)
    if not lo < hi:
        raise ValueError(f"Invalid EPK range [{lo!r}, {hi!r})")
    if n <= 1:
        return [(lo, hi)]
    points: list[str] = []
    if kind.lower() != "multihash" and version < 2:
        points = _grid_points(lo, hi, n, _v1_grid())
    if len(points) < n - 1:
        eff_lo, eff_hi = _effective_bounds(lo, hi, kind, version)
        points = _numeric_points(lo, hi, n, eff_lo, eff_hi)
        if len(points) < n - 1:
            points = _numeric_points(lo, hi, n, lo, hi)
    bounds = [lo, *points, hi]
    return list(pairwise(bounds))


def is_cover(ranges: Sequence[tuple[str, str]]) -> bool:
    """Return True if ``ranges`` (sorted) exactly tile ``["", "FF")``."""
    if not ranges or ranges[0][0] != MIN_EPK or ranges[-1][1] != MAX_EPK:
        return False
    for (_, hi), (lo, _) in pairwise(ranges):
        if hi != lo:
            return False
    return all(lo < hi for lo, hi in ranges)


def split_at_boundaries(lo: str, hi: str, boundaries: Sequence[str]) -> list[tuple[str, str]]:
    """Cut ``[lo, hi)`` at every boundary strictly inside it (``boundaries`` sorted)."""
    start = bisect.bisect_right(boundaries, lo)
    cuts = []
    for b in boundaries[start:]:
        if b >= hi:
            break
        cuts.append(b)
    bounds = [lo, *cuts, hi]
    return list(pairwise(bounds))


def overlay(left: Sequence[tuple[str, str]], right: Sequence[tuple[str, str]]) -> list[tuple[str, str, int, int]]:
    """Return the common refinement of two covers of the EPK space.

    Items are ``(lo, hi, i, j)`` for every non-empty intersection of ``left[i]`` and ``right[j]``.
    Both inputs must be sorted and tile ``["", "FF")``.
    """
    if not is_cover(left) or not is_cover(right):
        raise ValueError("overlay() requires two complete covers of the EPK space")
    bounds = sorted({b for r in left for b in r} | {b for r in right for b in r})
    left_starts = [r[0] for r in left]
    right_starts = [r[0] for r in right]
    out = []
    for lo, hi in pairwise(bounds):
        i = bisect.bisect_right(left_starts, lo) - 1
        j = bisect.bisect_right(right_starts, lo) - 1
        out.append((lo, hi, i, j))
    return out
