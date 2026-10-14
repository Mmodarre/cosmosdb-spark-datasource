"""EPK sub-splitting: exact coverage (no gaps, no overlaps) and balance."""

from __future__ import annotations

import bisect
import collections
import random
import string
from itertools import pairwise

import pytest
from azure.cosmos._routing.routing_range import Range
from azure.cosmos.partition_key import PartitionKey

from cosmosdb_datasource import epk

HEX = "0123456789ABCDEF"


def rand_hex(rng: random.Random, min_len: int = 0, max_len: int = 70, even: bool | None = None) -> str:
    n = rng.randint(min_len, max_len)
    if even is True and n % 2:
        n += 1
    if even is False and n % 2 == 0:
        n += 1
    return "".join(rng.choice(HEX) for _ in range(n))


def rand_range(rng: random.Random) -> tuple[str, str]:
    choice = rng.random()
    if choice < 0.1:
        return "", "FF"
    while True:
        a = "" if rng.random() < 0.15 else rand_hex(rng, 1, 40, even=rng.random() < 0.8)
        b = "FF" if rng.random() < 0.15 else rand_hex(rng, 1, 40, even=rng.random() < 0.8)
        if a > b:
            a, b = b, a
        if a < b:
            return a, b


def probes(rng: random.Random, lo: str, hi: str, pieces: list[tuple[str, str]], n: int = 300) -> list[str]:
    out = {"", "FF", lo, hi}
    for a, b in pieces:  # every boundary and its neighbours (prefixes / extensions)
        out.update({a, b, a + "0", a + "00", a[:-1], b + "0", b[:-1], a + "FF"})
    out.update(rand_hex(rng, 0, 66) for _ in range(n))
    pk = PartitionKey(path="/x", kind="Hash", version=2)
    out.update(pk._get_epk_range_for_partition_key(rng.random()).min for _ in range(50))
    return sorted(out)


def check_exact_cover(lo: str, hi: str, pieces: list[tuple[str, str]], samples: list[str]) -> None:
    assert pieces[0][0] == lo and pieces[-1][1] == hi
    for (_, b), (a, _) in pairwise(pieces):
        assert b == a, "sub-ranges must be contiguous"
    for a, b in pieces:
        assert a < b, "sub-ranges must be non-empty"
        assert a == "" or len(a) % 2 == 0 or a == lo, "generated boundaries are whole bytes"
    parent = Range(lo, hi, True, False)
    subs = [Range(a, b, True, False) for a, b in pieces]
    for s in samples:
        hits = sum(1 for r in subs if r.contains(s))
        assert hits == (1 if parent.contains(s) else 0), (lo, hi, s, pieces)


@pytest.mark.parametrize("seed", range(40))
def test_split_is_exact_cover_property(seed: int) -> None:
    rng = random.Random(seed)
    for _ in range(25):
        lo, hi = rand_range(rng)
        n = rng.choice([1, 2, 3, 4, 7, 8, 16, 33, 64])
        kind, version = rng.choice([("Hash", 2), ("Hash", 1), ("MultiHash", 2)])
        pieces = epk.split_range(lo, hi, n, kind=kind, version=version)
        assert 1 <= len(pieces) <= n
        check_exact_cover(lo, hi, pieces, probes(rng, lo, hi, pieces))


def test_full_range_split_counts_and_sentinels() -> None:
    for n in (1, 2, 5, 16, 128):
        pieces = epk.split_range("", "FF", n)
        assert len(pieces) == n
        assert pieces[0][0] == "" and pieces[-1][1] == "FF"


def test_narrow_ranges_still_split_or_stay_whole() -> None:
    # adjacent byte strings: interpolation must extend the width instead of producing duplicates
    pieces = epk.split_range("05C1D7", "05C1D8", 8)
    assert len(pieces) == 8
    check_exact_cover("05C1D7", "05C1D8", pieces, probes(random.Random(1), "05C1D7", "05C1D8", pieces))
    # an interval containing only the point "1A" cannot be split
    assert epk.split_range("1A", "1A00", 4) == [("1A", "1A00")]


def test_lowercase_input_is_normalized_and_bad_input_rejected() -> None:
    assert epk.split_range("0a", "ff", 1) == [("0A", "FF")]
    with pytest.raises(ValueError):
        epk.split_range("ZZ", "FF", 2)
    with pytest.raises(ValueError):
        epk.split_range("FF", "FF", 2)


def _bucket_counts(kind: str, version: int, lo: str, hi: str, n: int, samples: int = 30000) -> list[int]:
    paths = ["/a", "/b"] if kind == "MultiHash" else "/a"
    pk = PartitionKey(path=paths, kind=kind, version=version)
    pieces = epk.split_range(lo, hi, n, kind=kind, version=version)
    starts = [p[0] for p in pieces]
    rng = random.Random(7)
    counts: collections.Counter[int] = collections.Counter()
    seen = 0
    while seen < samples:
        value = "".join(rng.choices(string.ascii_letters, k=rng.randint(3, 20)))
        key = [value, str(rng.random())] if kind == "MultiHash" else value
        e = pk._get_epk_range_for_partition_key(key).min
        if lo <= e < hi:
            counts[bisect.bisect_right(starts, e) - 1] += 1
            seen += 1
    return [counts[i] for i in range(len(pieces))]


@pytest.mark.parametrize(("kind", "version"), [("Hash", 2), ("Hash", 1), ("MultiHash", 2)])
def test_full_range_split_is_balanced(kind: str, version: int) -> None:
    counts = _bucket_counts(kind, version, "", "FF", 8)
    mean = sum(counts) / len(counts)
    assert all(abs(c - mean) / mean < 0.15 for c in counts), counts


def test_sub_range_of_a_physical_partition_is_balanced_v2() -> None:
    lo, hi = epk.split_range("", "FF", 4)[1]  # a realistic physical partition boundary pair
    counts = _bucket_counts("Hash", 2, lo, hi, 6, samples=12000)
    mean = sum(counts) / len(counts)
    assert all(abs(c - mean) / mean < 0.2 for c in counts), counts


def test_cover_overlay_and_split_at_boundaries() -> None:
    a = [("", "40"), ("40", "FF")]
    b = [("", "20"), ("20", "80"), ("80", "FF")]
    assert epk.is_cover(a) and epk.is_cover(b)
    assert not epk.is_cover([("", "40"), ("41", "FF")])
    pieces = epk.overlay(a, b)
    assert [(lo, hi) for lo, hi, _, _ in pieces] == [("", "20"), ("20", "40"), ("40", "80"), ("80", "FF")]
    assert [(i, j) for _, _, i, j in pieces] == [(0, 0), (0, 1), (1, 1), (1, 2)]
    assert epk.split_at_boundaries("10", "90", ["", "20", "80", "FF"]) == [("10", "20"), ("20", "80"), ("80", "90")]
    with pytest.raises(ValueError):
        epk.overlay([("", "40")], b)


def test_range_key_round_trip() -> None:
    for lo, hi in [("", "FF"), ("05C1D7", "FF"), ("", "3F")]:
        assert epk.parse_range_key(epk.range_key(lo, hi)) == (lo, hi)
    with pytest.raises(ValueError):
        epk.parse_range_key("nohyphen")
