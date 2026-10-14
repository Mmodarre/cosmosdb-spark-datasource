"""Streaming offset model.

An offset records, for every EPK range of a cover of the container's key space, the LSN up to
which (inclusive) changes have been handed to Spark::

    {"v": 1, "rid": "<container _rid>", "mode": "LatestVersion",
     "ranges": {"-05C1D7": 1234, "05C1D7-FF": 980}}

Keys are ``"<min>-<max>"`` EPK bounds (stable, derived from the range, see ``epk.range_key``).
Spark compares offsets by their JSON text to detect "no new data", so ``to_dict`` always emits keys
in a fixed order and ranges sorted by their lower bound.

Batch 0 determinism: Spark's offset log stores only *end* offsets; the start of the first
micro-batch is whatever ``initialOffset()`` returns, and Spark calls it again in a new process when
it replays an uncommitted batch 0 after a restart. ``start_from=Now`` (or a timestamp) would then
resolve to a different position. Therefore initial offsets are marked ``"initial": true`` and the
end offset of the batch that starts there carries the original start under ``"init"``; ``partitions``
uses that instead of the re-resolved start.

Initial snapshot (``initial_snapshot=true``): the initial offset carries ``"snapshot"`` with the
session token observed for every range while its start was resolved, and the end offset of the first
batch copies it::

    {..., "initial": true, "snapshot": {"session": {"-FF": "0:-1#1234"}}}     (initial, never logged)
    {..., "init": {"-FF": 1234}, "snapshot": {"session": {"-FF": "0:-1#1234"}}} (end of batch 0, logged)

Every later offset omits the field. Snapshot partitions are planned only for a micro-batch whose start
is an initial offset and whose end carries ``"snapshot"``, i.e. only for batch 0, and only if batch 0
was *planned* with a snapshot. That decision and the session tokens therefore come from the logged end
offset, so a replay of batch 0 after a restart reads the snapshot again with the original tokens, even
if the option was changed in between. Offsets without the field (all offsets written by 0.1.x) mean
"no snapshot". Offsets without a snapshot serialize exactly as in 0.1.x.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cosmosdb_datasource import epk
from cosmosdb_datasource.errors import CosmosOffsetError

OFFSET_VERSION = 1


@dataclass(frozen=True)
class RangeState:
    lo: str
    hi: str
    lsn: int

    @property
    def key(self) -> str:
        return epk.range_key(self.lo, self.hi)


def _ordered_cover(ranges: tuple[RangeState, ...], what: str) -> tuple[RangeState, ...]:
    ordered = tuple(sorted(ranges, key=lambda r: r.lo))
    if not epk.is_cover([(r.lo, r.hi) for r in ordered]):
        raise CosmosOffsetError(f"Offset {what} do not tile the container key space ['', 'FF').")
    return ordered


@dataclass(frozen=True)
class SnapshotState:
    """Initial-snapshot marker: session token per range of the initial offset (may be empty)."""

    session: tuple[tuple[str, str], ...] = ()  # (range key, session token), sorted by key

    def __post_init__(self) -> None:
        object.__setattr__(self, "session", tuple(sorted(self.session)))

    def to_dict(self) -> dict[str, Any]:
        return {"session": dict(self.session)}

    @classmethod
    def from_dict(cls, raw: Any) -> SnapshotState:
        session = raw.get("session") if isinstance(raw, Mapping) else None
        if not isinstance(session, Mapping) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in session.items()
        ):
            raise CosmosOffsetError(f"Offset field 'snapshot' must look like {{'session': {{...}}}}, got {raw!r}.")
        for key in session:
            try:
                epk.parse_range_key(key)
            except ValueError as exc:
                raise CosmosOffsetError(str(exc)) from None
        return cls(tuple(session.items()))

    def session_token_for(self, lo: str, hi: str) -> str | None:
        """Compound session token of every recorded range overlapping ``[lo, hi)`` (None if none).

        Without a split since the start was resolved, exactly one range overlaps. After a split the
        parent's token is sent for the children (see docs/DESIGN.md, "Initial snapshot").
        """
        tokens: list[str] = []
        for key, token in self.session:
            r_lo, r_hi = epk.parse_range_key(key)
            if r_lo < hi and lo < r_hi:
                tokens.extend(t for t in token.split(",") if t and t not in tokens)
        return ",".join(tokens) or None


def _parse_ranges(raw: Any, what: str) -> tuple[RangeState, ...]:
    if not isinstance(raw, Mapping) or not raw:
        raise CosmosOffsetError(f"Offset has no '{what}'.")
    out = []
    for key, lsn in raw.items():
        try:
            lo, hi = epk.parse_range_key(key)
        except ValueError as exc:
            raise CosmosOffsetError(str(exc)) from None
        if isinstance(lsn, bool) or not isinstance(lsn, int):
            raise CosmosOffsetError(f"Offset LSN for range {key!r} must be an integer, got {lsn!r}.")
        out.append(RangeState(lo, hi, lsn))
    return tuple(out)


@dataclass(frozen=True)
class Offset:
    rid: str
    mode: str
    ranges: tuple[RangeState, ...]
    initial: bool = False  # produced by initialOffset()
    init: tuple[RangeState, ...] | None = None  # original start of batch 0 (see module doc)
    snapshot: SnapshotState | None = None  # initial snapshot pending (initial) / planned (end of batch 0)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ranges", _ordered_cover(self.ranges, "ranges"))
        if self.init is not None:
            object.__setattr__(self, "init", _ordered_cover(self.init, "init ranges"))

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "v": OFFSET_VERSION,
            "rid": self.rid,
            "mode": self.mode,
            "ranges": {r.key: r.lsn for r in self.ranges},
        }
        if self.initial:
            out["initial"] = True
        if self.init is not None:
            out["init"] = {r.key: r.lsn for r in self.init}
        if self.snapshot is not None:
            out["snapshot"] = self.snapshot.to_dict()
        return out

    def same_position(self, other: Offset) -> bool:
        return self.rid == other.rid and self.mode == other.mode and self.ranges == other.ranges

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Offset:
        if not isinstance(data, Mapping):
            raise CosmosOffsetError(f"Offset must be a JSON object, got {type(data).__name__}.")
        version = data.get("v")
        if version != OFFSET_VERSION:
            raise CosmosOffsetError(f"Unsupported cosmosdb offset version {version!r} (expected {OFFSET_VERSION}).")
        ranges = _parse_ranges(data.get("ranges"), "ranges")
        init = _parse_ranges(data["init"], "init") if data.get("init") is not None else None
        snapshot = SnapshotState.from_dict(data["snapshot"]) if data.get("snapshot") is not None else None
        rid, mode = data.get("rid"), data.get("mode")
        if not isinstance(rid, str) or not isinstance(mode, str):
            raise CosmosOffsetError("Offset is missing 'rid' or 'mode'.")
        return cls(rid=rid, mode=mode, ranges=ranges, initial=data.get("initial") is True, init=init, snapshot=snapshot)

    def validate_for(self, rid: str, mode: str) -> None:
        if self.rid != rid:
            raise CosmosOffsetError(
                "The checkpointed offset belongs to a different container instance "
                f"(checkpoint rid={self.rid!r}, current rid={rid!r}); the container was probably "
                "deleted and re-created. Start the stream with a new checkpoint location."
            )
        if self.mode != mode:
            raise CosmosOffsetError(
                f"The checkpoint was created with change_feed_mode={self.mode} but the stream now uses "
                f"change_feed_mode={mode}. Use a new checkpoint location to switch modes."
            )

    def lsn_bounds(self) -> list[tuple[str, str, int]]:
        return [(r.lo, r.hi, r.lsn) for r in self.ranges]


def rekey(offset: Offset, physical: list[tuple[str, str]]) -> Offset:
    """Refine ``offset`` so no range spans more than one physical partition (handles splits).

    Children inherit the parent's LSN, which is exactly what the SDK does for a split
    (``FeedRangeCompositeContinuation.handle_feed_range_gone`` copies the parent token to every
    child). Ranges are never coarsened: after a merge several ranges simply map to one partition,
    and each is read with an EPK filter.
    """
    boundaries = sorted({b for rng in physical for b in rng})
    out = []
    for r in offset.ranges:
        for lo, hi in epk.split_at_boundaries(r.lo, r.hi, boundaries):
            out.append(RangeState(lo, hi, r.lsn))
    return Offset(
        offset.rid, offset.mode, tuple(out), initial=offset.initial, init=offset.init, snapshot=offset.snapshot
    )


def min_overlapping_lsn(target: Offset, lo: str, hi: str) -> int:
    """Smallest LSN of any ``target`` range overlapping ``[lo, hi)``."""
    values = [r.lsn for r in target.ranges if r.lo < hi and lo < r.hi]
    if not values:
        raise CosmosOffsetError(f"No target range overlaps [{lo!r}, {hi!r}).")
    return min(values)
