"""Change feed continuation tokens and the LSN-bounded, replayable read loop.

Continuation token format (azure-cosmos >= 4.14, verified against 4.17.1):
``base64(json(ChangeFeedStateV2.to_dict()))`` where the state is::

    {"v": "v2", "containerRid": rid, "mode": "LatestVersion"|"AllVersionsAndDeletes",
     "startFrom": {"Type": "Beginning"|"Now"|"PointInTime"|...},
     "continuation": {"v": "v2", "rid": rid,
                      "continuation": [{"token": "\\"<lsn>\\"", "range": <Range dict>}, ...],
                      "Range": <Range dict>}}

(``azure/cosmos/_change_feed/change_feed_state.py`` ``ChangeFeedStateV2.to_dict`` and
``feed_range_composite_continuation_token.py`` ``FeedRangeCompositeContinuation.to_dict``.)

Each ``token`` is the server etag of one physical (sub-)range, i.e. its change-feed LSN position;
the next request sends it as ``If-None-Match`` and receives changes with a strictly larger LSN.
We build single-range tokens ourselves so that a streaming offset can be a plain integer LSN per
range. ``startFrom`` is never ``PointInTime`` in tokens we build: azure-cosmos 4.17.1 cannot parse
its own PointInTime tokens (``ChangeFeedStartFromPointInTime.from_json`` passes milliseconds to
``datetime.fromtimestamp``).
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Any

from cosmosdb_datasource import retry
from cosmosdb_datasource.documents import item_lsn
from cosmosdb_datasource.errors import CosmosChangeFeedError
from cosmosdb_datasource.options import MODE_ALL_VERSIONS_AND_DELETES

logger = logging.getLogger("cosmosdb_datasource")


def _range_dict(lo: str, hi: str) -> dict[str, Any]:
    return {"min": lo, "max": hi, "isMinInclusive": True, "isMaxInclusive": False}


def format_etag(lsn: int) -> str:
    return f'"{int(lsn)}"'


def parse_etag_lsn(etag: Any) -> int:
    """Return the LSN carried by a change-feed etag such as ``"1234"`` (quotes included)."""
    if etag is None:
        raise CosmosChangeFeedError("Change feed response did not carry an etag.")
    text = str(etag).strip().strip('"').strip()
    try:
        return int(text)
    except ValueError:
        raise CosmosChangeFeedError(f"Unexpected change feed etag format: {etag!r}") from None


@dataclass(frozen=True)
class Position:
    lo: str
    hi: str
    lsn: int | None  # None: this sub-range has not been read yet


def build_composite_continuation(container_rid: str, mode: str, lo: str, hi: str, children: list[Position]) -> str:
    """Build a continuation for ``[lo, hi)`` with one child token per sub-range (each after its LSN)."""
    start_from = "Now" if mode == MODE_ALL_VERSIONS_AND_DELETES else "Beginning"
    state = {
        "v": "v2",
        "containerRid": container_rid,
        "mode": mode,
        "startFrom": {"Type": start_from},
        "continuation": {
            "v": "v2",
            "rid": container_rid,
            "continuation": [
                {"token": format_etag(c.lsn if c.lsn is not None else 0), "range": _range_dict(c.lo, c.hi)}
                for c in children
            ],
            "Range": _range_dict(lo, hi),
        },
    }
    return base64.b64encode(json.dumps(state).encode("utf-8")).decode("ascii")


def build_continuation(container_rid: str, mode: str, lo: str, hi: str, lsn: int) -> str:
    """Build a single-range continuation that resumes the change feed of ``[lo, hi)`` after ``lsn``."""
    return build_composite_continuation(container_rid, mode, lo, hi, [Position(lo, hi, lsn)])


def clip_positions(positions: list[Position], previous: list[Position], lo: str, hi: str) -> list[Position]:
    """Clip children to ``[lo, hi)``, positioned at the LSNs held *before* the offending page.

    After a split, azure-cosmos replaces a sub-range token by tokens for the *full* child
    partitions (``FeedRangeCompositeContinuation.handle_feed_range_gone`` uses
    ``Range.PartitionKeyRangeToRange(child_range)`` without intersecting with the token's range),
    so a read scoped to part of a partition would suddenly cover all of it. ``previous`` tiles
    ``[lo, hi)``; every clipped child inherits the LSN of the previous range(s) it overlaps.
    """
    out: list[Position] = []
    for child in positions:
        a, b = max(child.lo, lo), min(child.hi, hi)
        if a >= b:
            continue
        for prev in previous:
            pa, pb = max(a, prev.lo), min(b, prev.hi)
            if pa < pb:
                out.append(Position(pa, pb, prev.lsn))
    out = sorted(set(out), key=lambda p: (p.lo, p.hi))
    tiles = bool(out) and out[0].lo == lo and out[-1].hi == hi and all(x.hi == y.lo for x, y in pairwise(out))
    if not tiles:
        raise CosmosChangeFeedError(
            f"Unexpected change feed continuation after a partition split for range [{lo!r}, {hi!r}); "
            "the task will be retried by Spark."
        )
    return out


def split_positions(positions: list[Position], physical: list[tuple[str, str]], lo: str, hi: str) -> list[Position]:
    """Split the positions of ``[lo, hi)`` at the current physical partition boundaries.

    Each piece keeps the LSN of the position it comes from, as a child partition continues its
    parent's LSN sequence.
    """
    out = sorted(
        {
            Position(max(p.lo, lo, a), min(p.hi, hi, b), p.lsn)
            for p in positions
            for a, b in physical
            if max(p.lo, lo, a) < min(p.hi, hi, b)
        },
        key=lambda p: (p.lo, p.hi),
    )
    if not (out and out[0].lo == lo and out[-1].hi == hi and all(x.hi == y.lo for x, y in pairwise(out))):
        raise CosmosChangeFeedError(
            f"Cannot rebuild the change feed continuation of range [{lo!r}, {hi!r}); the task will be retried by Spark."
        )
    return out


def _outside(positions: list[Position], lo: str, hi: str) -> bool:
    return any(p.lo < lo or p.hi > hi for p in positions)


def decode_positions(token: str | None) -> list[Position]:
    """Decode the per-physical-range positions inside an SDK-produced composite continuation token."""
    if not token:
        return []
    try:
        state = json.loads(base64.b64decode(token).decode("utf-8"))
        children = state["continuation"]["continuation"]
    except Exception as exc:
        raise CosmosChangeFeedError(
            "Could not decode the azure-cosmos change feed continuation token; the installed SDK "
            "version may use an unsupported format."
        ) from exc
    out = []
    for child in children:
        rng = child["range"]
        tok = child.get("token")
        out.append(
            Position(str(rng["min"]).upper(), str(rng["max"]).upper(), None if tok is None else parse_etag_lsn(tok))
        )
    return out


@dataclass
class ChangeFeedPage:
    items: list[dict[str, Any]]
    positions: list[Position]
    resume_token: str | None
    drained: bool = False
    request_charge: float = 0.0


@dataclass
class ReadStats:
    rows: int = 0
    pages: int = 0
    request_charge: float = 0.0
    retries: int = 0
    lag_waits: int = 0
    skipped_beyond_end: int = 0
    extra: dict[str, Any] = field(default_factory=dict)


_SDK_CHANGE_FEED_PACKAGE = "azure.cosmos._change_feed"


def sdk_recursion_error(exc: BaseException) -> bool:
    """Return True for a ``RecursionError`` raised inside the SDK's change-feed code (its 410 handling)."""
    if not isinstance(exc, RecursionError):
        return False
    tb = exc.__traceback__
    while tb is not None:
        if str(tb.tb_frame.f_globals.get("__name__", "")).startswith(_SDK_CHANGE_FEED_PACKAGE):
            return True
        tb = tb.tb_next
    return False


def all_reached(positions: list[Position], end_lsn: int) -> bool:
    return bool(positions) and all(p.lsn is not None and p.lsn >= end_lsn for p in positions)


def read_bounded(
    open_pages: Callable[[str | None], Iterator[ChangeFeedPage]],
    *,
    start_lsn: int,
    end_lsn: int,
    mode: str,
    policy: retry.RetryPolicy,
    end_wait_seconds: float,
    stats: ReadStats,
    describe: str,
    lo: str = "",
    hi: str = "FF",
    rebuild_token: Callable[[list[Position]], str] | None = None,
    replan: Callable[[list[Position]], str] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield every change-feed item with ``start_lsn < lsn <= end_lsn`` exactly once.

    ``open_pages(resume_token)`` must return a page iterator starting at ``start_lsn`` when
    ``resume_token`` is None, or right after the page that produced ``resume_token``. Pages carry
    the per-physical-range positions of the SDK continuation after the page; the loop finishes
    once every physical range overlapping the requested range has advanced to ``end_lsn``.

    * Items beyond ``end_lsn`` are skipped (they belong to a later micro-batch). LSNs are
      monotonic per physical partition; after a split during the read the SDK round-robins over
      the children, which is why completion is judged per child, not on the first item > end.
    * A drained feed (HTTP 304) that has not reached ``end_lsn`` means the replica serving the
      request lags behind the one that produced the end offset; we wait and resume rather than
      return a short, non-replayable result.
    * Transient errors resume from the last fully consumed page (no duplicates, no gaps).
    * If the SDK widens the scope after a split (children outside ``[lo, hi)``), the page is
      discarded and the read resumes from a token clipped to ``[lo, hi)`` (``clip_positions``).
    * A partition that is gone (410) after the SDK's own handling, or a ``RecursionError`` from the
      SDK's change-feed code, is retried like a transient error; ``replan(positions)`` then rebuilds
      the continuation from the current topology. azure-cosmos 4.17.1 handles a 410 by calling
      ``fetch_next_block`` again without a bound (``ChangeFeedFetcherV2``), so while it keeps getting
      410 for a range it fails with ``RecursionError``; a continuation built from freshly read
      partitions holds no gone range. Any other ``RecursionError`` is not retried.
    """
    resume: str | None = None
    attempt = 0
    waited = 0.0
    lag_sleep = 0.25
    accepted: list[Position] = [Position(lo, hi, start_lsn)]
    last_positions: list[Position] = []
    rescoped = False
    while True:
        rescoped = False
        try:
            for page in open_pages(resume):
                stats.pages += 1
                stats.request_charge += page.request_charge
                if page.positions and _outside(page.positions, lo, hi):
                    if rebuild_token is None:
                        raise CosmosChangeFeedError(
                            f"Change feed of {describe} was widened beyond the requested range after a split."
                        )
                    accepted = clip_positions(page.positions, accepted, lo, hi)
                    resume = rebuild_token(accepted)
                    stats.extra["rescoped"] = stats.extra.get("rescoped", 0) + 1
                    logger.info("cosmosdb: partition split during read of %s; re-scoping the continuation", describe)
                    rescoped = True
                    break
                for item in page.items:
                    lsn = item_lsn(item, mode)
                    if lsn is None:
                        field_name = "metadata.lsn" if mode == MODE_ALL_VERSIONS_AND_DELETES else "_lsn"
                        raise CosmosChangeFeedError(
                            f"Change feed item without '{field_name}' in {describe}; cannot bound the read."
                        )
                    if lsn <= start_lsn:
                        continue  # defensive: never re-emit changes owned by an earlier batch
                    if lsn > end_lsn:
                        stats.skipped_beyond_end += 1
                        continue
                    stats.rows += 1
                    yield item
                if page.resume_token:
                    resume = page.resume_token
                attempt = 0
                if page.positions:
                    last_positions = page.positions
                    accepted = page.positions
                if all_reached(page.positions, end_lsn):
                    return
                if page.drained:
                    break
        except Exception as exc:
            recursion = sdk_recursion_error(exc)
            gone = recursion or retry.status_code(exc) == 410
            if isinstance(exc, CosmosChangeFeedError) or not (gone or retry.is_transient(exc)):
                raise
            attempt += 1
            stats.retries += 1
            if attempt > policy.max_retries:
                if recursion:
                    raise CosmosChangeFeedError(
                        f"azure-cosmos kept failing with RecursionError while reading the change feed of {describe} "
                        f"after a partition split ({policy.max_retries} retries); the task will be retried by Spark."
                    ) from exc
                raise
            if gone and replan is not None:
                resume = replan(accepted)
                stats.extra["replans"] = stats.extra.get("replans", 0) + 1
            delay = policy.delay(attempt, exc)
            retry.log_retry(f"reading the change feed of {describe}", attempt, policy.max_retries, delay, exc)
            retry.sleep(delay)
            continue
        if rescoped:
            continue

        # Drained before reaching end_lsn: replica lag (or a just-split range); wait and resume.
        if waited >= end_wait_seconds:
            observed = [p.lsn for p in last_positions]
            raise CosmosChangeFeedError(
                f"The change feed of {describe} did not reach LSN {end_lsn} within "
                f"{end_wait_seconds:.0f}s (observed positions {observed}). The replica serving reads "
                "is lagging; the task will be retried by Spark. Increase "
                "'change_feed_end_wait_seconds' or use a stronger consistency_level if this persists."
            )
        stats.lag_waits += 1
        retry.sleep(lag_sleep)
        waited += lag_sleep
        lag_sleep = min(lag_sleep * 2, 5.0)
