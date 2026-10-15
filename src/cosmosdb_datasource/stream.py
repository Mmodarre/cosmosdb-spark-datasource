"""Change-feed stream reader.

Micro-batch ``(start, end]`` is defined per EPK range by two integer LSNs captured on the driver
(``latestOffset``). ``read()`` returns exactly the change-feed items of the range whose LSN lies
in ``(start, end]``, so a retried task or a replayed batch after a failure reproduces the same
rows (see docs/DESIGN.md for the LatestVersion caveat).

Initial snapshot (``initial_snapshot=true``): the first micro-batch, whose start is the initial
offset P0, reads the whole container (``SELECT * FROM c`` per feed range, EPK sub-split like batch
reads) instead of change-feed rows; its change-feed end equals P0, so change-feed rows start with
batch 1. The snapshot is read only after P0 was resolved and logged (inside the end offset of batch
0), each query carries the session token observed when P0 was resolved, and no read starts earlier
than ``initial_snapshot_delay_seconds`` after the resolution. See :mod:`cosmosdb_datasource.offsets`
for the offset encoding and docs/DESIGN.md for the merge argument.

PySpark compatibility:

* 4.0 / 4.1 call ``latestOffset()`` without arguments, and on restart call
  ``partitions(offset, offset)`` (``AcceptsLatestSeenOffset.setLatestSeenOffset``), so the reader
  tracks the last planned end offset itself.
* 4.2 inspects ``latestOffset``'s signature; because it takes parameters, 4.2 treats the source as
  ``SupportsAdmissionControl`` and passes ``(start, limit)``; ``getDefaultReadLimit``,
  ``reportLatestOffset`` and ``prepareForTriggerAvailableNow`` are used there as well.
* Databricks serverless runs the 4.2+ protocol from its engine PySpark (``_engine_pyspark``) while
  ``pyspark`` in the worker is the client 4.0 package; see :mod:`cosmosdb_datasource._compat`.

Trigger.AvailableNow with an initial snapshot (4.2+): Spark captures the target before ``initialOffset()``
resolves P0, so the target is behind P0. Once batch 0 has run, the next ``latestOffset`` (whose start is
batch 0's end offset) moves the target once to the positions it has just probed. The same run then reads
the changes made while the snapshot was read, and stops at that position. On 4.0/4.1 Spark runs a single
batch per AvailableNow run, so the first run still ends after the snapshot.
"""

from __future__ import annotations

import collections
import logging
import time
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import _compat, client, epk, retry
from cosmosdb_datasource._base import (
    AVAILABLE_NOW_WRAPPER_UNSUPPORTED,
    CosmosReaderBase,
    is_empty_partition,
    unexpected_partition,
)
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.batch import CosmosQueryPartition, QueryStats, query_documents, split_feed_ranges
from cosmosdb_datasource.changefeed import ReadStats, build_composite_continuation, read_bounded, split_positions
from cosmosdb_datasource.documents import ChangeRecord, PartitionKeyInfo, to_change_record, to_snapshot_record
from cosmosdb_datasource.errors import CosmosConfigError, CosmosOffsetError
from cosmosdb_datasource.offsets import Offset, RangeState, SnapshotState, min_overlapping_lsn, rekey
from cosmosdb_datasource.options import AUTO, CosmosOptions
from cosmosdb_datasource.schema import RecordView

# SupportsTriggerAvailableNow of the PySpark that runs the reader (Databricks engine, PySpark >= 4.2),
# or a placeholder on PySpark 4.0 / 4.1, where Spark handles Trigger.AvailableNow itself.
_AvailableNowBase: type = _compat.available_now_base()

logger = logging.getLogger("cosmosdb_datasource")
logger.info(
    "cosmosdb streaming API: %s (Trigger.AvailableNow base %s.%s)",
    getattr(_compat.streaming_api_module(), "__name__", "none (PySpark 4.0/4.1 placeholder)"),
    _AvailableNowBase.__module__,
    _AvailableNowBase.__qualname__,
)

_T = TypeVar("_T")
_R = TypeVar("_R")
_MAX_PROBE_THREADS = 16
SNAPSHOT_QUERY = "SELECT * FROM c"


def _parallel_map(fn: Callable[[_T], _R], items: Sequence[_T]) -> list[_R]:
    if len(items) <= 1:
        return [fn(i) for i in items]
    with ThreadPoolExecutor(max_workers=min(_MAX_PROBE_THREADS, len(items))) as pool:
        return list(pool.map(fn, items))


def allocate_budget(starts: Sequence[int], currents: Sequence[int], cap: int | None) -> list[int]:
    """Return the end LSN per range so that the total LSN advance across ranges is about ``cap``.

    This is an approximation of an item cap: one LSN usually holds one change, but bulk and
    transactional-batch writes commit many documents under a single LSN (observed: ~50 documents
    per LSN for bulk-loaded containers), and LatestVersion collapses repeated updates. Every range
    with pending changes advances by at least one LSN so progress is guaranteed.
    """
    lags = [max(0, c - s) for s, c in zip(starts, currents, strict=True)]
    total = sum(lags)
    if cap is None or total <= cap:
        return [max(s, c) for s, c in zip(starts, currents, strict=True)]
    ends = []
    for s, lag in zip(starts, lags, strict=True):
        if lag == 0:
            ends.append(s)
            continue
        share = max(1, (cap * lag) // total)
        ends.append(s + min(lag, share))
    return ends


class CosmosChangeFeedPartition(InputPartition):
    def __init__(
        self,
        index: int,
        lo: str,
        hi: str,
        feed_range: str,
        start_lsn: int,
        end_lsn: int,
        rid: str,
        pk: PartitionKeyInfo,
        token: SecretToken | None,
        not_before: int | None = None,
    ) -> None:
        super().__init__(index)
        self.index = index
        self.lo = lo
        self.hi = hi
        self.feed_range = feed_range
        self.start_lsn = start_lsn
        self.end_lsn = end_lsn
        self.rid = rid
        self.pk = pk
        self.token = token
        self.not_before = not_before


class CosmosSnapshotPartition(CosmosQueryPartition):
    """One EPK (sub-)range of the initial snapshot, read in the first micro-batch only."""

    def __init__(
        self,
        index: int,
        lo: str,
        hi: str,
        feed_range: str,
        pk: PartitionKeyInfo,
        token: SecretToken | None,
        session_token: str | None,
    ) -> None:
        super().__init__(index, lo, hi, feed_range, pk, token)
        self.session_token = session_token


def _view(rec: ChangeRecord, feed_range: str) -> RecordView:
    return RecordView(
        rec.doc,
        id=rec.id,
        pk=rec.pk,
        operation=rec.operation,
        ts=rec.ts,
        lsn=rec.lsn,
        lsn_seq=rec.lsn_seq,
        crts=rec.crts,
        ttl_expired=rec.ttl_expired,
        feed_range=feed_range,
    )


class CosmosStreamReader(CosmosReaderBase, DataSourceStreamReader, _AvailableNowBase):
    def __init__(self, opts: CosmosOptions, schema: StructType) -> None:
        super().__init__(opts, schema, stream=True)
        if opts.query_explicit:
            raise CosmosConfigError(
                "Option 'query' does not apply to change-feed reads (streams, and spark.read with change-feed "
                "options): the change feed cannot be filtered server-side. Filter the rows in Spark instead."
            )
        self._known: Offset | None = None
        self._initial: Offset | None = None
        self._target: Offset | None = None
        self._target_refreshed = False  # AvailableNow target moved past the initial snapshot (once per run)
        self._reported: Offset | None = None
        self._resolved_at: float | None = None  # monotonic time this process resolved the initial offset
        self._initial_requested = False  # Spark called initialOffset()
        self._latest_from_initial = False  # latestOffset() without arguments started from a resolved start

    def _context(self) -> tuple[Any, SecretToken | None, str, PartitionKeyInfo]:
        token = self._driver_token()
        backend = self._driver_backend(token)
        # refresh=True detects a re-created container (new _rid)
        props = self._driver_call("reading container properties", lambda: backend.properties(refresh=True))
        return backend, token, str(props["_rid"]), PartitionKeyInfo.from_container_properties(props)

    def _feed_ranges(self, backend: Any) -> list[tuple[str, str]]:
        return self._driver_call("reading feed ranges", lambda: backend.feed_ranges(force_refresh=True))

    def _probe_current(self, backend: Any, ranges: Sequence[tuple[str, str]]) -> list[int]:
        mode = self._opts.change_feed_mode

        def probe(rng: tuple[str, str]) -> int:
            what = f"probing the change feed position of range {epk.range_key(*rng)}"
            return self._driver_call(what, lambda: backend.current_lsn(rng, mode))

        return _parallel_map(probe, list(ranges))

    def _current_offset(self, backend: Any, rid: str) -> Offset:
        ranges = self._feed_ranges(backend)
        lsns = self._probe_current(backend, ranges)
        mode = self._opts.change_feed_mode
        return Offset(rid, mode, tuple(RangeState(lo, hi, lsn) for (lo, hi), lsn in zip(ranges, lsns, strict=True)))

    def _cap(self, limit: Any) -> int | None:
        if limit is None:  # PySpark 4.0/4.1: no admission control, use the option
            return self._opts.max_items_per_trigger
        kind = type(limit).__name__
        if kind == "ReadAllAvailable":
            return None
        if kind == "ReadMaxRows":
            return int(limit.max_rows)
        logger.warning("Read limit %s is not supported by the cosmosdb source; reading all available.", kind)
        return None

    def initialOffset(self) -> dict:
        self._initial_requested = True
        return self._initial_offset()

    def _initial_offset(self) -> dict:
        # Cached: on PySpark 4.0/4.1 a brand-new query calls latestOffset() first (which needs the
        # start) and initialOffset() later when building the batch; both must see the same start.
        if self._initial is None:
            backend, _token, rid, _pk = self._context()
            opts = self._opts
            mode = opts.change_feed_mode
            start: Any = opts.start_from_datetime or opts.start_from
            ranges = self._feed_ranges(backend)

            def resolve(rng: tuple[str, str]) -> tuple[int, str | None]:
                return self._driver_call(
                    f"resolving the start position of range {epk.range_key(*rng)}",
                    lambda: backend.resolve_start(rng, mode, start),
                )

            resolved = _parallel_map(resolve, list(ranges))
            self._resolved_at = time.monotonic()
            snapshot = None
            if opts.initial_snapshot:
                snapshot = SnapshotState(
                    tuple((epk.range_key(*rng), tok) for rng, (_lsn, tok) in zip(ranges, resolved, strict=True) if tok)
                )
                if opts.start_from == "Beginning":
                    logger.info(
                        "cosmosdb: initial_snapshot=true with start_from=Beginning is redundant (the change feed "
                        "from the beginning already holds the latest version of every document); the snapshot "
                        "is read anyway."
                    )
            self._initial = Offset(
                rid,
                mode,
                tuple(RangeState(lo, hi, lsn) for (lo, hi), (lsn, _tok) in zip(ranges, resolved, strict=True)),
                initial=True,
                snapshot=snapshot,
                not_before=int(opts.start_from_datetime.timestamp()) if opts.start_from_datetime else None,
            )
            logger.info(
                "cosmosdb change feed initial offset (start_from=%s, initial_snapshot=%s): %d range(s)",
                opts.start_from,
                opts.initial_snapshot,
                len(ranges),
            )
        if self._known is None:
            self._known = self._initial
        return self._initial.to_dict()

    def latestOffset(self, start: dict | None = None, limit: Any = None) -> dict:
        if limit is not None:
            _compat.warn_if_available_now_unsupported(_AvailableNowBase, "latestOffset(start, limit)")
        base: Offset | None
        if start is not None:
            base = Offset.from_dict(start)
        else:
            if self._known is None:
                # PySpark 4.0/4.1 on a brand-new query (no checkpoint): latestOffset() is called
                # before initialOffset() (MicroBatchExecution.constructNextBatch). On a restart
                # setLatestSeenOffset -> partitions(offset, offset) has already set the start.
                self._initial_offset()
                self._latest_from_initial = True
            base = self._known
        assert base is not None
        backend, _token, rid, _pk = self._context()
        base.validate_for(rid, self._opts.change_feed_mode)
        mode = self._opts.change_feed_mode
        refined = rekey(base, self._feed_ranges(backend))
        currents = self._probe_current(backend, [(r.lo, r.hi) for r in refined.ranges])
        observed = zip(refined.ranges, currents, strict=True)
        self._reported = Offset(rid, mode, tuple(RangeState(r.lo, r.hi, max(r.lsn, c)) for r, c in observed))
        if base.initial and base.snapshot is not None:
            # First batch with an initial snapshot: it reads only the snapshot, so its change-feed end
            # equals its start (no per-trigger budget or AvailableNow target applies). The end carries
            # the original start ("init") and the snapshot record.
            return Offset(
                rid, mode, base.ranges, init=base.ranges, snapshot=base.snapshot, not_before=base.not_before
            ).to_dict()
        starts = [r.lsn for r in refined.ranges]
        ends = allocate_budget(starts, currents, self._cap(limit))
        if self._target is not None and not self._target_refreshed and base.snapshot is not None:
            # The start is the end offset of batch 0 (the initial snapshot), which has just run. Spark
            # captured the AvailableNow target before P0 was resolved, so the run would stop here and
            # the changes made while the snapshot was read would wait for the next run. Move the target
            # once to the positions probed now: this run drains them, and it still terminates.
            self._target = self._reported
            self._target_refreshed = True
            logger.info(
                "cosmosdb: Trigger.AvailableNow target moved to the current position after the initial snapshot"
            )
        if self._target is not None:
            ends = [
                max(r.lsn, min(e, min_overlapping_lsn(self._target, r.lo, r.hi)))
                for r, e in zip(refined.ranges, ends, strict=True)
            ]
        new_ranges = tuple(RangeState(r.lo, r.hi, e) for r, e in zip(refined.ranges, ends, strict=True))
        if base.initial:
            # First batch: remember where it starts, so a replay after a restart (where Spark calls
            # initialOffset() again in a new process) uses exactly the same start.
            return Offset(rid, mode, new_ranges, init=base.ranges, not_before=base.not_before).to_dict()
        end = Offset(rid, mode, new_ranges, not_before=base.not_before)
        if end.ranges == base.ranges:
            return base.to_dict()  # byte-identical: Spark sees "no new data"
        return end.to_dict()

    def getDefaultReadLimit(self) -> Any:  # admission-control protocol (PySpark >= 4.2, Databricks)
        _compat.warn_if_available_now_unsupported(_AvailableNowBase, "getDefaultReadLimit()")
        return _compat.read_limit(self._opts.max_items_per_trigger)

    def reportLatestOffset(self) -> dict | None:  # admission-control protocol (PySpark >= 4.2, Databricks)
        # Only used for the "latestOffset" in query progress, so it reuses the positions probed by the
        # last latestOffset() call (no extra requests). Defined here because the base class of client
        # PySpark 4.0 lacks it while the engine still calls it.
        return self._reported.to_dict() if self._reported is not None else None

    def prepareForTriggerAvailableNow(self) -> None:  # PySpark >= 4.2 only
        backend, _token, rid, _pk = self._context()
        self._target = self._current_offset(backend, rid)
        self._target_refreshed = False
        logger.info("cosmosdb Trigger.AvailableNow target captured for %d range(s)", len(self._target.ranges))

    def partitions(self, start: dict, end: dict) -> list[CosmosChangeFeedPartition | CosmosSnapshotPartition]:
        end_offset = Offset.from_dict(end)
        if start == end and self._latest_from_initial and not self._initial_requested:
            raise CosmosConfigError(AVAILABLE_NOW_WRAPPER_UNSUPPORTED)
        self._known = end_offset
        if start == end:
            return []
        start_offset = Offset.from_dict(start)
        # The snapshot belongs to the first batch, and only if that batch was planned with one: the
        # decision and the session tokens come from the logged end offset (see offsets module doc).
        snapshot = end_offset.snapshot if start_offset.initial else None
        if start_offset.initial:
            self._log_snapshot_decision(start_offset, end_offset)
        if start_offset.initial and end_offset.init is not None:
            if not start_offset.same_position(Offset(end_offset.rid, end_offset.mode, end_offset.init)):
                logger.info("cosmosdb: replaying the first micro-batch from its original start position")
            start_offset = Offset(end_offset.rid, end_offset.mode, end_offset.init)
        if start_offset.rid != end_offset.rid or start_offset.mode != end_offset.mode:
            raise CosmosOffsetError("Start and end offsets belong to different containers or change feed modes.")
        backend, token, rid, pk = self._context()
        end_offset.validate_for(rid, self._opts.change_feed_mode)
        factor = self._opts.partitions_per_feed_range
        splits = 1 if factor == AUTO else int(factor)
        parts: list[CosmosChangeFeedPartition | CosmosSnapshotPartition] = []
        if snapshot is not None:
            parts.extend(self._snapshot_partitions(backend, token, pk, snapshot))
        pieces = epk.overlay([(r.lo, r.hi) for r in start_offset.ranges], [(r.lo, r.hi) for r in end_offset.ranges])
        for lo, hi, i, j in pieces:
            s_lsn = start_offset.ranges[i].lsn
            e_lsn = end_offset.ranges[j].lsn
            if e_lsn < s_lsn:
                raise CosmosOffsetError(
                    f"End LSN {e_lsn} is behind start LSN {s_lsn} for range {epk.range_key(lo, hi)}."
                )
            if e_lsn == s_lsn:
                continue
            key = end_offset.ranges[j].key
            for a, b in epk.split_range(lo, hi, splits, kind=pk.kind, version=pk.version):
                parts.append(
                    CosmosChangeFeedPartition(
                        len(parts), a, b, key, s_lsn, e_lsn, rid, pk, token, not_before=end_offset.not_before
                    )
                )
        if snapshot is not None:
            self._await_snapshot_delay()
        logger.info("cosmosdb micro-batch planned: %d partition(s)", len(parts))
        return self._with_budget(parts)

    def _log_snapshot_decision(self, start: Offset, end: Offset) -> None:
        pending, planned = start.snapshot is not None, end.snapshot is not None
        if pending and not planned:
            logger.warning(
                "cosmosdb: initial_snapshot=true, but the first micro-batch of this checkpoint was planned "
                "without a snapshot (its offset was written by an earlier version, or the option was enabled "
                "after the batch was planned). No snapshot is read; use a new checkpoint location to read one."
            )
        elif planned and not pending:
            logger.info(
                "cosmosdb: replaying the first micro-batch with the initial snapshot it was planned with "
                "(initial_snapshot is now false)."
            )

    def _snapshot_partitions(
        self, backend: Any, token: SecretToken | None, pk: PartitionKeyInfo, snapshot: SnapshotState
    ) -> list[CosmosSnapshotPartition]:
        # Current physical partitions, like a batch read: they tile the key space whether or not a
        # partition was split since the start was resolved.
        ranges = self._feed_ranges(backend)
        splits = self._splits_per_range()
        parts = [
            CosmosSnapshotPartition(i, lo, hi, epk.range_key(*feed), pk, token, snapshot.session_token_for(*feed))
            for i, (feed, (lo, hi)) in enumerate(split_feed_ranges(ranges, splits, pk))
        ]
        pinned = sum(1 for p in parts if p.session_token)
        logger.info(
            "cosmosdb initial snapshot planned: %d feed range(s), %d partition(s), %d with a session token",
            len(ranges),
            len(parts),
            pinned,
        )
        return parts

    def _await_snapshot_delay(self) -> None:
        """Block planning until ``initial_snapshot_delay_seconds`` after the start was resolved.

        The original resolution happened in this process (first attempt) or before this process
        re-resolved the start (replay after a restart), so waiting from this process's own resolution
        is never too short. Monotonic clock: no wall-clock skew between machines is involved.
        """
        delay = self._opts.initial_snapshot_delay_seconds
        if delay <= 0:
            return
        remaining = delay if self._resolved_at is None else self._resolved_at + delay - time.monotonic()
        remaining = min(delay, remaining)
        if remaining > 0:
            logger.info("cosmosdb: waiting %.1fs before reading the initial snapshot", remaining)
            retry.sleep(remaining)

    def read(self, partition: InputPartition | None) -> Iterator[tuple[Any, ...]]:
        if is_empty_partition(partition):
            return
        if isinstance(partition, CosmosSnapshotPartition):
            yield from self._read_snapshot(partition)
            return
        if not isinstance(partition, CosmosChangeFeedPartition):
            raise unexpected_partition(partition)
        started = time.monotonic()
        opts = self._opts
        mode = opts.change_feed_mode
        with self._task_backend(partition.token, partition) as backend:
            builder = self._row_builder()
            pk = partition.pk
            rng = (partition.lo, partition.hi)
            stats = ReadStats()
            describe = f"range {epk.range_key(*rng)} LSN ({partition.start_lsn}, {partition.end_lsn}]"

            def open_pages(resume: str | None) -> Iterator[Any]:
                return backend.change_feed_pages(
                    rng, mode, partition.start_lsn, opts.page_size, resume, container_rid=partition.rid
                )

            items = read_bounded(
                open_pages,
                start_lsn=partition.start_lsn,
                end_lsn=partition.end_lsn,
                mode=mode,
                policy=self._retry_policy(),
                end_wait_seconds=opts.change_feed_end_wait_seconds,
                stats=stats,
                describe=describe,
                lo=partition.lo,
                hi=partition.hi,
                rebuild_token=lambda children: build_composite_continuation(
                    partition.rid, mode, partition.lo, partition.hi, children
                ),
                replan=lambda positions: build_composite_continuation(
                    partition.rid,
                    mode,
                    partition.lo,
                    partition.hi,
                    split_positions(positions, backend.feed_ranges(force_refresh=True), partition.lo, partition.hi),
                ),
            )
            emitted = 0
            skipped_before_start = 0
            group_lsn: int | None = None
            group: collections.Counter[tuple[str | None, str | None]] = collections.Counter()
            for item in items:
                rec = to_change_record(item, mode, pk)
                # The feed is in LSN order and a page holds whole LSN groups, so the versions of a document
                # at one LSN are adjacent; pages of other partitions after a split hold other documents.
                if rec.lsn != group_lsn:
                    group_lsn = rec.lsn
                    group.clear()
                rec.lsn_seq = group[(rec.pk, rec.id)]
                group[(rec.pk, rec.id)] += 1
                # A start_from later than the position the stream started at: the service has no position for
                # a future time, so the stream starts at "now" and the earlier changes are dropped here.
                if partition.not_before is not None and rec.ts is not None and rec.ts < partition.not_before:
                    skipped_before_start += 1
                    continue
                row = builder.build(_view(rec, partition.feed_range))
                if row is not None:
                    emitted += 1
                    yield row
            self._log_metrics(
                f"change feed read of {describe}",
                started,
                rows=emitted,
                pages=stats.pages,
                request_charge=stats.request_charge,
                retries=stats.retries,
                lag_waits=stats.lag_waits,
                skipped_beyond_end=stats.skipped_beyond_end,
                skipped_before_start=skipped_before_start,
            )

    def _read_snapshot(self, partition: CosmosSnapshotPartition) -> Iterator[tuple[Any, ...]]:
        started = time.monotonic()
        builder = self._row_builder()
        stats = QueryStats()
        describe = f"range {epk.range_key(partition.lo, partition.hi)}"
        with self._task_backend(partition.token, partition) as backend:
            docs = query_documents(
                backend,
                SNAPSHOT_QUERY,
                None,
                (partition.lo, partition.hi),
                self._opts.page_size,
                policy=self._retry_policy(),
                stats=stats,
                describe=f"the initial snapshot of {describe}",
                session_token=partition.session_token,
                pk=partition.pk,
            )
            rows = 0
            for doc in docs:
                row = builder.build(_view(to_snapshot_record(doc, partition.pk), partition.feed_range))
                if row is not None:
                    rows += 1
                    yield row
            self._log_metrics(
                f"initial snapshot read of {describe}",
                started,
                rows=rows,
                pages=stats.pages,
                request_charge=stats.request_charge,
                retries=stats.retries,
                replans=stats.replans,
                duplicates=stats.duplicates,
                session_token=partition.session_token is not None,
            )

    def commit(self, end: dict) -> None:
        if self._known is None:
            self._known = Offset.from_dict(end)

    def stop(self) -> None:
        client.close_all()
