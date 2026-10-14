"""Snapshot stream reader (``snapshot_stream=true``): every micro-batch is one complete snapshot.

It lets an SDP append flow keep a history of container states: a flow that writes a streaming table
needs a streaming source, ``once=True`` runs only on the first update and on a full refresh, and a
materialized view replaces its contents.

Offsets are :class:`cosmosdb_datasource.offsets.SnapshotStreamOffset`. The batch ``(seq n-1, seq n]``
reads snapshot n, and every row carries ``snapshot_ts`` from the batch's logged end offset, so a
retried task or a replay keeps the value while the rows are re-read. ``snapshot_ts`` is chosen in
``latestOffset`` as ``max(now, previous + 1 ms)`` in whole milliseconds: SDP hands a timestamp
snapshot version back to a ``create_auto_cdc_from_snapshot_flow`` source function truncated to
milliseconds (observed on DBR 18.3), so a microsecond stamp would never compare equal to it.

* ``snapshot_interval_seconds``: no new snapshot until that long after the previous ``snapshot_ts``.
* ``Trigger.AvailableNow`` (PySpark >= 4.2, Databricks): ``prepareForTriggerAvailableNow()`` allows one
  new snapshot per run. PySpark 4.0/4.1 call ``latestOffset()`` without arguments and run one batch
  per AvailableNow run, so the reader tracks the last planned end offset itself.
* Read limits are ignored (``ReadAllAvailable``): splitting a snapshot would break its atomicity.

Planning is the batch reader's (current feed ranges, EPK sub-splits, the resumable query loop). Each
query carries the session token of a ``Now`` probe of its feed range taken at planning, so with
Session consistency a snapshot includes every write acknowledged before it was planned. Offsets hold
no per-range state, so splits between snapshots need no handling.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import _compat, client, epk
from cosmosdb_datasource._base import CosmosReaderBase
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.batch import (
    _CROSS_PARTITION_CLAUSES,
    CosmosQueryPartition,
    QueryStats,
    query_documents,
    split_feed_ranges,
)
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.errors import CosmosOffsetError
from cosmosdb_datasource.offsets import SnapshotStreamOffset, format_snapshot_ts
from cosmosdb_datasource.options import MODE_LATEST_VERSION, CosmosOptions
from cosmosdb_datasource.schema import SNAPSHOT_TS_COLUMN, RecordView

_AvailableNowBase: type = _compat.available_now_base()

logger = logging.getLogger("cosmosdb_datasource")

ONE_MILLISECOND = timedelta(milliseconds=1)
# Two snapshots planned this close together without AvailableNow (and with no interval) usually mean a
# continuous pipeline reading the whole container in every micro-batch.
_BACK_TO_BACK_SECONDS = 60.0


def utcnow() -> datetime:
    """Wall clock used for ``snapshot_ts`` and the interval (a module function, so tests can replace it)."""
    return datetime.now(timezone.utc)


class CosmosSnapshotStreamPartition(CosmosQueryPartition):
    """One EPK (sub-)range of one snapshot."""

    def __init__(
        self,
        index: int,
        lo: str,
        hi: str,
        feed_range: str,
        pk: PartitionKeyInfo,
        token: SecretToken | None,
        session_token: str | None,
        snapshot_ts: str,
    ) -> None:
        super().__init__(index, lo, hi, feed_range, pk, token)
        self.session_token = session_token
        self.snapshot_ts = snapshot_ts


class CosmosSnapshotStreamReader(CosmosReaderBase, DataSourceStreamReader, _AvailableNowBase):
    def __init__(self, opts: CosmosOptions, schema: StructType) -> None:
        super().__init__(opts, schema, stream=False)  # rows are documents, as in batch reads
        if SNAPSHOT_TS_COLUMN not in self._schema.fieldNames():
            logger.warning(
                "cosmosdb snapshot_stream: the schema has no %r column, so the rows of different snapshots "
                "cannot be told apart downstream.",
                SNAPSHOT_TS_COLUMN,
            )
        if opts.query_explicit and _CROSS_PARTITION_CLAUSES.search(opts.query):
            logger.warning(
                "The cosmosdb query is executed independently for every feed range; ORDER BY, GROUP BY, "
                "DISTINCT, TOP/OFFSET and aggregates apply per partition, not globally."
            )
        self._known: SnapshotStreamOffset | None = None  # last planned (or committed) end offset
        self._planned: tuple[SnapshotStreamOffset, SnapshotStreamOffset] | None = None  # (start, end) last returned
        self._reported: SnapshotStreamOffset | None = None
        self._available_now = False
        self._target_seq: int | None = None  # AvailableNow: the one snapshot this run may plan
        self._last_planned_at: float | None = None  # monotonic time of the last new snapshot (warning only)
        self._warned_back_to_back = False

    def initialOffset(self) -> dict:
        initial = SnapshotStreamOffset(0)
        if self._known is None:
            self._known = initial
        return initial.to_dict()

    def latestOffset(self, start: dict | None = None, limit: Any = None) -> dict:
        if limit is not None:
            _compat.warn_if_available_now_unsupported(_AvailableNowBase, "latestOffset(start, limit)")
        base: SnapshotStreamOffset | None
        if start is not None:
            base = SnapshotStreamOffset.from_dict(start)
        else:  # PySpark 4.0/4.1: no arguments; a new query calls this before initialOffset()
            if self._known is None:
                self.initialOffset()
            base = self._known
        assert base is not None
        if self._planned is not None and self._planned[0] == base:
            return self._planned[1].to_dict()  # asked again for the same start: the same snapshot
        if self._available_now and self._target_seq is not None and base.seq >= self._target_seq:
            return base.to_dict()  # this AvailableNow run has planned its snapshot
        now = utcnow()
        previous = base.snapshot_datetime
        interval = self._opts.snapshot_interval_seconds
        if interval > 0 and previous is not None and now < previous + timedelta(seconds=interval):
            if self._available_now:
                self._target_seq = base.seq  # nothing due in this run
            return base.to_dict()
        rid = self._container_rid()
        if base.rid is not None and base.rid != rid:
            raise self._recreated(base.rid, rid)
        now_ms = now.replace(microsecond=now.microsecond // 1000 * 1000)
        stamp = now_ms if previous is None else max(now_ms, previous + ONE_MILLISECOND)
        end = SnapshotStreamOffset(base.seq + 1, format_snapshot_ts(stamp), rid)
        if self._available_now and self._target_seq is None:
            self._target_seq = end.seq
        self._note_planned()
        self._planned = (base, end)
        self._reported = end
        logger.info("cosmosdb snapshot %d planned, snapshot_ts=%s", end.seq, end.snapshot_ts)
        return end.to_dict()

    def getDefaultReadLimit(self) -> Any:  # admission-control protocol (PySpark >= 4.2, Databricks)
        _compat.warn_if_available_now_unsupported(_AvailableNowBase, "getDefaultReadLimit()")
        return _compat.read_limit(None)  # ReadAllAvailable: a snapshot is never split across batches

    def reportLatestOffset(self) -> dict | None:  # admission-control protocol (PySpark >= 4.2, Databricks)
        return self._reported.to_dict() if self._reported is not None else None

    def prepareForTriggerAvailableNow(self) -> None:  # PySpark >= 4.2 only
        self._available_now = True
        self._target_seq = None
        logger.info("cosmosdb snapshot_stream: Trigger.AvailableNow, one new snapshot in this run")

    def partitions(self, start: dict, end: dict) -> list[CosmosSnapshotStreamPartition]:
        end_offset = SnapshotStreamOffset.from_dict(end)
        self._known = end_offset
        if start == end:
            return []
        start_offset = SnapshotStreamOffset.from_dict(start)
        if end_offset.seq != start_offset.seq + 1:
            logger.warning(
                "cosmosdb snapshot_stream: micro-batch from seq %d to %d; reading one snapshot (seq %d)",
                start_offset.seq,
                end_offset.seq,
                end_offset.seq,
            )
        assert end_offset.snapshot_ts is not None and end_offset.rid is not None
        token = self._driver_token()
        backend = self._driver_backend(token)
        props = self._driver_call("reading container properties", lambda: backend.properties(refresh=True))
        rid = str(props["_rid"])
        if rid != end_offset.rid:
            raise self._recreated(end_offset.rid, rid)
        pk = PartitionKeyInfo.from_container_properties(props)
        ranges = self._driver_call("reading feed ranges", lambda: backend.feed_ranges(force_refresh=True))
        sessions = self._session_tokens(backend, ranges)
        splits = self._splits_per_range(backend, len(ranges))
        parts = [
            CosmosSnapshotStreamPartition(
                i, lo, hi, epk.range_key(*feed), pk, token, sessions.get(feed), end_offset.snapshot_ts
            )
            for i, (feed, (lo, hi)) in enumerate(split_feed_ranges(ranges, splits, pk))
        ]
        logger.info(
            "cosmosdb snapshot %d (snapshot_ts=%s) planned: %d feed range(s), %d partition(s)",
            end_offset.seq,
            end_offset.snapshot_ts,
            len(ranges),
            len(parts),
        )
        return parts

    def read(self, partition: InputPartition | None) -> Iterator[tuple[Any, ...]]:
        if partition is None:
            return
        partition = cast(CosmosSnapshotStreamPartition, partition)
        started = time.monotonic()
        opts = self._opts
        builder = self._row_builder(snapshot=True)
        pk = partition.pk
        stats = QueryStats()
        describe = f"snapshot {partition.snapshot_ts} of range {epk.range_key(partition.lo, partition.hi)}"
        docs = query_documents(
            self._backend(partition.token),
            opts.query,
            opts.parameters_for_sdk(),
            (partition.lo, partition.hi),
            opts.page_size,
            policy=self._retry_policy(),
            stats=stats,
            describe=describe,
            session_token=partition.session_token,
            pk=partition.pk,
        )
        rows = 0
        for doc in docs:
            if isinstance(doc, Mapping):
                view = RecordView(
                    doc,
                    id=doc.get("id"),
                    pk=pk.extract(doc),
                    feed_range=partition.feed_range,
                    snapshot_ts=partition.snapshot_ts,
                )
            else:  # SELECT VALUE <scalar>
                view = RecordView(None, feed_range=partition.feed_range, snapshot_ts=partition.snapshot_ts)
                view.doc = doc
            row = builder.build(view)
            if row is not None:
                rows += 1
                yield row
        self._log_metrics(
            f"read of {describe}",
            started,
            rows=rows,
            pages=stats.pages,
            request_charge=stats.request_charge,
            retries=stats.retries,
            replans=stats.replans,
            duplicates=stats.duplicates,
        )

    def commit(self, end: dict) -> None:
        if self._known is None:
            self._known = SnapshotStreamOffset.from_dict(end)

    def stop(self) -> None:
        client.close_all()

    def _container_rid(self) -> str:
        backend = self._driver_backend(self._driver_token())
        props = self._driver_call("reading container properties", lambda: backend.properties(refresh=True))
        return str(props["_rid"])

    def _session_tokens(self, backend: Any, ranges: list[tuple[str, str]]) -> dict[tuple[str, str], str | None]:
        # Sequential like the change feed's start probes; one cheap request per physical partition.
        def probe(rng: tuple[str, str]) -> tuple[int, str | None]:
            return self._driver_call(
                f"probing the session token of range {epk.range_key(*rng)}",
                lambda: backend.resolve_start(rng, MODE_LATEST_VERSION, "Now"),
            )

        return {rng: probe(rng)[1] for rng in ranges}

    def _note_planned(self) -> None:
        now = time.monotonic()
        if (
            not self._available_now
            and not self._warned_back_to_back
            and self._opts.snapshot_interval_seconds <= 0
            and self._last_planned_at is not None
            and now - self._last_planned_at < _BACK_TO_BACK_SECONDS
        ):
            self._warned_back_to_back = True
            logger.warning(
                "cosmosdb snapshot_stream: two snapshots planned %.0fs apart; every micro-batch reads the whole "
                "container. Set snapshot_interval_seconds when the stream runs continuously.",
                now - self._last_planned_at,
            )
        self._last_planned_at = now

    @staticmethod
    def _recreated(checkpoint_rid: str, current_rid: str) -> CosmosOffsetError:
        return CosmosOffsetError(
            "The checkpointed snapshot stream belongs to a different container instance "
            f"(checkpoint rid={checkpoint_rid!r}, current rid={current_rid!r}); the container was probably "
            "deleted and re-created. Reset this stream's checkpoint (or use a new checkpoint location); the "
            "target table and its snapshot history can be kept."
        )
