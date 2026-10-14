"""Change feed as a batch read: the changes between two positions, optionally continued from a checkpoint.

``spark.read`` with change-feed options reads, per feed range, the changes with ``start < lsn <= end``,
using the stream reader's planning and LSN-bounded read. The start is ``start_from`` or, with
``batch_checkpoint_location``, the end of the last committed read. The end is the position probed at
planning, or the last change before ``end_at``, capped by ``max_items_per_trigger`` (an LSN budget).

A batch read has no hook that runs after its output was written, so the checkpoint advances only when the
job says so (:func:`commit_batch_checkpoint`). Planning records the range in ``offsets/<n>`` before it is
read; while ``commits/<n>`` is missing, every new read with the checkpoint plans that same range again,
so a failed or repeated job reads the same changes (the stream's replay rule). A read that consumed its
whole range reports ``caught_up``, which ends a loop of capped reads::

    while True:
        df = spark.read.format("cosmosdb").options(..., batch_checkpoint_location=path, max_items_per_trigger=...)
        df.load().write...
        if commit_batch_checkpoint(path):
            break

The checkpoint is a directory written with Python file APIs on the driver: a local path, or a FUSE path
such as ``/Volumes/...`` or ``/dbfs/...`` on Databricks.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import epk
from cosmosdb_datasource._base import CosmosReaderBase
from cosmosdb_datasource.batch_checkpoint import BatchCheckpoint, PlannedRange
from cosmosdb_datasource.offsets import Offset, RangeState, rekey
from cosmosdb_datasource.options import MODE_LATEST_VERSION, CosmosOptions, parse_timestamp_option
from cosmosdb_datasource.stream import CosmosStreamReader, allocate_budget

logger = logging.getLogger("cosmosdb_datasource")


class CosmosChangeFeedBatchReader(CosmosReaderBase, DataSourceReader):
    def __init__(self, opts: CosmosOptions, schema: StructType) -> None:
        super().__init__(opts, schema, stream=True)
        self._feed = CosmosStreamReader(opts, schema)

    def partitions(self) -> list[InputPartition]:
        opts = self._opts
        checkpoint = BatchCheckpoint(opts.batch_checkpoint_location) if opts.batch_checkpoint_location else None
        latest = checkpoint.latest() if checkpoint is not None else None
        if checkpoint is not None and latest is not None and not checkpoint.committed(latest.index):
            logger.info("cosmosdb change-feed batch read: reading the uncommitted range offsets/%d again", latest.index)
            planned = latest
        else:
            start = latest.end if latest is not None else Offset.from_dict(self._feed.initialOffset())
            planned = self._plan(start, latest.index + 1 if latest is not None else 0)
            if checkpoint is not None:
                checkpoint.plan(planned)
        parts = self._feed.partitions(planned.start.to_dict(), planned.end.to_dict())
        logger.info(
            "cosmosdb change-feed batch read planned: %d partition(s), caught_up=%s", len(parts), planned.caught_up
        )
        return list(parts)

    def _plan(self, start: Offset, index: int) -> PlannedRange:
        feed = self._feed
        backend, _token, rid, _pk = feed._context()
        start.validate_for(rid, self._opts.change_feed_mode)
        refined = rekey(start, feed._feed_ranges(backend))
        ranges = [(r.lo, r.hi) for r in refined.ranges]
        if self._opts.end_at is not None:
            end_at = parse_timestamp_option("end_at", self._opts.end_at)
            targets = [self._position_before(backend, rng, end_at) for rng in ranges]
        else:
            targets = feed._probe_current(backend, ranges)
        starts = [r.lsn for r in refined.ranges]
        ends = allocate_budget(starts, targets, self._opts.max_items_per_trigger)
        caught_up = all(e >= t for e, t in zip(ends, targets, strict=True))
        end = Offset(
            rid,
            start.mode,
            tuple(RangeState(lo, hi, e) for (lo, hi), e in zip(ranges, ends, strict=True)),
            init=start.ranges if start.initial else None,
        )
        return PlannedRange(index, start, end, caught_up)

    def _position_before(self, backend: Any, rng: tuple[str, str], when: datetime) -> int:
        what = f"resolving the end position of range {epk.range_key(*rng)}"
        lsn, _session = self._driver_call(what, lambda: backend.resolve_start(rng, MODE_LATEST_VERSION, when))
        return int(lsn)

    def read(self, partition: InputPartition | None) -> Iterator[tuple[Any, ...]]:
        if partition is None:
            return iter(())
        return self._feed.read(partition)
