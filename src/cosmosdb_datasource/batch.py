"""Batch reader: one Spark partition per feed range (optionally sub-split by EPK).

The planning helper (:func:`split_feed_ranges`) and the resumable query loop
(:func:`query_documents`) are shared with the stream reader's initial snapshot.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import epk, retry
from cosmosdb_datasource._base import CosmosReaderBase
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.options import CosmosOptions
from cosmosdb_datasource.schema import RecordView

logger = logging.getLogger("cosmosdb_datasource")

# Clauses that are evaluated per partition when a query is fanned out by feed range; results are
# then not globally ordered/aggregated/limited.
_CROSS_PARTITION_CLAUSES = re.compile(
    r"\b(ORDER\s+BY|GROUP\s+BY|DISTINCT|TOP\s+\d+|OFFSET\s+\d+|COUNT\s*\(|SUM\s*\(|AVG\s*\(|MIN\s*\(|MAX\s*\()",
    re.IGNORECASE,
)


class CosmosQueryPartition(InputPartition):
    def __init__(
        self,
        index: int,
        lo: str,
        hi: str,
        feed_range: str,
        pk: PartitionKeyInfo,
        token: SecretToken | None,
    ) -> None:
        super().__init__(index)
        self.index = index
        self.lo = lo
        self.hi = hi
        self.feed_range = feed_range
        self.pk = pk
        self.token = token


def split_feed_ranges(
    ranges: Sequence[tuple[str, str]], splits: int, pk: PartitionKeyInfo
) -> list[tuple[tuple[str, str], tuple[str, str]]]:
    """Return ``(feed range, sub-range)`` pairs: every feed range EPK-split into ``splits`` sub-ranges."""
    out: list[tuple[tuple[str, str], tuple[str, str]]] = []
    for lo, hi in ranges:
        out.extend(((lo, hi), sub) for sub in epk.split_range(lo, hi, splits, kind=pk.kind, version=pk.version))
    return out


@dataclass
class QueryStats:
    pages: int = 0
    request_charge: float = 0.0
    retries: int = 0


def query_documents(
    backend: Any,
    query: str,
    parameters: list[dict[str, Any]] | None,
    rng: tuple[str, str],
    page_size: int,
    *,
    policy: retry.RetryPolicy,
    stats: QueryStats,
    describe: str,
    session_token: str | None = None,
) -> Iterator[Any]:
    """Yield every result of ``query`` over ``rng``, page by page.

    After the SDK's own retries are exhausted, transient errors resume from the last fully consumed
    page's continuation, so no result is yielded twice and none is skipped. Partition splits during
    the query are handled by the SDK (the feed range is intersected with the child partitions).
    """
    continuation: str | None = None
    attempt = 0
    while True:
        try:
            for page in backend.query_pages(
                query, parameters, rng, page_size, continuation, session_token=session_token
            ):
                stats.pages += 1
                stats.request_charge += page.request_charge
                yield from page.items
                continuation = page.continuation
                attempt = 0
            return
        except Exception as exc:
            if not retry.is_transient(exc):
                raise
            attempt += 1
            stats.retries += 1
            if attempt > policy.max_retries:
                raise
            delay = policy.delay(attempt, exc)
            retry.log_retry(f"querying {describe}", attempt, policy.max_retries, delay, exc)
            retry.sleep(delay)


class CosmosBatchReader(CosmosReaderBase, DataSourceReader):
    def __init__(self, opts: CosmosOptions, schema: StructType) -> None:
        super().__init__(opts, schema, stream=False)
        if _CROSS_PARTITION_CLAUSES.search(opts.query):
            logger.warning(
                "The cosmosdb query is executed independently for every feed range; ORDER BY, GROUP BY, "
                "DISTINCT, TOP/OFFSET and aggregates apply per partition, not globally. Do them in Spark."
            )

    def partitions(self) -> list[CosmosQueryPartition]:
        started = time.monotonic()
        token = self._driver_token()
        backend = self._driver_backend(token)
        props = self._driver_call("reading container properties", lambda: backend.properties(refresh=True))
        pk = PartitionKeyInfo.from_container_properties(props)
        ranges = self._driver_call("reading feed ranges", lambda: backend.feed_ranges(force_refresh=True))
        splits = self._splits_per_range(backend, len(ranges))
        parts = [
            CosmosQueryPartition(i, lo, hi, epk.range_key(*feed), pk, token)
            for i, (feed, (lo, hi)) in enumerate(split_feed_ranges(ranges, splits, pk))
        ]
        self._log_metrics("batch planning", started, feed_ranges=len(ranges), partitions=len(parts))
        logger.info("cosmosdb batch read planned: %d feed range(s), %d partition(s)", len(ranges), len(parts))
        return parts

    def read(self, partition: InputPartition | None) -> Iterator[tuple[Any, ...]]:
        if partition is None:  # Spark substitutes [None] for an empty partition list
            return
        partition = cast(CosmosQueryPartition, partition)
        started = time.monotonic()
        opts = self._opts
        builder = self._row_builder()
        pk = partition.pk
        describe = f"range {epk.range_key(partition.lo, partition.hi)}"
        stats = QueryStats()
        rows = 0
        docs = query_documents(
            self._backend(partition.token),
            opts.query,
            opts.parameters_for_sdk(),
            (partition.lo, partition.hi),
            opts.page_size,
            policy=self._retry_policy(),
            stats=stats,
            describe=describe,
        )
        for doc in docs:
            if isinstance(doc, Mapping):
                view = RecordView(doc, id=doc.get("id"), pk=pk.extract(doc), feed_range=partition.feed_range)
            else:  # SELECT VALUE <scalar>
                view = RecordView(None, feed_range=partition.feed_range)
                view.doc = doc
            row = builder.build(view)
            if row is not None:
                rows += 1
                yield row
        self._log_metrics(
            f"batch read of {describe}",
            started,
            rows=rows,
            pages=stats.pages,
            request_charge=stats.request_charge,
            retries=stats.retries,
        )
