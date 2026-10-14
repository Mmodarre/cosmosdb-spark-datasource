"""Batch reader: one Spark partition per feed range (optionally sub-split by EPK).

The planning helper (:func:`split_feed_ranges`) and the resumable query loop
(:func:`query_documents`) are shared with the stream reader's initial snapshot.
"""

from __future__ import annotations

import logging
import re
import time
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from pyspark.sql.datasource import DataSourceReader, InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import epk, retry
from cosmosdb_datasource._base import CosmosReaderBase
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.dedup import SeenKeys, fingerprint
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.errors import CosmosDataSourceError
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
    replans: int = 0  # scopes moved onto the children of a split partition
    duplicates: int = 0  # documents dropped because their key had been emitted already


# Documents with a _ts at least this far before the server time of the first response cannot have been
# written during the read: the allowance covers clock skew between the gateway (Date) and _ts.
RECENT_MARGIN_SECONDS = 120.0
# Re-planning a scope without reading a page from it this often means routing does not settle.
_MAX_REPLANS_WITHOUT_PROGRESS = 10


@dataclass
class _Scope:
    lo: str
    hi: str
    continuation: str | None = None  # of the last fully consumed page, from one physical partition


def _within_partitions(physical: Sequence[tuple[str, str]], scope: _Scope) -> list[_Scope]:
    return [
        _Scope(max(lo, scope.lo), min(hi, scope.hi), scope.continuation)
        for lo, hi in physical
        if lo < scope.hi and scope.lo < hi
    ]


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
    pk: PartitionKeyInfo | None = None,
) -> Iterator[Any]:
    """Yield every result of ``query`` over ``rng``, page by page, each document at most once.

    Every SDK query is scoped to one physical partition. A partition split while a scope is read
    shows as a page served by another partition key range, or as the routing map no longer mapping
    the scope to one partition. That page is dropped, and the scope is replaced by its children, each
    resumed from the continuation of the scope's last consumed page: the service continues a child
    after the last ``_rid`` returned by the parent. Left to itself, azure-cosmos 4.17.1 restarts a
    split range from its beginning (a single-partition continuation cannot be carried over to several
    children), and 4.14.0 raises 410/1002 and then sends one continuation to every child.

    With ``pk``, a document written after the read started is dropped if a document with the same
    partition key and id was emitted already: the key was deleted and re-created during the scan
    (see :mod:`cosmosdb_datasource.dedup`).

    After the SDK's own retries are exhausted, transient errors resume the scope from its last
    consumed page.
    """
    scopes: deque[_Scope] = deque([_Scope(*rng)])
    seen = SeenKeys() if pk is not None else None
    recent_from: float | None = None
    refresh = False
    attempt = 0
    stalled = 0
    try:
        while scopes:
            scope = scopes[0]
            try:
                parts = _within_partitions(backend.feed_ranges(force_refresh=refresh), scope)
                refresh = False
                if len(parts) > 1:
                    scopes.popleft()
                    scopes.extendleft(reversed(parts))
                    continue
                served: str | None = None
                moved = False
                for page in backend.query_pages(
                    query, parameters, (scope.lo, scope.hi), page_size, scope.continuation, session_token=session_token
                ):
                    stats.pages += 1
                    stats.request_charge += page.request_charge
                    ids = page.partitions
                    if (
                        len(ids) > 1
                        or (served is not None and ids and ids[0] != served)
                        or len(_within_partitions(backend.feed_ranges(), scope)) > 1
                    ):
                        moved = True
                        break
                    served = ids[0] if ids else served
                    if recent_from is None:
                        recent_from = (page.server_time or time.time()) - RECENT_MARGIN_SECONDS
                    for item in page.items:
                        if seen is not None and pk is not None and isinstance(item, Mapping):
                            fp = fingerprint(item, pk)
                            if fp is not None:
                                ts = item.get("_ts")
                                written_during_read = not isinstance(ts, int) or ts >= recent_from
                                if written_during_read and fp in seen:
                                    stats.duplicates += 1
                                    continue
                                seen.add(fp)
                        yield item
                    scope.continuation = page.continuation
                    attempt = 0
                    stalled = 0
                if moved:
                    stats.replans += 1
                    stalled += 1
                    if stalled > _MAX_REPLANS_WITHOUT_PROGRESS:
                        raise CosmosDataSourceError(
                            f"The physical partitions of {describe} kept changing while it was read; the task "
                            "will be retried by Spark."
                        )
                    refresh = True
                    logger.info("cosmosdb: partition split while querying %s; resuming the child ranges", describe)
                    continue
                scopes.popleft()
            except Exception as exc:
                if not retry.is_transient(exc):
                    raise
                attempt += 1
                stats.retries += 1
                if attempt > policy.max_retries:
                    raise
                refresh = retry.status_code(exc) == 410  # partition gone: re-read the topology
                delay = policy.delay(attempt, exc)
                retry.log_retry(f"querying {describe}", attempt, policy.max_retries, delay, exc)
                retry.sleep(delay)
    finally:
        if seen is not None:
            seen.close()


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
            pk=pk,
        )
        for doc in docs:
            if isinstance(doc, Mapping):
                view = RecordView(doc, id=doc.get("id"), pk=pk.render(doc), feed_range=partition.feed_range)
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
            replans=stats.replans,
            duplicates=stats.duplicates,
        )
