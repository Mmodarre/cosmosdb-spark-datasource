"""Reads of one logical partition (``partition_key``) and of given items (``item_ids``, :func:`read_many`).

A query routed by partition key (``query_items(partition_key=...)``, the ``x-ms-documentdb-partitionkey``
header) reads only that logical partition: 4.3 RU for its 50 documents, measured on an account. The same
query scoped by the key's feed range (``feed_range_from_partition_key``, an EPK point range) cost 2,386 RU,
because an EPK-range query scans its whole physical partition. Logical-partition reads are therefore
routed by partition key, one Spark task per key, through the batch query loop (resume from the last page,
each document once).

Items are read with ``ContainerProxy.read_items``, which groups them by physical partition and reads a
group per request; items that do not exist are not returned. Planning groups the identities by the
physical partition their key hashes to, one Spark task per partition.
"""

from __future__ import annotations

import bisect
import json
from collections.abc import Iterator, Sequence
from typing import Any

from pyspark.sql.datasource import InputPartition

from cosmosdb_datasource import epk
from cosmosdb_datasource.auth import SecretToken
from cosmosdb_datasource.backend import QueryPage
from cosmosdb_datasource.batch import CosmosQueryPartition
from cosmosdb_datasource.documents import PartitionKeyInfo

ITEMS_PER_REQUEST = 1000  # identities per read_items call, which bounds a task's memory


class CosmosLogicalPartition(CosmosQueryPartition):
    """One logical partition (a full partition key, or a prefix of a hierarchical one), read routed by key."""

    def __init__(
        self, index: int, key_json: str, feed_range: str, pk: PartitionKeyInfo, token: SecretToken | None
    ) -> None:
        super().__init__(index, epk.MIN_EPK, epk.MAX_EPK, feed_range, pk, token)
        self.key_json = key_json

    @property
    def key(self) -> Any:
        return json.loads(self.key_json)


class CosmosItemsPartition(InputPartition):
    """The identities ``(id, partition key)`` of one physical partition."""

    def __init__(
        self,
        index: int,
        feed_range: str,
        items: Sequence[tuple[str, str]],
        pk: PartitionKeyInfo,
        token: SecretToken | None,
    ) -> None:
        super().__init__(index)
        self.index = index
        self.feed_range = feed_range
        self.items = tuple(items)  # (id, key as JSON)
        self.pk = pk
        self.token = token

    def __repr__(self) -> str:  # identities can be numerous
        return f"CosmosItemsPartition(index={self.index}, feed_range={self.feed_range!r}, items={len(self.items)})"


class KeyRouted:
    """Backend view in which every query is routed to one logical partition (a full key).

    ``batch.query_documents`` scopes queries by EPK range and checks the physical partitions while it
    reads. Routed by partition key, a query belongs to one physical partition even across a split (the
    service routes the continuation), so the view reports a single range covering the key space.
    """

    def __init__(self, backend: Any, key: Any) -> None:
        self._backend = backend
        self._key = key

    def feed_ranges(self, force_refresh: bool = False) -> list[tuple[str, str]]:
        return [(epk.MIN_EPK, epk.MAX_EPK)]

    def query_pages(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None,
        rng: tuple[str, str],
        page_size: int,
        continuation: str | None = None,
        session_token: str | None = None,
    ) -> Iterator[QueryPage]:
        return self._backend.query_partition_pages(query, parameters, self._key, page_size, continuation)


def group_items(
    items: Sequence[tuple[str, str]], ranges: Sequence[tuple[str, str]], key_epk: Any
) -> list[tuple[tuple[str, str], list[tuple[str, str]]]]:
    """Group ``(id, key JSON)`` pairs by the feed range that holds their key, dropping repeated pairs."""
    bounds = [lo for lo, _hi in ranges]  # sorted, and the ranges tile the key space
    owners: dict[str, tuple[str, str]] = {}  # partition key JSON -> feed range
    groups: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for item in dict.fromkeys(items):
        owner = owners.get(item[1])
        if owner is None:
            point = key_epk(json.loads(item[1]))
            owner = ranges[bisect.bisect_right(bounds, point) - 1]
            if not owner[0] <= point < owner[1]:
                raise ValueError(f"No feed range holds the effective partition key {point!r}.")
            owners[item[1]] = owner
        groups.setdefault(owner, []).append(item)
    return sorted(groups.items())


def chunks(items: Sequence[tuple[str, str]], size: int = ITEMS_PER_REQUEST) -> Iterator[list[tuple[str, Any]]]:
    for start in range(0, len(items), size):
        yield [(item_id, json.loads(key)) for item_id, key in items[start : start + size]]
