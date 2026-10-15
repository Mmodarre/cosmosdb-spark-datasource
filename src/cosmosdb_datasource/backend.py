"""Thin adapter over ``azure.cosmos.ContainerProxy``.

Readers depend only on this small interface (also implemented by the test fake), which keeps
SDK-specific behaviour in one place:

* ``properties()``                 container properties (``_rid``, ``partitionKey``)
* ``feed_ranges(force_refresh)``   physical partition EPK ranges, sorted
* ``query_pages(...)``             query pages for one EPK range, resumable by continuation,
                                   optionally pinned to a session token; each page names the
                                   physical partitions that served it; read ahead on a thread
* ``change_feed_pages(...)``       change-feed pages for one EPK range from an LSN, resumable
* ``current_lsn(rng, mode)``       current change-feed position without reading data
* ``resolve_start(rng, mode, s)``  LSN position for Beginning / Now / a timestamp, plus the session
                                   token of the response that determined it
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

from cosmosdb_datasource import epk
from cosmosdb_datasource.changefeed import (
    ChangeFeedPage,
    build_continuation,
    decode_positions,
    parse_etag_lsn,
)
from cosmosdb_datasource.diagnostics import SlowRequestLog
from cosmosdb_datasource.documents import item_lsn
from cosmosdb_datasource.errors import CosmosChangeFeedError, CosmosDataSourceError, CosmosOffsetError
from cosmosdb_datasource.options import MODE_LATEST_VERSION
from cosmosdb_datasource.prefetch import prefetched

logger = logging.getLogger("cosmosdb_datasource")

SESSION_TOKEN_HEADER = "x-ms-session-token"
PARTITION_KEY_RANGE_HEADER = "x-ms-documentdb-partitionkeyrangeid"


@dataclass
class QueryPage:
    items: list[dict[str, Any]]
    continuation: str | None
    request_charge: float = 0.0
    partitions: tuple[str, ...] = ()  # partition key range ids of the responses that make up the page
    server_time: float | None = None  # epoch seconds, from the first response's Date header
    # The physical partitions the client's routing map held right after the page was fetched (EPK-range
    # queries only), read under the same lock as the page.
    feed_ranges: list[tuple[str, str]] | None = None


def _server_time(headers: Any) -> float | None:
    try:
        value = headers.get("Date") or headers.get("date")
        return parsedate_to_datetime(value).timestamp() if value else None
    except (TypeError, ValueError, AttributeError):
        return None


def feed_range_dict(lo: str, hi: str) -> dict[str, Any]:
    """Return the feed range in the dict form accepted by ``feed_range=`` (``FeedRangeInternalEpk.to_dict``)."""
    return {"Range": {"min": lo, "max": hi, "isMinInclusive": True, "isMaxInclusive": False}}


def _continuation(headers: Any) -> str | None:
    """Return the change-feed continuation of one pager from its last response's headers.

    ``ChangeFeedFetcherV2.fetch_change_feed_items`` writes the pager's composite state into the
    ``etag`` of the response headers after the response hook received that same object (azure-cosmos
    4.14.0 to 4.17.1). The pager's own ``continuation_token`` and the drained state after an HTTP
    304 come from the client-wide ``last_response_headers`` instead, which concurrent requests on the
    shared client overwrite.
    """
    return headers.get("etag") if headers is not None else None


def _query_continuation(headers: Any) -> str | None:
    """Return the query continuation of one pager from its last response's headers.

    The pager's ``continuation_token`` is read from the client-wide ``last_response_headers``
    (``QueryIterable._unpack``), which a concurrent query on the shared client may have replaced
    (azure-cosmos 4.14.0 to 4.17.1). The response's own headers hold the same token: the backend's
    in 4.14.0, and the outbound token that 4.17.1 writes into the last response's headers after the
    response hook received them.
    """
    return headers.get("x-ms-continuation") if headers is not None else None


def _charge(headers: Any) -> float:
    try:
        return float(headers.get("x-ms-request-charge") or 0.0)
    except (TypeError, ValueError, AttributeError):
        return 0.0


class SdkBackend:
    def __init__(
        self,
        container: Any,
        *,
        priority: str | None = None,
        prefetch_pages: int = 0,
        cache_staleness_ms: int | None = None,
        slow_requests: SlowRequestLog | None = None,
        sdk_lock: threading.RLock | None = None,
    ) -> None:
        self._container = container
        # azure-cosmos clients are not safe for concurrent requests (4.14.0 to 4.17.1): every response
        # replaces the client-wide ``last_response_headers``, which pagers read their continuation from,
        # and 4.17.1 writes throttle counters into that object after each request while the query path
        # copies it, which fails with "OrderedDict mutated during iteration". Requests on one client are
        # therefore serialized, also across backends of different containers, which share the client's
        # lock. The read-ahead thread holds it only while it fetches a page and reads the routing map after
        # it. Change-feed position probes are the exception (see ``_probe``).
        self._sdk_lock = sdk_lock if sdk_lock is not None else threading.RLock()
        self._priority = priority
        self._prefetch_pages = prefetch_pages
        self._cache_staleness_ms = cache_staleness_ms
        self._slow = slow_requests
        self._properties: dict[str, Any] | None = None

    def _kwargs(self) -> dict[str, Any]:
        return {"priority": self._priority} if self._priority else {}

    def properties(self, refresh: bool = False) -> dict[str, Any]:
        if self._properties is None or refresh:
            with self._sdk_lock:
                self._properties = dict(self._container.read())
        return self._properties

    @property
    def rid(self) -> str:
        return str(self.properties()["_rid"])

    def feed_ranges(self, force_refresh: bool = False) -> list[tuple[str, str]]:
        # Even without force_refresh this can send a request: the SDK reads the partition key ranges when its
        # routing-map cache is empty, as after its handling of a split.
        with self._sdk_lock:
            return self._feed_ranges(force_refresh)

    def _feed_ranges(self, force_refresh: bool) -> list[tuple[str, str]]:
        ranges = []
        feed_ranges = list(self._container.read_feed_ranges(force_refresh=force_refresh))
        for fr in feed_ranges:
            rng = fr["Range"]
            ranges.append((str(rng["min"]).upper(), str(rng["max"]).upper()))
        ranges.sort()
        if not epk.is_cover(ranges):
            raise CosmosDataSourceError(f"Feed ranges returned by the service do not tile the key space: {ranges}")
        return ranges

    def query_pages(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None,
        rng: tuple[str, str],
        page_size: int,
        continuation: str | None = None,
        session_token: str | None = None,
    ) -> Iterator[QueryPage]:
        """Yield the query pages of ``rng``, ``prefetch_pages`` ahead of the consumer.

        ``session_token`` (Session consistency only; the SDK drops it for other levels) makes the
        service serve every page from a replica that has applied at least that session's writes;
        otherwise it answers 404/1002, which the SDK and :mod:`cosmosdb_datasource.retry` retry.
        """
        scope = {"feed_range": feed_range_dict(*rng)}
        return prefetched(
            self._query_pages(query, parameters, scope, page_size, continuation, session_token), self._prefetch_pages
        )

    def query_partition_pages(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None,
        partition_key: Any,
        page_size: int,
        continuation: str | None = None,
    ) -> Iterator[QueryPage]:
        """Yield the query pages of one logical partition (routed by partition key, not by EPK range)."""
        scope = {"partition_key": partition_key}
        return prefetched(
            self._query_pages(query, parameters, scope, page_size, continuation, None), self._prefetch_pages
        )

    def read_items(self, items: list[tuple[str, Any]]) -> tuple[list[dict[str, Any]], float]:
        """Return the existing documents among ``(id, partition key)`` pairs, and the request charge."""
        responses: list[Any] = []
        started = time.monotonic()
        with self._sdk_lock:
            # max_concurrency=1: the SDK would otherwise run the queries of one call in parallel on this
            # client (azure-cosmos 4.14.0 to 4.17.1), which is unsafe (see __init__).
            docs = self._container.read_items(
                items=items,
                max_concurrency=1,
                response_hook=lambda headers, _body: responses.append(headers),
                **self._kwargs(),
            )
        if self._slow is not None:
            self._slow.observe("read_items", time.monotonic() - started, responses, items=len(items))
        return [dict(d) for d in docs], sum(_charge(h) for h in responses)

    def key_epk(self, partition_key: Any) -> str:
        """Return the effective partition key of a (full) partition key value."""
        return self.key_range(partition_key)[0]

    def key_range(self, partition_key: Any) -> tuple[str, str]:
        """Return the EPK range of a partition key value or of a prefix of a hierarchical key."""
        with self._sdk_lock:
            rng = self._container.feed_range_from_partition_key(partition_key)["Range"]
        return str(rng["min"]).upper(), str(rng["max"]).upper()

    def _query_pages(
        self,
        query: str,
        parameters: list[dict[str, Any]] | None,
        scope: dict[str, Any],
        page_size: int,
        continuation: str | None,
        session_token: str | None,
    ) -> Iterator[QueryPage]:
        charges: list[float] = []
        served: list[str] = []
        times: list[float | None] = []
        responses: list[Any] = []

        def hook(headers: Any, _body: Any) -> None:
            charges.append(_charge(headers))
            pk_range = headers.get(PARTITION_KEY_RANGE_HEADER)
            if pk_range and str(pk_range) not in served:
                served.append(str(pk_range))
            times.append(_server_time(headers))
            responses.append(headers)

        kwargs: dict[str, Any] = dict(self._kwargs())
        if parameters:
            kwargs["parameters"] = parameters
        if session_token:
            kwargs["session_token"] = session_token
        if self._cache_staleness_ms is not None:
            kwargs["max_integrated_cache_staleness_in_ms"] = self._cache_staleness_ms
        with self._sdk_lock:
            pager = self._container.query_items(
                query=query, max_item_count=page_size, response_hook=hook, **scope, **kwargs
            )
            pages = pager.by_page(continuation_token=continuation) if continuation else pager.by_page()
        fetch_started = time.monotonic()
        while True:
            with self._sdk_lock:
                page = next(pages, None)
                topology = self._feed_ranges(False) if page is not None and "feed_range" in scope else None
            if page is None:
                return
            items = list(page)
            if self._slow is not None:
                rng = scope.get("feed_range", {}).get("Range")
                where = f"{rng['min']}-{rng['max']}" if rng else f"partition_key={scope.get('partition_key')!r}"
                self._slow.observe(
                    "query page", time.monotonic() - fetch_started, responses, items=len(items), scope=where
                )
            yield QueryPage(
                items=items,
                continuation=_query_continuation(responses[-1] if responses else None),
                request_charge=sum(charges),
                partitions=tuple(served),
                server_time=next((t for t in times if t is not None), None),
                feed_ranges=topology,
            )
            charges.clear()
            served.clear()
            times.clear()
            responses.clear()
            fetch_started = time.monotonic()

    def change_feed_pages(
        self,
        rng: tuple[str, str],
        mode: str,
        start_lsn: int,
        page_size: int,
        resume_token: str | None = None,
        container_rid: str | None = None,
    ) -> Iterator[ChangeFeedPage]:
        charges: list[float] = []
        last: list[Any] = [None]
        responses: list[Any] = []

        def hook(headers: Any, _body: Any) -> None:
            charges.append(_charge(headers))
            last[0] = headers
            responses.append(headers)

        if container_rid is not None:
            self.ensure_rid(container_rid)
        rid = container_rid or self.rid
        token = resume_token or build_continuation(rid, mode, rng[0], rng[1], start_lsn)
        with self._sdk_lock:
            pager = self._container.query_items_change_feed(
                continuation=token, max_item_count=page_size, response_hook=hook, **self._kwargs()
            )
            pages = pager.by_page()
        fetch_started = time.monotonic()
        while True:
            with self._sdk_lock:
                page = next(pages, None)
            if page is None:
                break
            items = list(page)
            if self._slow is not None:
                self._slow.observe(
                    "change feed page",
                    time.monotonic() - fetch_started,
                    responses,
                    items=len(items),
                    range=f"{rng[0]}-{rng[1]}",
                )
            tok = _continuation(last[0])
            charge = sum(charges)
            charges.clear()
            responses.clear()
            yield ChangeFeedPage(items=items, positions=decode_positions(tok), resume_token=tok, request_charge=charge)
            fetch_started = time.monotonic()
        # The pager stops on HTTP 304 without a page; the state after it is in the last response.
        tok = _continuation(last[0])
        yield ChangeFeedPage(
            items=[], positions=decode_positions(tok), resume_token=tok, drained=True, request_charge=sum(charges)
        )

    def ensure_rid(self, rid: str) -> None:
        """Make the SDK's cached state match the container instance ``rid`` that a read was planned for.

        A client caches container properties and the routing map per container link, so after the
        container was deleted and re-created a worker that read the old instance would build
        requests for it: azure-cosmos rejects a continuation of the new instance with "Invalid
        continuation: [Mismatch collection rid]". ``ContainerProxy.read()`` refreshes the client's
        properties cache (azure-cosmos 4.14.0 to 4.17.1), and a forced feed-range read the routing map.
        """
        if self._properties is not None and self._properties.get("_rid") == rid:
            return
        current = str(self.properties(refresh=True)["_rid"])
        if current != rid:
            raise CosmosOffsetError(
                f"The container was re-created after this micro-batch was planned (planned rid={rid!r}, "
                f"current rid={current!r}). Start the stream with a new checkpoint location."
            )
        with self._sdk_lock:
            self._container.read_feed_ranges(force_refresh=True)

    def _probe(
        self, rng: tuple[str, str], mode: str, start_time: str | datetime, *, want_positions: bool = False
    ) -> tuple[list[dict[str, Any]], list[int], list[Any], str | None]:
        # Not serialized: the driver probes every range in parallel (stream._parallel_map), and the
        # change-feed request path does not copy response headers (no ``response_headers`` argument in
        # azure-cosmos 4.14.0 to 4.17.1), so concurrent probes cannot fail that way; each reads its
        # position from its own responses. No query runs on the client while a stream plans.
        etags: list[Any] = []
        sessions: list[str] = []
        last: list[Any] = [None]

        def hook(headers: Any, _body: Any) -> None:
            etags.append(headers.get("etag"))
            token = headers.get(SESSION_TOKEN_HEADER)
            if token and token not in sessions:
                sessions.append(str(token))
            last[0] = headers

        pager = self._container.query_items_change_feed(
            feed_range=feed_range_dict(*rng),
            start_time=start_time,
            mode=mode,
            max_item_count=1,
            response_hook=hook,
            **self._kwargs(),
        )
        pages = pager.by_page()
        first: list[dict[str, Any]] = []
        for page in pages:
            first = list(page)
            break
        lsns = [parse_etag_lsn(e) for e in etags if e]
        positions = decode_positions(_continuation(last[0])) if first and want_positions else []
        # One response per physical partition; several only if the range was split meanwhile.
        return first, lsns, positions, ",".join(sessions) or None

    def current_lsn(self, rng: tuple[str, str], mode: str) -> int:
        """Return the position of "now": the etag of an HTTP 304 for ``start_time="Now"`` (no data read).

        If the range spans several physical partitions (split race) the minimum is returned,
        which is a valid bound for every child.
        """
        return self._now(rng, mode)[0]

    def _now(self, rng: tuple[str, str], mode: str) -> tuple[int, str | None]:
        _items, lsns, _, session = self._probe(rng, mode, "Now")
        if not lsns:
            raise CosmosChangeFeedError(f"No change feed etag returned for range {epk.range_key(*rng)}.")
        return min(lsns), session

    def resolve_start(self, rng: tuple[str, str], mode: str, start: str | datetime) -> tuple[int, str | None]:
        """Return the exclusive LSN to start from for Beginning / Now / a point in time, and a session token.

        The session token is the one returned with the probe response, or None if it carried none
        (verified against the service: for a ``Now`` probe its LSN equals the etag LSN, e.g.
        ``0:-1#6`` with etag ``"6"``).
        """
        if start == "Now":
            return self._now(rng, mode)
        items, lsns, positions, session = self._probe(rng, MODE_LATEST_VERSION, start, want_positions=True)
        if items:
            if len(positions) > 1:
                raise CosmosChangeFeedError(
                    f"Range {epk.range_key(*rng)} was split while resolving the start position; retry."
                )
            lsn = item_lsn(items[0], MODE_LATEST_VERSION)
            if lsn is None:
                raise CosmosChangeFeedError("Change feed item without '_lsn'; cannot resolve start position.")
            return lsn - 1, session
        if not lsns:
            raise CosmosChangeFeedError(f"No change feed etag returned for range {epk.range_key(*rng)}.")
        return min(lsns), session

    def close(self) -> None:  # clients are owned by the process-level cache
        return None
