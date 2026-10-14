"""Thin adapter over ``azure.cosmos.ContainerProxy``.

Readers depend only on this small interface (also implemented by the test fake), which keeps
SDK-specific behaviour in one place:

* ``properties()``                 container properties (``_rid``, ``partitionKey``)
* ``quota()``                      ``x-ms-resource-usage`` sizes, if reported
* ``feed_ranges(force_refresh)``   physical partition EPK ranges, sorted
* ``query_pages(...)``             query pages for one EPK range, resumable by continuation,
                                   optionally pinned to a session token
* ``change_feed_pages(...)``       change-feed pages for one EPK range from an LSN, resumable
* ``current_lsn(rng, mode)``       current change-feed position without reading data
* ``resolve_start(rng, mode, s)``  LSN position for Beginning / Now / a timestamp, plus the session
                                   token of the response that determined it
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cosmosdb_datasource import epk
from cosmosdb_datasource.changefeed import (
    ChangeFeedPage,
    build_continuation,
    decode_positions,
    parse_etag_lsn,
)
from cosmosdb_datasource.documents import item_lsn
from cosmosdb_datasource.errors import CosmosChangeFeedError, CosmosDataSourceError
from cosmosdb_datasource.options import MODE_LATEST_VERSION

logger = logging.getLogger("cosmosdb_datasource")

SESSION_TOKEN_HEADER = "x-ms-session-token"


@dataclass
class QueryPage:
    items: list[dict[str, Any]]
    continuation: str | None
    request_charge: float = 0.0


def feed_range_dict(lo: str, hi: str) -> dict[str, Any]:
    """Return the feed range in the dict form accepted by ``feed_range=`` (``FeedRangeInternalEpk.to_dict``)."""
    return {"Range": {"min": lo, "max": hi, "isMinInclusive": True, "isMaxInclusive": False}}


def _charge(headers: Any) -> float:
    try:
        return float(headers.get("x-ms-request-charge") or 0.0)
    except (TypeError, ValueError, AttributeError):
        return 0.0


def parse_resource_usage(header: str | None) -> dict[str, int]:
    """Parse ``x-ms-resource-usage`` (``documentsSize=...;documentsCount=...``; sizes in KB)."""
    out: dict[str, int] = {}
    for part in (header or "").split(";"):
        name, sep, value = part.partition("=")
        if sep:
            try:
                out[name.strip()] = int(value.strip())
            except ValueError:
                continue
    return out


class SdkBackend:
    def __init__(self, container: Any, *, priority: str | None = None) -> None:
        self._container = container
        self._priority = priority
        self._properties: dict[str, Any] | None = None

    def _kwargs(self) -> dict[str, Any]:
        return {"priority": self._priority} if self._priority else {}

    def properties(self, refresh: bool = False) -> dict[str, Any]:
        if self._properties is None or refresh:
            self._properties = dict(self._container.read())
        return self._properties

    @property
    def rid(self) -> str:
        return str(self.properties()["_rid"])

    def quota(self) -> dict[str, int] | None:
        captured: dict[str, Any] = {}

        def hook(headers: Any, _body: Any) -> None:
            captured["usage"] = headers.get("x-ms-resource-usage")

        try:
            props = self._container.read(populate_quota_info=True, response_hook=hook)
        except Exception as exc:  # noqa: BLE001 - size is only a planning hint
            logger.warning(
                "Could not read container quota info (%s); size-based planning disabled.", type(exc).__name__
            )
            return None
        self._properties = dict(props)
        usage = parse_resource_usage(captured.get("usage"))
        return usage or None

    def feed_ranges(self, force_refresh: bool = False) -> list[tuple[str, str]]:
        ranges = []
        for fr in self._container.read_feed_ranges(force_refresh=force_refresh):
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
        """Yield the query pages of ``rng``.

        ``session_token`` (Session consistency only; the SDK drops it for other levels) makes the
        service serve every page from a replica that has applied at least that session's writes;
        otherwise it answers 404/1002, which the SDK and :mod:`cosmosdb_datasource.retry` retry.
        """
        charges: list[float] = []

        def hook(headers: Any, _body: Any) -> None:
            charges.append(_charge(headers))

        kwargs: dict[str, Any] = dict(self._kwargs())
        if parameters:
            kwargs["parameters"] = parameters
        if session_token:
            kwargs["session_token"] = session_token
        pager = self._container.query_items(
            query=query,
            feed_range=feed_range_dict(*rng),
            max_item_count=page_size,
            response_hook=hook,
            **kwargs,
        )
        pages = pager.by_page(continuation_token=continuation) if continuation else pager.by_page()
        for page in pages:
            items = list(page)
            charge = sum(charges)
            charges.clear()
            yield QueryPage(items=items, continuation=pages.continuation_token, request_charge=charge)

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

        def hook(headers: Any, _body: Any) -> None:
            charges.append(_charge(headers))

        rid = container_rid or self.rid
        token = resume_token or build_continuation(rid, mode, rng[0], rng[1], start_lsn)
        pager = self._container.query_items_change_feed(
            continuation=token, max_item_count=page_size, response_hook=hook, **self._kwargs()
        )
        pages = pager.by_page()
        for page in pages:
            items = list(page)
            tok = pages.continuation_token
            charge = sum(charges)
            charges.clear()
            yield ChangeFeedPage(items=items, positions=decode_positions(tok), resume_token=tok, request_charge=charge)
        # The pager stops on HTTP 304 without updating ``continuation_token``; the SDK stores the
        # refreshed composite state in the last response's etag (ChangeFeedFetcherV2).
        headers = self._container.client_connection.last_response_headers or {}
        tok = headers.get("etag")
        yield ChangeFeedPage(
            items=[], positions=decode_positions(tok), resume_token=tok, drained=True, request_charge=sum(charges)
        )

    def _probe(
        self, rng: tuple[str, str], mode: str, start_time: str | datetime, *, want_positions: bool = False
    ) -> tuple[list[dict[str, Any]], list[int], list[Any], str | None]:
        etags: list[Any] = []
        sessions: list[str] = []

        def hook(headers: Any, _body: Any) -> None:
            etags.append(headers.get("etag"))
            token = headers.get(SESSION_TOKEN_HEADER)
            if token and token not in sessions:
                sessions.append(str(token))

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
        # continuation_token comes from the client-wide last_response_headers: only read it when
        # the caller runs probes sequentially (resolve_start).
        positions = decode_positions(pages.continuation_token) if first and want_positions else []
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
