"""SdkBackend against a stub of the azure-cosmos container proxy.

The stub reproduces what ``ChangeFeedFetcherV2`` does with response headers: the response hook gets
the headers of each request, then the fetcher writes the pager's composite continuation into that
same object, and the client-wide ``last_response_headers`` holds whatever response came last on
the client, possibly another thread's.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from requests.structures import CaseInsensitiveDict

from cosmosdb_datasource import retry
from cosmosdb_datasource.backend import SdkBackend
from cosmosdb_datasource.batch import QueryStats, query_documents
from cosmosdb_datasource.changefeed import Position, build_composite_continuation, decode_positions
from cosmosdb_datasource.errors import CosmosOffsetError

RID = "cid1"


def token(lsn: int) -> str:
    return build_composite_continuation(RID, "LatestVersion", "", "FF", [Position("", "FF", lsn)])


# What another reader's request on the same client leaves in last_response_headers: two children.
OTHER = build_composite_continuation(
    RID, "LatestVersion", "", "FF", [Position("", "80", 10**9), Position("80", "FF", 10**9)]
)


class _Connection:
    def __init__(self) -> None:
        self.last_response_headers: dict = {}


class _ChangeFeedPages:
    """by_page() of a change-feed pager: (items, lsn) per request; an empty list of items is a 304."""

    def __init__(self, container: _Container, responses, hook) -> None:
        self._container = container
        self._responses = list(responses)
        self._hook = hook
        self.continuation_token = None

    def __iter__(self):
        return self

    def __next__(self):
        while self._responses:
            items, lsn = self._responses.pop(0)
            headers = {"etag": f'"{lsn}"', "x-ms-request-charge": "1"}
            self._hook(headers, {})
            headers["etag"] = token(lsn)  # the fetcher stores the composite state in the same object
            self._container.client_connection.last_response_headers = headers
            self.continuation_token = token(lsn)
            # another reader on the same client finishes a request now
            self._container.client_connection.last_response_headers = {"etag": OTHER}
            self.continuation_token = OTHER
            if items:
                return iter(items)
        raise StopIteration


class _Container:
    def __init__(self, responses, rids=(RID,)) -> None:
        self.client_connection = _Connection()
        self.responses = responses
        self.rids = list(rids)
        self.reads = 0
        self.refreshed_ranges = 0

    def read(self, **_kwargs):
        self.reads += 1
        return {"_rid": self.rids[min(self.reads, len(self.rids)) - 1], "partitionKey": {"paths": ["/pk"]}}

    def read_feed_ranges(self, force_refresh: bool = False):
        self.refreshed_ranges += int(force_refresh)
        return [{"Range": {"min": "", "max": "FF"}}]

    def query_items_change_feed(self, **kwargs):
        container = self

        class Pager:
            def by_page(self):
                return _ChangeFeedPages(container, container.responses, kwargs["response_hook"])

        return Pager()


def test_change_feed_continuations_come_from_the_pagers_own_responses() -> None:
    container = _Container([([{"id": "a", "_lsn": 5}], 5), ([{"id": "b", "_lsn": 7}], 7), ([], 7)])
    pages = list(SdkBackend(container).change_feed_pages(("", "FF"), "LatestVersion", 4, 10, container_rid=RID))
    assert [p.positions[0].lsn for p in pages] == [5, 7, 7]
    assert pages[-1].drained and pages[-1].resume_token == token(7)
    assert decode_positions(pages[0].resume_token) == [Position("", "FF", 5)]


def test_start_probe_positions_come_from_the_probes_own_response() -> None:
    container = _Container([([{"id": "a", "_lsn": 12}], 12)])
    lsn, _session = SdkBackend(container).resolve_start(("", "FF"), "LatestVersion", "Beginning")
    assert lsn == 11


def test_a_recreated_container_refreshes_the_cached_properties_and_routing_map() -> None:
    container = _Container([([], 3)], rids=("old", "new"))
    backend = SdkBackend(container)
    assert backend.rid == "old"  # this worker read the container before it was re-created
    pages = list(backend.change_feed_pages(("", "FF"), "LatestVersion", 3, 10, container_rid="new"))
    assert pages[-1].drained and container.reads == 2 and container.refreshed_ranges == 1
    list(backend.change_feed_pages(("", "FF"), "LatestVersion", 3, 10, container_rid="new"))
    assert container.reads == 2  # known instance: no further reads


def test_a_partition_planned_for_another_container_instance_fails_clearly() -> None:
    container = _Container([([], 3)], rids=("old", "newer"))
    backend = SdkBackend(container)
    assert backend.rid == "old"
    with pytest.raises(CosmosOffsetError, match="re-created after this micro-batch was planned"):
        list(backend.change_feed_pages(("", "FF"), "LatestVersion", 3, 10, container_rid="new"))


class _SharedClient:
    """The request bookkeeping of an azure-cosmos 4.17.1 client that makes concurrent requests unsafe.

    A query response becomes the client-wide ``last_response_headers`` and is copied into the pager's
    header dict (``__QueryFeed``), the retry wrapper then writes throttle counters into whatever object
    that attribute holds (``_retry_utility.Execute``), and the pager's ``continuation_token`` is read from
    it (``QueryIterable._unpack``). The copy yields the GIL after every header, so concurrent requests
    interleave the way they do while waiting on sockets.
    """

    def __init__(self) -> None:
        self.last_response_headers: CaseInsensitiveDict | None = None

    def request(self, headers: CaseInsensitiveDict, hook) -> None:
        self.last_response_headers = headers
        copy = CaseInsensitiveDict()
        for name in headers:  # "OrderedDict mutated during iteration" if another request adds a header now
            copy[name] = headers[name]
            time.sleep(0.0002)
        hook(headers, {})
        self.last_response_headers["x-ms-throttle-retry-count"] = "0"


RANGES = [(f"{i * 32:02X}" if i else "", f"{(i + 1) * 32:02X}" if i < 7 else "FF") for i in range(8)]


class _QueryContainer:
    def __init__(self, docs_per_range: int) -> None:
        self.client = _SharedClient()
        self.docs = {rng: [{"id": f"{n}-{j}"} for j in range(docs_per_range)] for n, rng in enumerate(RANGES)}

    def read_feed_ranges(self, force_refresh: bool = False):
        return [{"Range": {"min": lo, "max": hi}} for lo, hi in RANGES]

    def query_items(self, *, max_item_count, response_hook, feed_range, **_kwargs):
        rng = (feed_range["Range"]["min"], feed_range["Range"]["max"])
        container = self

        class Pages:
            def __init__(self, continuation_token) -> None:
                self.offset = int(continuation_token.split(":")[1]) if continuation_token else 0
                self.continuation_token = None

            def __iter__(self):
                return self

            def __next__(self):
                docs = container.docs[rng]
                if self.offset >= len(docs):
                    raise StopIteration
                items = docs[self.offset : self.offset + max_item_count]
                self.offset += len(items)
                headers = CaseInsensitiveDict(
                    {
                        "x-ms-request-charge": "1",
                        "x-ms-documentdb-partitionkeyrangeid": str(RANGES.index(rng)),
                        "x-ms-activity-id": "a",
                        "x-ms-session-token": "0:1#1",
                        "x-ms-item-count": str(len(items)),
                        "Date": "Wed, 14 Oct 2026 17:54:55 GMT",
                    }
                )
                if self.offset < len(docs):
                    headers["x-ms-continuation"] = f"{RANGES.index(rng)}:{self.offset}"
                container.client.request(headers, response_hook)
                self.continuation_token = container.client.last_response_headers.get("x-ms-continuation")
                return iter(items)

        class Pager:
            def by_page(self, continuation_token=None):
                return Pages(continuation_token)

        return Pager()


@pytest.mark.parametrize("prefetch_pages", [0, 1])
def test_concurrent_query_reads_on_one_client(prefetch_pages: int) -> None:
    """Eight reads on one client at once, as in schema sampling or with several readers in a process."""
    container = _QueryContainer(docs_per_range=40)
    backend = SdkBackend(container, prefetch_pages=prefetch_pages, sdk_lock=threading.RLock())

    def read(rng: tuple[str, str]) -> list[str]:
        docs = query_documents(
            backend,
            "SELECT * FROM c",
            None,
            rng,
            5,
            policy=retry.RetryPolicy(max_retries=0),
            stats=QueryStats(),
            describe=f"{rng}",
        )
        return [d["id"] for d in docs]

    with ThreadPoolExecutor(len(RANGES)) as pool:
        results = list(pool.map(read, RANGES))
    assert results == [[d["id"] for d in container.docs[rng]] for rng in RANGES]


def test_read_items_runs_its_queries_one_at_a_time() -> None:
    calls: list[dict] = []

    class Container:
        def read_items(self, **kwargs):
            calls.append(kwargs)
            kwargs["response_hook"]({"x-ms-request-charge": "3"}, None)
            return [{"id": "a"}]

    docs, charge = SdkBackend(Container()).read_items([("a", "p")])
    assert docs == [{"id": "a"}] and charge == 3.0 and calls[0]["max_concurrency"] == 1


def test_the_topology_check_of_a_query_read_runs_under_the_client_lock() -> None:
    """The routing map can be empty after the SDK handled a split; reading it then sends a request."""
    active: list[str] = []
    overlaps: list[tuple[str, tuple[str, ...]]] = []
    guard = threading.Lock()

    class InFlight:
        def __init__(self, name: str) -> None:
            self.name = name

        def __enter__(self) -> None:
            with guard:
                if active:
                    overlaps.append((self.name, tuple(active)))
                active.append(self.name)

        def __exit__(self, *exc: object) -> None:
            with guard:
                active.remove(self.name)

    class Pager:
        def __init__(self, hook):
            self.hook = hook

        def by_page(self, continuation_token=None):
            for i in range(6):
                with InFlight("query page"):
                    time.sleep(0.02)
                    self.hook({"x-ms-documentdb-partitionkeyrangeid": "0", "x-ms-continuation": f"c{i}"}, None)
                yield iter([{"id": f"d{i}", "pk": "p"}])

    class Container:
        def read_feed_ranges(self, force_refresh=False):
            with InFlight("partition key ranges"):  # a cold routing-map cache
                time.sleep(0.01)
            return [{"Range": {"min": "", "max": "FF"}}]

        def query_items(self, query, max_item_count, response_hook, **kwargs):
            return Pager(response_hook)

    backend = SdkBackend(Container(), prefetch_pages=1)
    docs = list(
        query_documents(
            backend,
            "SELECT * FROM c",
            None,
            ("", "FF"),
            10,
            policy=retry.RetryPolicy(),
            stats=QueryStats(),
            describe="test",
        )
    )
    assert len(docs) == 6 and overlaps == []
