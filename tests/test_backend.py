"""SdkBackend against a stub of the azure-cosmos container proxy.

The stub reproduces what ``ChangeFeedFetcherV2`` does with response headers: the response hook gets
the headers of each request, then the fetcher writes the pager's composite continuation into that
same object, and the client-wide ``last_response_headers`` holds whatever response came last on
the client, possibly another thread's.
"""

from __future__ import annotations

import pytest

from cosmosdb_datasource.backend import SdkBackend
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
