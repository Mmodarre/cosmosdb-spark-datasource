"""Query reads (batch reads, the initial snapshot, snapshot streams) while partitions split and documents change.

The fake serves queries in ``_rid`` order and handles a split like azure-cosmos 4.17.1 (it restarts a range
that now spans several children) or, with ``query_split="raise"``, like 4.14.0 (HTTP 410/1002).
"""

from __future__ import annotations

import collections
import random

import pytest

from cosmosdb_datasource import batch, retry
from cosmosdb_datasource.batch import QueryStats, query_documents
from cosmosdb_datasource.dedup import SeenKeys, fingerprint
from cosmosdb_datasource.documents import PartitionKeyInfo
from tests.conftest import fake_options
from tests.fake_cosmos import FakeBackend
from tests.harness import StreamHarness, run_batch

SPLIT_MODES = ["restart", "raise"]


def load(store, n: int, start: int = 0, keys: int = 41) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i})


def split_before_page(store, *pages: int) -> list[int]:
    """Split the first partition before the given query pages (1-based) of the next reads."""
    seen = [0]

    def hook() -> None:
        seen[0] += 1
        if seen[0] in pages:
            store.split(store.partitions[0].id)

    store._before_query_page = hook
    return seen


def keys(rows) -> list[tuple]:
    return [(r[1], r[0]) for r in rows]


def live_keys(store) -> list[tuple]:
    return sorted((d["pk"], d["id"]) for d in store.live_documents())


def assert_each_live_document_once(store, rows) -> None:
    got = keys(rows)
    assert len(got) == len(set(got)), [k for k, n in collections.Counter(got).items() if n > 1][:5]
    assert sorted(got) == live_keys(store)


def test_the_fake_reproduces_the_sdk_restart() -> None:
    """Without the package's split handling, a range read across a split returns rows twice (QA-06)."""
    from tests.fake_cosmos import FakeCosmosStore

    store = FakeCosmosStore("restart-probe", partitions=1).register()
    load(store, 120)
    split_before_page(store, 4)
    pages = FakeBackend(store).query_pages("SELECT * FROM c", None, ("", "FF"), 10)
    docs = [d["id"] for page in pages for d in page.items]
    assert len(docs) > 120 and len(set(docs)) == 120


@pytest.mark.parametrize("mode", SPLIT_MODES)
@pytest.mark.parametrize("factor", [1, 3, 4])
@pytest.mark.parametrize("at", [1, 3, 9])
def test_split_during_a_batch_read_returns_every_document_once(make_store, no_sleep, mode, factor, at) -> None:
    store = make_store(partitions=1)
    store.query_split = mode
    load(store, 300)
    split_before_page(store, at)
    rows, parts = run_batch(fake_options(store, partitions_per_feed_range=factor, page_size=10))
    assert len(parts) == factor and len(store.partitions) == 2
    assert_each_live_document_once(store, rows)


@pytest.mark.parametrize("mode", SPLIT_MODES)
def test_two_splits_during_one_read(make_store, no_sleep, mode) -> None:
    store = make_store(partitions=1)
    store.query_split = mode
    load(store, 400)
    split_before_page(store, 3, 12)
    rows, _ = run_batch(fake_options(store, page_size=10))
    assert len(store.partitions) == 3
    assert_each_live_document_once(store, rows)


def test_split_planned_partition_before_the_read_starts(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 200)
    split_before_page(store, 1)
    rows, _ = run_batch(fake_options(store, page_size=7))
    assert_each_live_document_once(store, rows)


def test_a_child_range_is_resumed_from_the_parent_continuation(make_store) -> None:
    """After a split the read continues where it stopped: no page of the parent range is read again."""
    store = make_store(partitions=1)
    load(store, 200)
    split_before_page(store, 6)
    rows, _ = run_batch(fake_options(store, page_size=10))
    assert_each_live_document_once(store, rows)
    pages = [e for e in store.call_log if e[0] == "query_page"]
    # 5 pages before the split, 1 dropped page, then the rest of each child: no restart from the beginning
    assert sum(e[4] for e in pages) <= 200 + 10


def test_delete_and_recreate_during_a_scan_returns_the_key_once(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 100)  # _rid order is d0 .. d99
    pages = [0]

    def churn() -> None:
        pages[0] += 1
        if pages[0] == 5:  # after d0 .. d39
            for i in (1, 80):  # read already / not yet; the re-created document comes at the end
                store.delete(f"p{i % 41}", f"d{i}")
                store.upsert({"id": f"d{i}", "pk": f"p{i % 41}", "v": "recreated"})
            for i in (2, 50):  # a replace keeps the document's place
                store.upsert({"id": f"d{i}", "pk": f"p{i % 41}", "v": "replaced"})

    store._before_query_page = churn
    rows, _ = run_batch(fake_options(store, page_size=10))
    store._before_query_page = None
    assert_each_live_document_once(store, rows)
    body = {r[0]: r[4] for r in rows}
    assert '"v":1,' in body["d1"] and '"v":"recreated"' in body["d80"]  # the version reached first
    assert '"v":2,' in body["d2"] and '"v":"replaced"' in body["d50"]


def test_delete_and_recreate_of_a_document_not_read_yet_returns_the_new_document(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 50)
    pages = [0]

    def churn() -> None:
        pages[0] += 1
        if pages[0] == 2:
            store.delete("p40", "d40")
            store.upsert({"id": "d40", "pk": "p40", "v": "new"})

    store._before_query_page = churn
    rows, _ = run_batch(fake_options(store, page_size=10))
    store._before_query_page = None
    by_key = {(r[1], r[0]): r for r in rows}
    assert len(rows) == len(by_key) == 50 and '"v":"new"' in by_key[("p40", "d40")][4]


@pytest.mark.parametrize("seed", range(12))
def test_random_churn_and_splits_never_duplicate_a_key(make_store, no_sleep, seed) -> None:
    rng = random.Random(seed)
    store = make_store(partitions=rng.choice([1, 2]))
    store.query_split = rng.choice(SPLIT_MODES)
    load(store, 150)
    stable = {(d["pk"], d["id"]) for d in store.live_documents()}  # left alone during the read
    pages = [0]
    split_at = rng.randrange(2, 10)

    def churn() -> None:
        pages[0] += 1
        if pages[0] == split_at:
            store.split(store.partitions[0].id)
        for _ in range(rng.randrange(0, 4)):
            doc = rng.choice(store.live_documents())
            key = (doc["pk"], doc["id"])
            r = rng.random()
            if r < 0.4:
                store.upsert({**doc, "v": rng.random()})
                stable.discard(key)
            elif r < 0.7:
                store.delete(*key)
                stable.discard(key)
                if rng.random() < 0.7:
                    store.upsert({"id": doc["id"], "pk": doc["pk"], "v": "again"})
            else:
                store.upsert({"id": f"n{rng.randrange(10**6)}", "pk": f"p{rng.randrange(41)}", "v": 0})

    store._before_query_page = churn
    rows, _ = run_batch(fake_options(store, page_size=8, partitions_per_feed_range=rng.choice([1, 3])))
    store._before_query_page = None
    got = keys(rows)
    assert len(got) == len(set(got))
    assert stable <= set(got)


def test_query_without_a_key_is_not_deduplicated(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 20)
    stats = QueryStats()
    values = list(
        query_documents(
            FakeBackend(store),
            "SELECT * FROM c",
            None,
            ("", "FF"),
            5,
            policy=retry.RetryPolicy(max_retries=0),
            stats=stats,
            describe="test",
            pk=PartitionKeyInfo(paths=("/pk",)),
        )
    )
    assert len(values) == 20 and stats.duplicates == 0 and stats.replans == 0


def test_replans_are_bounded(make_store, monkeypatch) -> None:
    store = make_store(partitions=1)
    load(store, 30)
    backend = FakeBackend(store)
    original = backend.query_pages

    def wandering(*args, **kwargs):
        for i, page in enumerate(original(*args, **kwargs)):
            page.partitions = (f"x{i}", f"y{i}")  # always two serving partitions
            yield page

    monkeypatch.setattr(backend, "query_pages", wandering)
    with pytest.raises(batch.CosmosDataSourceError, match="kept changing"):
        list(
            query_documents(
                backend,
                "SELECT * FROM c",
                None,
                ("", "FF"),
                5,
                policy=retry.RetryPolicy(max_retries=0),
                stats=QueryStats(),
                describe="test",
            )
        )


@pytest.mark.parametrize("mode", SPLIT_MODES)
def test_split_during_the_initial_snapshot(make_store, no_sleep, mode) -> None:
    store = make_store(partitions=1)
    store.query_split = mode
    load(store, 150)
    h = StreamHarness(
        fake_options(
            store,
            change_feed_mode="AllVersionsAndDeletes",
            initial_snapshot="true",
            initial_snapshot_delay_seconds="0",
            page_size=10,
        )
    )
    start, end = h.start(), h.latest()
    split_before_page(store, 4)
    rows = h.read_range(start, end)
    ids = h.column(rows, "id")
    assert len(store.partitions) == 2 and len(ids) == len(set(ids)) == 150


@pytest.mark.parametrize("mode", SPLIT_MODES)
def test_split_during_a_snapshot_stream_scan(make_store, no_sleep, mode) -> None:
    store = make_store(partitions=1)
    store.query_split = mode
    load(store, 120)
    h = StreamHarness(fake_options(store, snapshot_stream="true", page_size=10))
    h.start()
    split_before_page(store, 5)
    rows = h.run_batch()
    ids = h.column(rows, "id")
    assert len(ids) == len(set(ids)) == 120


def test_header_mutations_on_different_pages_are_each_repeated(make_store, no_sleep) -> None:
    """The limit applies to one page: a long scan may meet the race on many pages."""
    store = make_store(partitions=1)
    load(store, 100)
    attempts = [0]

    def race() -> None:
        attempts[0] += 1
        if attempts[0] % 3 == 0:  # every third request: once on most pages
            raise RuntimeError(retry.CONCURRENT_HEADER_MUTATION)

    store._before_query_page = race
    stats = QueryStats()
    try:
        docs = query_documents(
            FakeBackend(store),
            "SELECT * FROM c",
            None,
            ("", "FF"),
            10,
            policy=retry.RetryPolicy(max_retries=8),
            stats=stats,
            describe="test",
            pk=PartitionKeyInfo(paths=("/pk",)),
        )
        assert sorted(d["id"] for d in docs) == sorted(f"d{i}" for i in range(100))
        assert stats.retries > retry.MAX_CONCURRENT_MUTATION_RETRIES
    finally:
        store._before_query_page = None


@pytest.mark.parametrize(("races", "retried"), [(2, True), (3, False)])
def test_a_concurrent_header_mutation_is_repeated_at_most_twice(make_store, no_sleep, races, retried) -> None:
    """azure-cosmos 4.17.1 raises this when another request on the client mutates the headers a query copies."""
    store = make_store(partitions=1)
    load(store, 100)
    pages = [0]

    def race() -> None:
        pages[0] += 1
        if 4 <= pages[0] < 4 + races:
            raise RuntimeError(retry.CONCURRENT_HEADER_MUTATION)

    store._before_query_page = race
    stats = QueryStats()
    docs = query_documents(
        FakeBackend(store),
        "SELECT * FROM c",
        None,
        ("", "FF"),
        10,
        policy=retry.RetryPolicy(max_retries=8),
        stats=stats,
        describe="test",
        pk=PartitionKeyInfo(paths=("/pk",)),
    )
    try:
        if retried:
            assert sorted(d["id"] for d in docs) == sorted(f"d{i}" for i in range(100)) and stats.retries == 2
        else:
            with pytest.raises(RuntimeError, match="mutated during iteration"):
                list(docs)
    finally:
        store._before_query_page = None


def test_seen_keys_is_exact_across_memory_and_disk_runs() -> None:
    rng = random.Random(5)
    seen = SeenKeys(spill_entries=1 << 9, pending_limit=1 << 6)
    added = {rng.getrandbits(64) for _ in range(5000)}
    for fp in added:
        seen.add(fp)
    try:
        assert seen.spilled_runs > 3
        assert all(fp in seen for fp in added)
        others = {rng.getrandbits(64) for _ in range(5000)} - added
        assert not any(fp in seen for fp in others)
        assert "not an int" not in seen
    finally:
        seen.close()


def test_fingerprint_tells_a_missing_partition_key_from_null() -> None:
    info = PartitionKeyInfo(paths=("/pk",))
    assert fingerprint({"id": "a", "pk": None}, info) != fingerprint({"id": "a"}, info)
    assert fingerprint({"id": "a", "pk": "x", "v": 1}, info) == fingerprint({"id": "a", "pk": "x", "v": 2}, info)
    assert fingerprint({"pk": "x"}, info) is None
    nested = PartitionKeyInfo(paths=("/a/b",))
    assert fingerprint({"id": "a", "a": {}}, nested) == fingerprint({"id": "a"}, nested)


class _StubPages:
    """Pages like the SDK's: the hook sees each response, and the last one carries the page's continuation.

    ``continuation_token`` comes from the client-wide headers, here those of another query on the client.
    """

    def __init__(self, pages, hook) -> None:
        self._pages = iter(pages)
        self._hook = hook
        self.continuation_token = None

    def __iter__(self):
        return self

    def __next__(self):
        items, headers_list, token = next(self._pages)
        for headers in headers_list:
            self._hook(headers, {})
        if token is not None:
            headers_list[-1]["x-ms-continuation"] = token
        self.continuation_token = "token of a concurrent query"
        return iter(items)


class _StubContainer:
    def __init__(self, pages) -> None:
        self.pages = pages
        self.calls: list[dict] = []

    def read_feed_ranges(self, force_refresh=False):
        return [{"Range": {"min": "", "max": "80"}}, {"Range": {"min": "80", "max": "FF"}}]

    def query_items(self, **kwargs):
        self.calls.append(kwargs)
        stub = self

        class Pager:
            def by_page(self, continuation_token=None):
                stub.calls[-1]["continuation"] = continuation_token
                return _StubPages(stub.pages, kwargs["response_hook"])

        return Pager()


def test_sdk_backend_reports_the_serving_partitions_and_the_server_time() -> None:
    from cosmosdb_datasource.backend import SdkBackend

    date = "Wed, 14 Oct 2026 17:54:55 GMT"
    pk_range = "x-ms-documentdb-partitionkeyrangeid"
    container = _StubContainer(
        [
            ([{"id": "a"}], [{"x-ms-request-charge": "2.5", pk_range: "1", "Date": date}], "c1"),
            ([{"id": "b"}], [{pk_range: "4"}, {pk_range: "5"}], None),
            ([], [{"Date": "not a date"}], None),
        ]
    )
    pages = list(SdkBackend(container).query_pages("SELECT * FROM c", None, ("", "FF"), 10, continuation="c0"))
    assert [p.partitions for p in pages] == [("1",), ("4", "5"), ()]
    assert pages[0].server_time == 1792000495.0 and pages[1].server_time is None and pages[2].server_time is None
    assert pages[0].request_charge == 2.5 and [p.continuation for p in pages] == ["c1", None, None]
    assert container.calls[0]["continuation"] == "c0" and container.calls[0]["feed_range"]["Range"]["max"] == "FF"
    assert all(p.feed_ranges == [("", "80"), ("80", "FF")] for p in pages)  # read with each page
