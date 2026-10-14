"""Integration tests against Azure Cosmos DB for NoSQL or a Cosmos DB emulator.

Configuration, from the environment:

``COSMOS_ENDPOINT``, ``COSMOS_KEY``
    Account endpoint and key. Required; every test is skipped without them.
``COSMOS_DATABASE``
    Database for the test containers, created if missing (default ``cosmosdb_datasource_it``).
``COSMOS_EMULATOR``
    ``vnext`` or ``legacy`` when the endpoint is that emulator: tests that need a capability it
    lacks are skipped (see ``EMULATOR_UNSUPPORTED``).
``COSMOS_UNSUPPORTED``
    Comma-separated capability markers to skip, e.g. ``requires_avad`` for an account without the
    AllVersionsAndDeletes change feed.
``COSMOS_MULTI_PARTITION_THROUGHPUT``
    RU/s for the containers of the ``requires_multi_partition`` tests; it must yield more than one
    physical partition (e.g. 20000 on the legacy emulator, at least 12000 on an account).
``COSMOS_AAD=1``
    Also run the ``auth_type=default`` test. ``DefaultAzureCredential`` must find an identity with a
    Cosmos DB data-plane role on the account.

Tests write only to containers they create (``it_<random>``) and delete them afterwards. Other
containers are created without explicit throughput, so serverless accounts work too.
"""

from __future__ import annotations

import contextlib
import json
import os
import pickle
import random
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from pyspark import cloudpickle
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType, TimestampType

from tests.conftest import java_major
from tests.harness import StreamHarness, as_rows, make_source, run_batch, run_batch_with_filters, validate_rows

ENDPOINT = os.environ.get("COSMOS_ENDPOINT", "")
KEY = os.environ.get("COSMOS_KEY", "")
DATABASE = os.environ.get("COSMOS_DATABASE") or "cosmosdb_datasource_it"
EMULATOR = os.environ.get("COSMOS_EMULATOR", "")
MULTI_PARTITION_THROUGHPUT = int(os.environ.get("COSMOS_MULTI_PARTITION_THROUGHPUT") or 0)
AAD = os.environ.get("COSMOS_AAD") == "1"
UNSUPPORTED = {s.strip() for s in os.environ.get("COSMOS_UNSUPPORTED", "").split(",") if s.strip()}

CAPABILITIES = {
    "requires_avad": "AllVersionsAndDeletes change feed",
    "requires_epk_ranges": "EPK sub-range filtering",
    "requires_multi_partition": "multiple physical partitions",
    "requires_session_tokens": "session token enforcement",
    "requires_aad": "Entra ID authentication",
}
# Observed on GitHub's ubuntu-24.04 runners with azure-cosmos 4.17.1.
EMULATOR_UNSUPPORTED = {
    # Linux vNext emulator, vnext-EN20261008: AllVersionsAndDeletes requests return LatestVersion
    # items, EPK sub-range filters are ignored, every container has one physical partition, session
    # tokens are not enforced and requests are not authenticated.
    "vnext": frozenset(CAPABILITIES),
    # Legacy Linux emulator, 2.14.28 (x64 only): AllVersionsAndDeletes is rejected with HTTP 400
    # ("must be enabled"), and only key authentication exists.
    "legacy": frozenset({"requires_avad", "requires_aad"}),
}
if EMULATOR and EMULATOR not in EMULATOR_UNSUPPORTED:
    raise ValueError(f"COSMOS_EMULATOR must be one of {', '.join(EMULATOR_UNSUPPORTED)}, got {EMULATOR!r}")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not (ENDPOINT and KEY), reason="COSMOS_ENDPOINT and COSMOS_KEY are not set"),
]
if EMULATOR:
    # azure-cosmos does not verify TLS certificates for localhost endpoints.
    pytestmark.append(pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning"))


def _skip_reason(names: set[str]) -> str | None:
    lacking = names & EMULATOR_UNSUPPORTED.get(EMULATOR, frozenset())
    if lacking:
        return f"the {EMULATOR} emulator does not support: {', '.join(sorted(CAPABILITIES[n] for n in lacking))}"
    if names & UNSUPPORTED:
        return f"disabled by COSMOS_UNSUPPORTED: {', '.join(sorted(names & UNSUPPORTED))}"
    if "requires_multi_partition" in names and not MULTI_PARTITION_THROUGHPUT:
        return "COSMOS_MULTI_PARTITION_THROUGHPUT is not set"
    if "requires_aad" in names and not AAD:
        return "COSMOS_AAD is not set"
    return None


def requires(*names: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Apply the capability markers ``names`` and skip the test where a capability is missing.

    A ``skipif`` mark (not a fixture) so that module-scoped fixtures are not set up for skipped tests.
    """
    unknown = set(names) - CAPABILITIES.keys()
    if unknown:
        raise ValueError(f"unknown capability markers: {sorted(unknown)}")
    reason = _skip_reason(set(names))

    def decorate(test: Callable[..., Any]) -> Callable[..., Any]:
        for name in names:
            test = getattr(pytest.mark, name)(test)
        return pytest.mark.skipif(reason is not None, reason=reason or "")(test)

    return decorate


@pytest.fixture(scope="session")
def sdk_db() -> Iterator[Any]:
    from azure.cosmos import CosmosClient

    client = CosmosClient(ENDPOINT, credential=KEY)
    yield client.create_database_if_not_exists(DATABASE)
    client.__exit__(None, None, None)  # CosmosClient.close() does not exist in azure-cosmos 4.14


_TRANSIENT_STATUS = frozenset({408, 429, 449, 500, 503})


def _retry_write(write: Callable[[], Any], *, delete: bool = False) -> Any:
    """Run one of the tests' own writes, retried on transient errors.

    The emulators time out writes under load (HTTP 408, seen on the legacy emulator right after a
    container was created), and the SDK does not retry timed-out writes. Upserts are idempotent; a
    retried delete whose first attempt went through returns 404, which counts as done.
    """
    from azure.core.exceptions import ServiceRequestError, ServiceResponseError
    from azure.cosmos.exceptions import CosmosHttpResponseError, CosmosResourceNotFoundError

    for attempt in range(6):
        try:
            return write()
        except CosmosResourceNotFoundError:
            if delete and attempt > 0:
                return None
            raise
        except (CosmosHttpResponseError, ServiceRequestError, ServiceResponseError) as exc:
            status = getattr(exc, "status_code", None)
            if attempt == 5 or (isinstance(exc, CosmosHttpResponseError) and status not in _TRANSIENT_STATUS):
                raise
            time.sleep(2 * (attempt + 1))
    raise AssertionError("unreachable")


class _RetryingContainer:
    """Container proxy whose writes are retried (``_retry_write``); everything else is delegated."""

    def __init__(self, container: Any) -> None:
        self._container = container

    def __getattr__(self, name: str) -> Any:
        return getattr(self._container, name)

    def upsert_item(self, *args: Any, **kwargs: Any) -> Any:
        return _retry_write(lambda: self._container.upsert_item(*args, **kwargs))

    def execute_item_batch(self, *args: Any, **kwargs: Any) -> Any:
        return _retry_write(lambda: self._container.execute_item_batch(*args, **kwargs))

    def delete_item(self, *args: Any, **kwargs: Any) -> Any:
        return _retry_write(lambda: self._container.delete_item(*args, **kwargs), delete=True)


@contextlib.contextmanager
def temporary_container(db: Any, *, version: int = 2, throughput: int | None = None) -> Iterator[tuple[str, Any]]:
    from azure.cosmos import PartitionKey
    from azure.cosmos.exceptions import CosmosResourceNotFoundError

    name = f"it_{uuid.uuid4().hex[:10]}"
    try:
        container = None
        for attempt in range(4):
            try:
                # Idempotent: a client-side timeout may still have created the container.
                container = db.create_container_if_not_exists(
                    name,
                    partition_key=PartitionKey(path="/pk", kind="Hash", version=version),
                    **({"offer_throughput": throughput} if throughput else {}),
                )
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))
        yield name, _RetryingContainer(container)
    finally:  # also after a failed creation that the service completed
        with contextlib.suppress(CosmosResourceNotFoundError):
            db.delete_container(name)


@pytest.fixture
def make_container(sdk_db: Any) -> Iterator[Callable[..., tuple[str, Any]]]:
    with contextlib.ExitStack() as stack:
        yield lambda **kwargs: stack.enter_context(temporary_container(sdk_db, **kwargs))


def options(container: str, **extra: Any) -> dict[str, str]:
    opts = {
        "endpoint": ENDPOINT,
        "database": DATABASE,
        "container": container,
        "auth_type": "key",
        "account_key": KEY,
    }
    opts.update({k: str(v) for k, v in extra.items()})
    return opts


def document(i: int, keys: int = 13, **fields: Any) -> dict[str, Any]:
    return {
        "id": f"d{i}",
        "pk": f"p{i % keys}",
        "seq": i,
        "createdAt": f"2026-01-{1 + i % 28:02d}T10:00:00.1234567Z",
        "tags": [f"t{i % 3}", "all"],
        "nested": {"x": i},
        **fields,
    }


def write(container: Any, start: int, n: int, **fields: Any) -> None:
    for i in range(start, start + n):
        container.upsert_item(document(i, **fields))


def seed(container: Any, n: int, keys: int = 20) -> None:
    """Write ``n`` documents in transactional batches (at most 100 operations, one partition key)."""
    by_key: dict[str, list[dict[str, Any]]] = {}
    for i in range(n):
        doc = document(i, keys=keys)
        by_key.setdefault(doc["pk"], []).append(doc)
    for pk, docs in by_key.items():
        for start in range(0, len(docs), 100):
            container.execute_item_batch([("upsert", (d,)) for d in docs[start : start + 100]], partition_key=pk)


def count(container: Any) -> int:
    return next(iter(container.query_items("SELECT VALUE COUNT(1) FROM c", enable_cross_partition_query=True)))


def feed_range_count(container: Any) -> int:
    return len(list(container.read_feed_ranges(force_refresh=True)))


def parallel_batch(opts: dict[str, str], workers: int = 8) -> tuple[list[tuple], list[Any], list[int]]:
    """Read like ``harness.run_batch``, but run the partitions concurrently, one executor per thread."""
    ds = make_source(opts)
    schema = ds.schema()
    reader = ds.reader(schema)
    shipped = cloudpickle.dumps(reader)
    parts = reader.partitions()

    def run(part: Any) -> list[tuple]:
        output = list(cloudpickle.loads(shipped).read(part))
        validate_rows(output, schema, ds)
        return as_rows(output, schema)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(run, parts))
    return [r for rows in results for r in rows], parts, [len(rows) for rows in results]


@pytest.fixture(scope="module")
def seeded(sdk_db: Any) -> Iterator[tuple[str, Any, int]]:
    """Container with 600 documents, shared by the read-only batch tests."""
    with temporary_container(sdk_db) as (name, container):
        seed(container, 600)
        yield name, container, 600


def test_batch_reads_every_document(seeded) -> None:
    name, container, n = seeded
    rows, parts = run_batch(options(name))
    assert len(parts) == feed_range_count(container)
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(n))
    for row in rows:
        body = json.loads(row[4])
        assert (row[0], row[1], row[2], row[3]) == (body["id"], body["pk"], body["_ts"], body["_etag"])


def test_batch_user_schema(seeded) -> None:
    name, _container, n = seeded
    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("seq", LongType()),
            StructField("createdAt", TimestampType()),
            StructField("tags", ArrayType(StringType())),
            StructField("nested", StructType([StructField("x", LongType())])),
            StructField("_corrupt_record", StringType()),
        ]
    )
    rows, _ = run_batch(options(name, page_size=100), schema)
    assert len(rows) == n
    assert all(r[5] is None for r in rows), "unexpected corrupt records"
    assert all(r[1] == r[4][0] and r[2] is not None and r[3][-1] == "all" for r in rows)


def test_batch_query_with_parameters(seeded) -> None:
    name, _container, _n = seeded
    rows, _ = run_batch(
        options(
            name,
            query="SELECT c.id, c.pk FROM c WHERE c.seq < @limit",
            query_parameters=json.dumps([{"name": "@limit", "value": 50}]),
        )
    )
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(50))


def test_batch_auto_partitioning(seeded) -> None:
    name, container, n = seeded
    rows, parts = run_batch(options(name, partitions_per_feed_range="auto"))
    assert len(parts) % feed_range_count(container) == 0
    assert len(rows) == n


def test_filter_pushdown_returns_what_spark_filtering_returns(make_container) -> None:
    """The pushed predicates against the service's own query engine: type-strict comparisons, undefined."""
    from tests.test_pushdown import SCHEMA, random_document, random_filter, spark_eval

    name, container = make_container()
    rng = random.Random(11)
    for i in range(120):
        doc = random_document(rng, i)
        if "pk" not in doc or doc["pk"] is None:
            doc["pk"] = "p0"  # documents without a partition key value are rejected by some endpoints
        container.upsert_item(doc)
    all_rows, _ = run_batch(options(name), SCHEMA)
    assert len(all_rows) == 120
    ts_values = sorted({r[2] for r in all_rows})
    reduced = 0
    for _ in range(40):
        filters = [random_filter(rng, ts_values) for _ in range(rng.randint(1, 3))]
        expected = sorted(repr(r) for r in all_rows if all(spark_eval(f, r) is True for f in filters))
        rows, _parts, remaining = run_batch_with_filters(options(name, filter_pushdown="true"), SCHEMA, filters)
        assert sorted(repr(r) for r in rows if all(spark_eval(f, r) is True for f in remaining)) == expected, filters
        reduced += len(rows) < len(all_rows)
    assert reduced > 10


def test_inferred_schema(seeded) -> None:
    name, _container, n = seeded
    ds = make_source(options(name, infer_schema="true", sampling_size=100))
    schema = ds.schema()
    assert schema.simpleString() == (
        "struct<id:string,pk:string,seq:bigint,createdAt:string,tags:array<string>,nested:struct<x:bigint>>"
    )
    rows, _ = run_batch(options(name, infer_schema="true", sampling_size=100), schema)
    assert len(rows) == n and all(r[2] == r[5][0] for r in rows)


def test_partition_key_item_ids_and_read_many_function(seeded) -> None:
    import pyarrow as pa

    from cosmosdb_datasource import readmany

    name, _container, n = seeded
    rows, parts = run_batch(options(name, partition_key="p3"))
    assert len(parts) == 1 and sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(n) if i % 20 == 3)
    ids = [[f"d{i}", f"p{i % 20}"] for i in range(0, n, 7)] + [["missing", "p1"]]
    rows, _ = run_batch(options(name, item_ids=json.dumps(ids)))
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(0, n, 7))

    class Frame:
        def __getitem__(self, column: str) -> str:
            return column

        def select(self, *columns: str) -> Any:
            return self

        def mapInArrow(self, fn: Any, schema: Any) -> Any:
            return fn

    read = readmany.read_many(Frame(), schema=StructType([StructField("id", StringType())]), **options(name))
    batch = pa.RecordBatch.from_pydict({"id": ["d5", "d6", "nope"], "pk": ["p5", "p6", "p1"]})
    assert sorted(pa.Table.from_batches(list(read(iter([batch])))).column("id").to_pylist()) == ["d5", "d6"]


def test_change_feed_batch_read_with_a_checkpoint(make_container, tmp_path: Path) -> None:
    from cosmosdb_datasource import commit_batch_checkpoint

    name, container = make_container()
    write(container, 0, 40)
    location = str(tmp_path / "cf")
    opts = options(name, change_feed_mode="LatestVersion", batch_checkpoint_location=location)
    first, _ = run_batch(opts)
    assert sorted(r[0] for r in first) == sorted(f"d{i}" for i in range(40))
    assert commit_batch_checkpoint(location)
    write(container, 40, 5)
    second, _ = run_batch(opts)
    assert sorted(r[0] for r in second) == sorted(f"d{i}" for i in range(40, 45))


@requires("requires_epk_ranges")
def test_batch_epk_sub_split_hash_v1(make_container) -> None:
    name, container = make_container(version=1)
    seed(container, 2000, keys=200)
    rows, parts, sizes = parallel_batch(options(name, partitions_per_feed_range=4))
    assert len(parts) == 4 * feed_range_count(container)
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)) == 2000
    mean = sum(sizes) / len(sizes)
    assert all(abs(s - mean) / mean < 0.5 for s in sizes), f"unbalanced V1 sub-split: {sizes}"


def test_batch_read_returns_a_recreated_document_once(make_container, monkeypatch: pytest.MonkeyPatch) -> None:
    """A document deleted and re-created after the read returned it is a new document (new _rid) that the
    scan reaches again; the read still returns its key once."""
    from cosmosdb_datasource.backend import SdkBackend

    name, container = make_container()
    write(container, 0, 120)
    original = SdkBackend.query_pages
    pages: list[int] = []

    def churn(self, *args, **kwargs):
        for page in original(self, *args, **kwargs):
            yield page
            pages.append(len(page.items))
            if len(pages) == 2:
                for doc in page.items[:3]:  # already read
                    container.delete_item(doc["id"], partition_key=doc["pk"])
                    container.upsert_item({**document(int(doc["id"][1:])), "recreated": True})

    monkeypatch.setattr(SdkBackend, "query_pages", churn)
    rows, _ = run_batch(options(name, page_size=20))
    keys = [(r[1], r[0]) for r in rows]
    assert len(pages) > 2
    assert len(keys) == len(set(keys)) == 120


@pytest.fixture(scope="module")
def multi_partition(sdk_db: Any) -> Iterator[tuple[str, Any, int]]:
    """Container with several physical partitions and 2000 documents."""
    with temporary_container(sdk_db, throughput=MULTI_PARTITION_THROUGHPUT) as (name, container):
        ranges = feed_range_count(container)
        assert ranges > 1, f"{MULTI_PARTITION_THROUGHPUT} RU/s gave a single physical partition"
        seed(container, 2000, keys=200)
        yield name, container, ranges


@requires("requires_multi_partition")
def test_batch_multi_partition(multi_partition) -> None:
    name, _container, ranges = multi_partition
    rows, parts, sizes = parallel_batch(options(name))
    assert len(parts) == ranges and all(sizes)
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)) == 2000


@requires("requires_multi_partition", "requires_epk_ranges")
def test_batch_multi_partition_sub_split(multi_partition) -> None:
    name, _container, ranges = multi_partition
    rows, parts, _sizes = parallel_batch(options(name, partitions_per_feed_range=3))
    assert len(parts) == 3 * ranges
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)) == 2000


@requires("requires_aad")
def test_batch_default_credential(seeded) -> None:
    pytest.importorskip("azure.identity")
    name, _container, n = seeded
    rows, parts = run_batch({"endpoint": ENDPOINT, "database": DATABASE, "container": name, "auth_type": "default"})
    assert parts[0].token is not None and "redacted" in repr(parts[0].token)
    assert len(rows) == n


def test_stream_offsets_are_stable_without_changes(make_container) -> None:
    name, container = make_container()
    write(container, 0, 20)
    h = StreamHarness(options(name, change_feed_mode="LatestVersion", start_from="Now"))
    start = json.loads(h.start())
    assert len(start["ranges"]) == feed_range_count(container)
    assert h.run_batch() == []  # batch 0 always runs for a new query
    assert h.run_batch() is None


def test_stream_latest_version_incremental_and_restart(make_container) -> None:
    name, container = make_container()
    write(container, 0, 120)
    container.execute_item_batch(
        [("upsert", ({"id": f"tb{i}", "pk": "batch", "seq": i},)) for i in range(30)], partition_key="batch"
    )
    h = StreamHarness(options(name, change_feed_mode="LatestVersion", page_size=25))
    h.start()
    first = h.run_batch()
    assert sorted(h.column(first, "id")) == sorted([f"d{i}" for i in range(120)] + [f"tb{i}" for i in range(30)])
    tb_lsns = {lsn for i, lsn in zip(h.column(first, "id"), h.column(first, "lsn"), strict=True) if i.startswith("tb")}
    assert len(tb_lsns) == 1, "a transactional batch shares one LSN"
    assert set(h.column(first, "operation")) == {"upsert"}
    assert h.run_batch() is None
    write(container, 120, 10)
    write(container, 0, 5, updated=True)
    second = h.run_batch()
    assert sorted(h.column(second, "id")) == sorted([f"d{i}" for i in range(120, 130)] + [f"d{i}" for i in range(5)])
    h.restart()
    assert h.run_batch() is None
    write(container, 130, 3)
    assert sorted(h.column(h.run_batch(), "id")) == ["d130", "d131", "d132"]


def test_stream_start_from_in_the_future(make_container) -> None:
    """A stream configured to start at a time that has not come yet delivers only changes from then on."""
    from datetime import datetime, timedelta, timezone

    name, container = make_container()
    start = (datetime.now(timezone.utc) + timedelta(seconds=45)).replace(microsecond=0)
    h = StreamHarness(options(name, start_from=start.strftime("%Y-%m-%dT%H:%M:%SZ")))
    h.start()
    write(container, 0, 5)
    written = {
        d["id"]: d["_ts"] for d in container.query_items("SELECT c.id, c._ts FROM c", enable_cross_partition_query=True)
    }
    expected = sorted(i for i, ts in written.items() if ts >= start.timestamp())  # empty unless the endpoint is slow
    assert sorted(h.column(h.run_until_idle(), "id")) == expected
    time.sleep(max(0.0, (start - datetime.now(timezone.utc)).total_seconds()) + 3)
    write(container, 10, 5)
    assert sorted(h.column(h.run_until_idle(), "id")) == [f"d{i}" for i in range(10, 15)]


def test_stream_max_items_per_trigger(make_container) -> None:
    name, container = make_container()
    write(container, 0, 150)
    h = StreamHarness(options(name, change_feed_mode="LatestVersion", max_items_per_trigger=40))
    h.start()
    rows = h.run_until_idle()
    assert len(h.batches) > 1
    assert sorted(h.column(rows, "id")) == sorted(f"d{i}" for i in range(150))


@requires("requires_epk_ranges")
def test_stream_epk_sub_split(make_container) -> None:
    name, container = make_container()
    write(container, 0, 150)
    whole = StreamHarness(options(name, change_feed_mode="LatestVersion"))
    split = StreamHarness(options(name, change_feed_mode="LatestVersion", partitions_per_feed_range=4))
    whole.start(), split.start()
    all_whole, all_split = whole.run_until_idle(), split.run_until_idle()
    assert sorted(split.column(all_split, "id")) == sorted(whole.column(all_whole, "id"))
    assert len(set(split.column(all_split, "id"))) == len(all_split) == 150


@requires("requires_multi_partition")
def test_stream_multi_partition(make_container) -> None:
    name, container = make_container(throughput=MULTI_PARTITION_THROUGHPUT)
    ranges = feed_range_count(container)
    assert ranges > 1
    seed(container, 1000, keys=200)
    h = StreamHarness(options(name, change_feed_mode="LatestVersion", max_items_per_trigger=50))
    start = json.loads(h.start())
    assert len(start["ranges"]) == ranges
    rows = h.run_until_idle()
    assert len(h.batches) > 1
    assert sorted(h.column(rows, "id")) == sorted(f"d{i}" for i in range(1000))
    assert len(set(h.column(rows, "feed_range"))) == ranges
    h.restart()
    write(container, 5000, 40, keys=200)
    assert sorted(h.column(h.run_until_idle(), "id")) == sorted(f"d{i}" for i in range(5000, 5040))


@requires("requires_avad")
def test_stream_all_versions_and_deletes(make_container) -> None:
    name, container = make_container()
    h = StreamHarness(options(name, change_feed_mode="AllVersionsAndDeletes", page_size=10))
    h.start()
    container.upsert_item({"id": "avad", "pk": "x", "v": 1})
    container.upsert_item({"id": "avad", "pk": "x", "v": 2})
    container.delete_item("avad", partition_key="x")
    write(container, 500, 15)
    rows = h.run_batch()
    events = [(r[0], r[2], r[4]) for r in rows if r[0] == "avad"]
    assert [op for _, op, _ in sorted(events, key=lambda e: e[2])] == ["create", "replace", "delete"]
    delete = next(r for r in rows if r[0] == "avad" and r[2] == "delete")
    col = {n: i for i, n in enumerate(h.schema.fieldNames())}
    assert delete[col["pk"]] == "x" and delete[col["body"]] is None and delete[col["_ts"]] == delete[col["crts"]]
    # A replay of the logged batch after further writes returns identical rows.
    write(container, 515, 5)
    container.delete_item("d500", partition_key=f"p{500 % 13}")
    assert h.read_range(*h.offset_log[0]) == rows
    nxt = h.run_batch()
    assert sorted((r[0], r[2]) for r in nxt) == sorted(
        [(f"d{i}", "create") for i in range(515, 520)] + [("d500", "delete")]
    )


@requires("requires_avad")
def test_partition_key_rendering_matches_across_row_types(make_container) -> None:
    """Delete rows carry the pk of the document's other rows: numbers above 2**53, JSON null and undefined."""
    from azure.cosmos.partition_key import NonePartitionKeyValue

    name, container = make_container()
    h = StreamHarness(options(name, change_feed_mode="AllVersionsAndDeletes"))
    h.start()
    h.run_batch()
    docs = [{"id": "big", "pk": 2**53 + 1}, {"id": "max", "pk": 2**63 - 1}, {"id": "same", "pk": None}, {"id": "same"}]
    for doc in docs:
        container.upsert_item(doc)
    batch_rows, _ = run_batch(options(name))
    container.delete_item("big", partition_key=2**53 + 1)
    container.delete_item("max", partition_key=2**63 - 1)
    container.delete_item("same", partition_key=None)
    container.delete_item("same", partition_key=NonePartitionKeyValue)
    time.sleep(2)
    rows = [dict(zip(h.schema.fieldNames(), r, strict=True)) for r in h.run_until_idle()]
    by_op: dict[str, list[tuple[Any, Any]]] = {}
    for r in rows:
        by_op.setdefault(r["operation"], []).append((r["pk"], r["id"]))
    expected = sorted([(str(2**53), "big"), (str(2**63), "max"), ("null", "same"), (None, "same")], key=str)
    assert sorted(by_op["create"], key=str) == expected
    assert sorted(by_op["delete"], key=str) == expected
    assert sorted(((r[1], r[0]) for r in batch_rows), key=str) == expected


@requires("requires_avad")
def test_versions_of_a_document_sharing_an_lsn_are_ordered(make_container) -> None:
    """A transactional batch writing a document three times: one LSN and one _ts, ordered by lsn_seq."""
    name, container = make_container()
    h = StreamHarness(options(name, change_feed_mode="AllVersionsAndDeletes"))
    h.start()
    h.run_batch()
    for i in range(5):
        doc_id = f"t{i}"
        container.execute_item_batch(
            [
                ("create", ({"id": doc_id, "pk": "tx", "v": 1},)),
                ("replace", (doc_id, {"id": doc_id, "pk": "tx", "v": 2})),
                ("upsert", ({"id": doc_id, "pk": "tx", "v": 3},)),
            ],
            partition_key="tx",
        )
    time.sleep(2)
    rows = [dict(zip(h.schema.fieldNames(), r, strict=True)) for r in h.run_until_idle()]
    for i in range(5):
        mine = [r for r in rows if r["id"] == f"t{i}"]
        assert [r["lsn_seq"] for r in mine] == [0, 1, 2] and len({(r["_ts"], r["lsn"]) for r in mine}) == 1
        last = max(mine, key=lambda r: (r["lsn"], r["lsn_seq"]))
        assert json.loads(last["body"])["v"] == 3


def _reused_executor(inbox: Any, outbox: Any) -> None:
    """A long-lived Python worker: reads the partitions it is sent, keeping its client cache between them."""
    while (job := inbox.get()) is not None:
        reader_blob, partition_blob = job
        try:
            rows = cloudpickle.loads(reader_blob).read(pickle.loads(partition_blob))
            outbox.put(("ok", sorted(r[0] for r in rows)))
        except Exception as exc:
            outbox.put(("error", f"{type(exc).__name__}: {exc}"[:500]))


def test_new_stream_after_the_container_is_recreated_in_a_reused_worker(make_container, sdk_db) -> None:
    """A worker that read the old container instance reads a new stream on the re-created container."""
    import multiprocessing

    from azure.cosmos import PartitionKey

    name, container = make_container()
    write(container, 0, 3)
    ctx = multiprocessing.get_context("spawn")
    inbox, outbox = ctx.Queue(), ctx.Queue()
    worker = ctx.Process(target=_reused_executor, args=(inbox, outbox), daemon=True)
    worker.start()

    def read_in_worker(h: StreamHarness) -> list[Any]:
        start = h.start()
        end = h.latest()
        results = []
        for part in h.planner.partitions(json.loads(start), json.loads(end)):
            inbox.put((h._executor_blob, pickle.dumps(part)))
            status, value = outbox.get(timeout=300)
            assert status == "ok", value
            results.extend(value)
        return sorted(results)

    try:
        assert read_in_worker(StreamHarness(options(name, change_feed_mode="LatestVersion"))) == ["d0", "d1", "d2"]
        sdk_db.delete_container(name)
        recreated = None
        for attempt in range(6):
            try:
                recreated = sdk_db.create_container_if_not_exists(name, partition_key=PartitionKey(path="/pk"))
                break
            except Exception:
                if attempt == 5:
                    raise
                time.sleep(5)
        for i in range(3):
            _RetryingContainer(recreated).upsert_item(document(100 + i))
        fresh = StreamHarness(options(name, change_feed_mode="LatestVersion"))  # a new checkpoint
        assert read_in_worker(fresh) == ["d100", "d101", "d102"]
    finally:
        inbox.put(None)
        worker.join(timeout=60)


def _merge_latest(rows: list[dict[str, Any]]) -> dict[tuple[Any, Any], int]:
    """Pick the latest row per (pk, id) by ``struct(operation <> 'snapshot', lsn, lsn_seq)``; a delete means absent."""
    best: dict[tuple[Any, Any], dict[str, Any]] = {}
    for r in rows:
        key = (r["pk"], r["id"])
        rank = (r["operation"] != "snapshot", r["lsn"], r["lsn_seq"])
        if key not in best or rank > best[key]["rank"]:
            best[key] = {**r, "rank": rank}
    return {k: r["_ts"] for k, r in best.items() if r["operation"] != "delete"}


def _container_state(container: Any) -> dict[tuple[Any, Any], int]:
    query = "SELECT c.id, c.pk, c._ts FROM c"
    return {(d["pk"], d["id"]): d["_ts"] for d in container.query_items(query, enable_cross_partition_query=True)}


def _run_snapshot_batch(
    h: StreamHarness,
    start: str,
    end: str,
    *,
    before_read: Callable[[list[Any]], None] = lambda parts: None,
    on_row: Callable[[int, tuple], None] = lambda i, row: None,
) -> list[tuple]:
    """Run batch 0 as Spark does: log the offsets, plan, then read each partition in a fresh executor."""
    from cosmosdb_datasource import client as client_module

    h.offset_log.append((start, end))
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    before_read(parts)
    rows: list[tuple] = []
    for part in parts:
        # A new client per task, as in a separate executor process: its session container is empty,
        # so a session token on the wire can only come from the partition.
        client_module.close_all()
        executor = cloudpickle.loads(h._executor_blob)
        for i, row in enumerate(executor.read(pickle.loads(pickle.dumps(part)))):
            rows.append(row)
            on_row(i, row)
    validate_rows(rows, h.schema, h.ds)
    h.planner.commit(json.loads(end))
    h.committed = end
    h.batches.append(rows)
    return rows


def test_initial_snapshot_latest_version_converges(make_container) -> None:
    """LatestVersion with initial_snapshot: batch 0 is read while documents are inserted and updated."""
    name, container = make_container()
    seed(container, 500)
    h = StreamHarness(
        options(
            name,
            change_feed_mode="LatestVersion",
            start_from="Now",
            initial_snapshot="true",
            initial_snapshot_delay_seconds="0",
            page_size=50,
        )
    )
    names = h.schema.fieldNames()
    start = h.start()
    end = h.latest()
    new_ids = iter(range(10_000, 20_000))
    rng = random.Random(3)

    def mutate(i: int, _row: tuple) -> None:  # after every page: one insert, one update
        if i % 50 == 49:
            container.upsert_item(document(next(new_ids)))
            container.upsert_item(document(rng.randrange(500), updated=True))

    rows = [dict(zip(names, r, strict=True)) for r in _run_snapshot_batch(h, start, end, on_row=mutate)]
    assert {r["operation"] for r in rows} == {"snapshot"} and len(rows) >= 500
    rows += [dict(zip(names, r, strict=True)) for r in h.run_until_idle()]
    assert any(r["operation"] == "upsert" for r in rows)
    assert _merge_latest(rows) == _container_state(container)


@requires("requires_avad", "requires_epk_ranges", "requires_session_tokens")
def test_initial_snapshot_all_versions_with_concurrent_writes_converges(make_container) -> None:
    """AVAD with initial_snapshot: batch 0 is read while another thread inserts, updates and deletes.

    The container has one physical partition, so the snapshot is spread over four EPK sub-ranges.
    With Session consistency every snapshot query must carry the session token recorded in batch 0's
    end offset, and a token ahead of the service must be rejected with 404/1002.
    """
    from azure.cosmos import _base as sdk_base
    from azure.cosmos.exceptions import CosmosHttpResponseError

    from cosmosdb_datasource.backend import feed_range_dict
    from cosmosdb_datasource.offsets import Offset
    from cosmosdb_datasource.stream import CosmosSnapshotPartition

    name, container = make_container()
    if feed_range_count(container) != 1:
        pytest.skip("needs a container with a single physical partition")
    seeded_docs = 2000
    for start in range(0, seeded_docs, 50):  # transactional batches of 50 documents per partition key
        pk = f"p{(start // 50) % 20}"
        ops = [("upsert", ({"id": f"s{i}", "pk": pk, "v": 0, "pad": "x" * 200},)) for i in range(start, start + 50)]
        container.execute_item_batch(ops, partition_key=pk)

    opts = options(
        name,
        change_feed_mode="AllVersionsAndDeletes",
        initial_snapshot="true",
        initial_snapshot_delay_seconds="1",
        partitions_per_feed_range=4,
        page_size=50,
        max_items_per_trigger=100,
    )
    h = StreamHarness(opts, admission_control=True)
    names = h.schema.fieldNames()
    limit = h.planner.getDefaultReadLimit()

    # AvailableNow run 1: target, P0, then batch 0 (the snapshot) under concurrent writes.
    h.planner.prepareForTriggerAvailableNow()
    start = h.start()
    end = h.latest(limit)
    p0, e0 = Offset.from_dict(json.loads(start)), Offset.from_dict(json.loads(end))
    assert e0.ranges == p0.ranges and e0.init == p0.ranges and e0.snapshot == p0.snapshot
    tokens = dict(e0.snapshot.session)
    assert tokens, "Session consistency: the start probe must return a session token"
    for r in p0.ranges:  # the session token's global LSN is the probe's etag LSN
        assert int(tokens[r.key].split(":")[1].split("#")[1]) == r.lsn

    sent: list[tuple[str, str | None]] = []
    original = sdk_base.set_session_token_header

    def spy(conn, headers, path, request_object, options, partition_key_range_id=None):
        original(conn, headers, path, request_object, options, partition_key_range_id)
        if request_object.operation_type == "SqlQuery":
            sent.append((path, headers.get("x-ms-session-token")))

    seen: list[tuple[str, str]] = []
    stop = threading.Event()
    write_lsns: list[int] = []
    counts = {"insert": 0, "update": 0, "delete": 0, "recreate": 0}
    deleted: list[tuple[str, str]] = []

    def hook(headers, _body):
        write_lsns.append(int(headers.get("lsn", 0)))

    def writer() -> None:
        rng = random.Random(7)
        n = 0
        while not stop.is_set():
            r = rng.random()
            dead = set(deleted)
            candidates = [k for k in seen if k not in dead]
            if r < 0.25 or not candidates:
                container.upsert_item({"id": f"n{n}", "pk": f"p{n % 20}", "v": 1}, response_hook=hook)
                n += 1
                counts["insert"] += 1
            elif r < 0.6:  # update a document the snapshot has already returned
                pk, doc_id = rng.choice(candidates)
                for v in range(rng.randrange(1, 3)):
                    container.upsert_item({"id": doc_id, "pk": pk, "v": 10 + v}, response_hook=hook)
                counts["update"] += 1
            elif r < 0.85:  # delete a document the snapshot has already returned
                pk, doc_id = rng.choice(candidates)
                container.delete_item(doc_id, partition_key=pk, response_hook=hook)
                deleted.append((pk, doc_id))
                counts["delete"] += 1
            elif deleted:  # re-create a deleted document
                pk, doc_id = deleted.pop(rng.randrange(len(deleted)))
                container.upsert_item({"id": doc_id, "pk": pk, "v": 99}, response_hook=hook)
                counts["recreate"] += 1
            time.sleep(0.02)

    thread = threading.Thread(target=writer, daemon=True)

    def start_writer(parts: list[Any]) -> None:
        assert len(parts) == 4 and all(isinstance(p, CosmosSnapshotPartition) for p in parts)
        thread.start()

    def interleave(i: int, row: tuple) -> None:
        seen.append((row[1], row[0]))
        if i % 50 == 49:
            time.sleep(0.3)  # let the writer interleave with the page reads

    sdk_base.set_session_token_header = spy
    try:
        snapshot_rows = _run_snapshot_batch(h, start, end, before_read=start_writer, on_row=interleave)
    finally:
        stop.set()
        thread.join(timeout=60)
        sdk_base.set_session_token_header = original
    rows = [dict(zip(names, r, strict=True)) for r in snapshot_rows]
    assert {r["operation"] for r in rows} == {"snapshot"} and {r["lsn"] for r in rows} == {0}
    assert sent and all(token == tokens["-FF"] for _path, token in sent), "every snapshot query is pinned"
    # Run 1 continues: its AvailableNow target, captured before P0, moves once after batch 0 to the
    # current position, so the same run drains the changes made while the snapshot was read. The writer
    # stopped before batch 0 ended; the pause lets the replicas apply its last writes.
    last_write = max(write_lsns)
    time.sleep(2)
    rows += [dict(zip(names, r, strict=True)) for r in h.run_until_idle(limit)]
    run1_batches = len(h.batches)
    assert min(json.loads(h.committed)["ranges"].values()) >= last_write, "run 1 drained the writes"
    assert _merge_latest(rows) == _container_state(container), "complete after the first AvailableNow run"
    assert run1_batches > 2  # the snapshot plus capped change batches

    h.restart()  # a later run finds nothing new
    h.planner.prepareForTriggerAvailableNow()
    assert h.run_until_idle(limit) == []

    # The race was exercised: changes overtook rows already read by the snapshot.
    snap = {(r["pk"], r["id"]): r["_ts"] for r in rows if r["operation"] == "snapshot"}
    changes = [r for r in rows if r["operation"] != "snapshot"]
    assert any((r["pk"], r["id"]) in snap and r["_ts"] > snap[(r["pk"], r["id"])] for r in changes)
    assert any(r["operation"] == "delete" and (r["pk"], r["id"]) in snap for r in changes)
    assert len(snapshot_rows) >= seeded_docs and counts["delete"] and counts["update"] and counts["insert"]

    # A session token ahead of the service is refused (404/1002), not silently ignored.
    pk_range, _, vector = tokens["-FF"].partition(":")
    version, global_lsn = vector.split("#")[:2]
    future = f"{pk_range}:{version}#{int(global_lsn) + 10**7}"
    with pytest.raises(CosmosHttpResponseError) as info:
        list(container.query_items("SELECT * FROM c", feed_range=feed_range_dict("", "FF"), session_token=future))
    assert (info.value.status_code, info.value.sub_status) == (404, 1002)


@requires("requires_epk_ranges")
def test_snapshot_stream(make_container, sdk_db) -> None:
    """snapshot_stream=true: one complete snapshot per AvailableNow run on both PySpark protocol paths,
    snapshot_ts kept by a replay, and a re-created container rejected until the checkpoint is reset."""
    from azure.cosmos import PartitionKey

    from cosmosdb_datasource import client as client_module
    from cosmosdb_datasource.errors import CosmosOffsetError
    from cosmosdb_datasource.offsets import SnapshotStreamOffset

    name, container = make_container()
    for start in range(0, 600, 50):
        pk = f"p{(start // 50) % 12}"
        ops = [("upsert", ({"id": f"s{i}", "pk": pk, "v": 0},)) for i in range(start, start + 50)]
        container.execute_item_batch(ops, partition_key=pk)

    def actual() -> dict[tuple, tuple]:
        docs = container.query_items("SELECT c.id, c.pk, c._ts, c._etag FROM c", enable_cross_partition_query=True)
        return {(d["pk"], d["id"]): (d["_ts"], d["_etag"]) for d in docs}

    opts = options(name, snapshot_stream="true", partitions_per_feed_range=3, page_size=100)
    h = StreamHarness(opts, admission_control=True)
    names = h.schema.fieldNames()
    limit = h.planner.getDefaultReadLimit()

    def state(rows: list[tuple]) -> tuple[dict[tuple, tuple], set[Any]]:
        docs = [dict(zip(names, r, strict=True)) for r in rows]
        assert len(docs) == len({(d["pk"], d["id"]) for d in docs}), "a key appears twice in one snapshot"
        return {(d["pk"], d["id"]): (d["_ts"], d["_etag"]) for d in docs}, {d["snapshot_ts"] for d in docs}

    stamps = []
    for run in range(2):  # PySpark 4.2 / Databricks protocol: one snapshot per AvailableNow run
        if run:
            for i in range(60):
                container.upsert_item({"id": f"s{i}", "pk": f"p{(i // 50) % 12}", "v": run})
            for i in range(100, 120):
                container.delete_item(f"s{i}", partition_key=f"p{(i // 50) % 12}")
            for i in range(30):
                container.upsert_item({"id": f"n{i}", "pk": f"p{i % 12}", "v": -1})
            time.sleep(2)
            h.restart()
        h.planner.prepareForTriggerAvailableNow()
        if not run:
            h.start()
        before = len(h.batches)
        got, stamp = state(h.run_until_idle(limit))
        assert len(h.batches) == before + 1
        assert got == actual() and len(stamp) == 1
        stamps.append(SnapshotStreamOffset.from_dict(json.loads(h.committed)))
    assert stamps[1].seq == 2 and stamps[1].snapshot_datetime > stamps[0].snapshot_datetime

    # A new run without AvailableNow plans a snapshot; every query carries a session token.
    h.restart()
    start = h.committed
    end = h.latest(limit)
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    assert len(parts) == 3 * feed_range_count(container)
    if _skip_reason({"requires_session_tokens"}) is None:
        assert all(p.session_token for p in parts)

    # Replay: the batch was logged but not committed; after a restart it keeps snapshot_ts.
    h.offset_log.append((start, end))
    for i in range(5):
        container.upsert_item({"id": f"r{i}", "pk": "p1", "v": 5})
    time.sleep(2)
    h.restart()
    got, stamp = state(h.read_range(start, end))
    assert stamp == {SnapshotStreamOffset.from_dict(json.loads(end)).snapshot_datetime}
    assert got == actual()  # re-read: the container at replay time

    # PySpark 4.0/4.1 protocol on a new checkpoint.
    old = StreamHarness(opts, admission_control=False)
    old.start()
    got, stamp = state(old.run_batch() or [])
    assert got == actual() and len(stamp) == 1

    # A re-created container fails until the checkpoint is reset.
    h.planner.commit(json.loads(end))
    h.committed = end
    sdk_db.delete_container(name)
    recreated = None
    for attempt in range(6):
        try:
            recreated = sdk_db.create_container_if_not_exists(name, partition_key=PartitionKey(path="/pk"))
            break
        except Exception:
            if attempt == 5:
                raise
            time.sleep(5)
    _RetryingContainer(recreated).upsert_item({"id": "x", "pk": "p0", "v": 1})
    time.sleep(2)
    client_module.close_all()  # the SDK caches container properties per client
    h.restart()
    h.planner.prepareForTriggerAvailableNow()
    with pytest.raises(CosmosOffsetError, match="re-created"):
        h.run_batch(limit)
    fresh = StreamHarness(opts, admission_control=True)
    fresh.planner.prepareForTriggerAvailableNow()
    fresh.start()
    assert [dict(zip(names, r, strict=True))["id"] for r in fresh.run_until_idle(limit)] == ["x"]


@pytest.mark.spark
def test_spark_batch_and_stream(make_container, tmp_path: Path) -> None:
    """Batch and AvailableNow stream through a real local SparkSession (needs Java 17+)."""
    java = java_major()
    if java is None or java < 17:
        pytest.skip(f"local Spark needs Java 17+ (found: {java or 'none'})")
    from pyspark.sql import SparkSession

    from cosmosdb_datasource import CosmosDBDataSource

    name, container = make_container()
    seed(container, 300)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("cosmosdb-datasource-integration")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.dataSource.register(CosmosDBDataSource)
        df = spark.read.format("cosmosdb").options(**options(name)).load()
        assert df.count() == 300
        assert df.select("id").distinct().count() == 300

        def run_stream() -> None:
            query = (
                spark.readStream.format("cosmosdb")
                .options(**options(name, change_feed_mode="LatestVersion"))
                .load()
                .writeStream.format("parquet")
                .option("path", str(tmp_path / "out"))
                .option("checkpointLocation", str(tmp_path / "chk"))
                .trigger(availableNow=True)
                .start()
            )
            query.awaitTermination(300)
            assert query.exception() is None

        run_stream()
        write(container, 1000, 20)
        run_stream()
        out = spark.read.parquet(str(tmp_path / "out"))
        assert out.count() == 320 and out.select("id").distinct().count() == 320
    finally:
        spark.stop()
