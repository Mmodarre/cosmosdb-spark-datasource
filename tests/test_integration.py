"""Integration tests against Azure Cosmos DB for NoSQL or the Linux (vNext) emulator.

Configuration, from the environment:

``COSMOS_ENDPOINT``, ``COSMOS_KEY``
    Account endpoint and key. Required; every test is skipped without them. The emulator does not
    check the key, but the SDK signs requests with it, so it must be base64.
``COSMOS_DATABASE``
    Database for the test containers, created if missing (default ``cosmosdb_datasource_it``).
``COSMOS_EMULATOR=1``
    The endpoint is the emulator: tests that need a capability it lacks are skipped (see
    ``EMULATOR_UNSUPPORTED``).
``COSMOS_UNSUPPORTED``
    Comma-separated capability markers to skip on an account, e.g. ``requires_avad`` when
    the account has no AllVersionsAndDeletes change feed.
``COSMOS_MULTI_PARTITION_CONTAINER``
    An existing container in ``COSMOS_DATABASE`` with more than one physical partition (read only).
``COSMOS_AAD=1``
    Also run the ``auth_type=default`` test. ``DefaultAzureCredential`` must find an identity with a
    Cosmos DB data-plane role on the account.

Tests write only to containers they create (``it_<random>``) and delete them afterwards. Containers
are created without explicit throughput, so serverless accounts work too.
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
from tests.harness import StreamHarness, make_source, run_batch, validate_rows

ENDPOINT = os.environ.get("COSMOS_ENDPOINT", "")
KEY = os.environ.get("COSMOS_KEY", "")
DATABASE = os.environ.get("COSMOS_DATABASE") or "cosmosdb_datasource_it"
EMULATOR = os.environ.get("COSMOS_EMULATOR") == "1"
MULTI_PARTITION_CONTAINER = os.environ.get("COSMOS_MULTI_PARTITION_CONTAINER", "")
AAD = os.environ.get("COSMOS_AAD") == "1"

UNSUPPORTED = {s.strip() for s in os.environ.get("COSMOS_UNSUPPORTED", "").split(",") if s.strip()}

CAPABILITIES = {
    "requires_avad": "AllVersionsAndDeletes change feed",
    "requires_epk_ranges": "EPK sub-range filtering",
    "requires_multi_partition": "multiple physical partitions",
    "requires_session_tokens": "session token enforcement",
    "requires_aad": "Entra ID authentication",
}
# The vNext emulator lacks all of them (observed with image version EN20261008):
# AllVersionsAndDeletes requests return LatestVersion items, EPK sub-range filters are ignored,
# every container has one physical partition, session tokens are not enforced and requests are
# not authenticated.
EMULATOR_UNSUPPORTED = frozenset(CAPABILITIES)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not (ENDPOINT and KEY), reason="COSMOS_ENDPOINT and COSMOS_KEY are not set"),
]
if EMULATOR:
    # azure-cosmos does not verify TLS certificates for localhost endpoints.
    pytestmark.append(pytest.mark.filterwarnings("ignore::urllib3.exceptions.InsecureRequestWarning"))


@pytest.fixture(autouse=True)
def _capabilities(request: pytest.FixtureRequest) -> None:
    names = {m.name for m in request.node.iter_markers()}
    if EMULATOR and names & EMULATOR_UNSUPPORTED:
        missing = ", ".join(sorted(CAPABILITIES[n] for n in names & EMULATOR_UNSUPPORTED))
        pytest.skip(f"the emulator does not support: {missing}")
    if names & UNSUPPORTED:
        pytest.skip(f"disabled by COSMOS_UNSUPPORTED: {', '.join(sorted(names & UNSUPPORTED))}")
    if "requires_multi_partition" in names and not MULTI_PARTITION_CONTAINER:
        pytest.skip("COSMOS_MULTI_PARTITION_CONTAINER is not set")
    if "requires_aad" in names and not AAD:
        pytest.skip("COSMOS_AAD is not set")


@pytest.fixture(scope="session")
def sdk_db() -> Iterator[Any]:
    from azure.cosmos import CosmosClient

    client = CosmosClient(ENDPOINT, credential=KEY)
    yield client.create_database_if_not_exists(DATABASE)
    client.__exit__(None, None, None)  # CosmosClient.close() does not exist in azure-cosmos 4.14


@contextlib.contextmanager
def temporary_container(db: Any, *, version: int = 2) -> Iterator[tuple[str, Any]]:
    from azure.cosmos import PartitionKey
    from azure.cosmos.exceptions import CosmosResourceNotFoundError

    name = f"it_{uuid.uuid4().hex[:10]}"
    try:
        container = None
        for attempt in range(4):
            try:
                # Idempotent: a client-side timeout may still have created the container.
                container = db.create_container_if_not_exists(
                    name, partition_key=PartitionKey(path="/pk", kind="Hash", version=version)
                )
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))
        yield name, container
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
    """``n`` documents, written in transactional batches (at most 100 operations, one partition key)."""
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
    """Like ``harness.run_batch``, but runs the partitions concurrently, one executor per thread."""
    ds = make_source(opts)
    schema = ds.schema()
    reader = ds.reader(schema)
    shipped = cloudpickle.dumps(reader)
    parts = reader.partitions()

    def run(part: Any) -> list[tuple]:
        rows = list(cloudpickle.loads(shipped).read(part))
        validate_rows(rows, schema, ds)
        return rows

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(run, parts))
    return [r for rows in results for r in rows], parts, [len(rows) for rows in results]


# -- batch reads ------------------------------------------------------------------------------------


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


@pytest.mark.requires_epk_ranges
def test_batch_epk_sub_split_hash_v1(make_container) -> None:
    name, container = make_container(version=1)
    seed(container, 2000, keys=200)
    rows, parts, sizes = parallel_batch(options(name, partitions_per_feed_range=4))
    assert len(parts) == 4 * feed_range_count(container)
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)) == 2000
    mean = sum(sizes) / len(sizes)
    assert all(abs(s - mean) / mean < 0.5 for s in sizes), f"unbalanced V1 sub-split: {sizes}"


@pytest.mark.requires_multi_partition
def test_batch_multi_partition_container(sdk_db) -> None:
    container = sdk_db.get_container_client(MULTI_PARTITION_CONTAINER)
    ranges = feed_range_count(container)
    assert ranges > 1, f"{MULTI_PARTITION_CONTAINER} has a single physical partition"
    before = count(container)
    rows, parts, _sizes = parallel_batch(options(MULTI_PARTITION_CONTAINER, query="SELECT c.id FROM c", page_size=5000))
    assert len(parts) == ranges
    keys = [(r[1], r[0]) for r in rows]
    assert len(keys) == len(set(keys))
    if count(container) != before:
        pytest.skip(f"{MULTI_PARTITION_CONTAINER} changed during the read; exact count not checkable")
    assert len(keys) == before


@pytest.mark.requires_aad
def test_batch_default_credential(seeded) -> None:
    pytest.importorskip("azure.identity")
    name, _container, n = seeded
    rows, parts = run_batch({"endpoint": ENDPOINT, "database": DATABASE, "container": name, "auth_type": "default"})
    assert parts[0].token is not None and "redacted" in repr(parts[0].token)
    assert len(rows) == n


# -- change feed ------------------------------------------------------------------------------------


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


def test_stream_max_items_per_trigger(make_container) -> None:
    name, container = make_container()
    write(container, 0, 150)
    h = StreamHarness(options(name, change_feed_mode="LatestVersion", max_items_per_trigger=40))
    h.start()
    rows = h.run_until_idle()
    assert len(h.batches) > 1
    assert sorted(h.column(rows, "id")) == sorted(f"d{i}" for i in range(150))


@pytest.mark.requires_epk_ranges
def test_stream_epk_sub_split(make_container) -> None:
    name, container = make_container()
    write(container, 0, 150)
    whole = StreamHarness(options(name, change_feed_mode="LatestVersion"))
    split = StreamHarness(options(name, change_feed_mode="LatestVersion", partitions_per_feed_range=4))
    whole.start(), split.start()
    all_whole, all_split = whole.run_until_idle(), split.run_until_idle()
    assert sorted(split.column(all_split, "id")) == sorted(whole.column(all_whole, "id"))
    assert len(set(split.column(all_split, "id"))) == len(all_split) == 150


@pytest.mark.requires_avad
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
    assert delete[1] == "x" and delete[7] is None and delete[3] == delete[5]
    # A replay of the logged batch after further writes returns identical rows.
    write(container, 515, 5)
    container.delete_item("d500", partition_key=f"p{500 % 13}")
    assert h.read_range(*h.offset_log[0]) == rows
    nxt = h.run_batch()
    assert sorted((r[0], r[2]) for r in nxt) == sorted(
        [(f"d{i}", "create") for i in range(515, 520)] + [("d500", "delete")]
    )


# -- initial snapshot -------------------------------------------------------------------------------


def _merge_latest(rows: list[dict[str, Any]]) -> dict[tuple[Any, Any], int]:
    """Latest row per (pk, id) by struct(operation <> 'snapshot', _ts, lsn); a delete means absent."""
    best: dict[tuple[Any, Any], dict[str, Any]] = {}
    for r in rows:
        key = (r["pk"], r["id"])
        rank = (r["operation"] != "snapshot", r["_ts"], r["lsn"])
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
    """Batch 0 as Spark runs it: log the offsets, plan, then read each partition in a fresh executor."""
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


@pytest.mark.requires_avad
@pytest.mark.requires_epk_ranges
@pytest.mark.requires_session_tokens
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
    rows += [dict(zip(names, r, strict=True)) for r in h.run_until_idle(limit)]

    # Later AvailableNow runs drain the feed (repeated only if a probe lagged behind the last write).
    last_write = max(write_lsns)
    for _ in range(5):
        time.sleep(1)
        h.restart()
        h.planner.prepareForTriggerAvailableNow()
        rows += [dict(zip(names, r, strict=True)) for r in h.run_until_idle(limit)]
        if min(json.loads(h.committed)["ranges"].values()) >= last_write:
            break
    assert min(json.loads(h.committed)["ranges"].values()) >= last_write
    assert _merge_latest(rows) == _container_state(container)

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


# -- local SparkSession -----------------------------------------------------------------------------


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
