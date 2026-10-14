"""Integration tests against the real account described by config/cosmos.local.json.

Skipped unless the config file exists. Shared containers (consults, events) are only read; all
writes go to dedicated containers (``it_<random>``, ``it_snap_<random>``, 400 RU/s) created and
deleted by this module.

Auth: account key from $COSMOS_KEY or ``auth.key_fetch_command`` (az CLI), and AAD via
``auth_type=default`` (AzureCliCredential through DefaultAzureCredential).
"""

from __future__ import annotations

import contextlib
import json
import os
import shlex
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from pyspark import cloudpickle
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType, TimestampType

from tests.harness import StreamHarness, make_source, run_batch, validate_rows

CONFIG = Path(__file__).resolve().parents[2] / "config" / "cosmos.local.json"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not CONFIG.exists(), reason=f"{CONFIG} not found (no real Cosmos DB account configured)"),
]


@pytest.fixture(scope="session")
def cfg() -> dict:
    return json.loads(CONFIG.read_text())


@pytest.fixture(scope="session")
def account_key(cfg: dict) -> str:
    key = os.environ.get("COSMOS_KEY")
    if key:
        return key
    command = cfg.get("auth", {}).get("key_fetch_command")
    if not command:
        pytest.skip("no COSMOS_KEY and no key_fetch_command in config")
    proc = subprocess.run(shlex.split(command), capture_output=True, text=True, timeout=120, check=False)
    if proc.returncode != 0 or not proc.stdout.strip():
        pytest.skip("could not fetch the account key with the az CLI")
    return proc.stdout.strip()


@pytest.fixture(scope="session")
def sdk_db(cfg: dict, account_key: str):
    from azure.cosmos import CosmosClient

    client = CosmosClient(cfg["endpoint"], credential=account_key)
    yield client.get_database_client(cfg["database"])
    client.__exit__(None, None, None)  # close() is missing in azure-cosmos 4.14


def key_options(cfg: dict, key: str, container: str, **extra) -> dict[str, str]:
    opts = {
        "endpoint": cfg["endpoint"],
        "database": cfg["database"],
        "container": container,
        "auth_type": "key",
        "account_key": key,
    }
    opts.update({k: str(v) for k, v in extra.items()})
    return opts


def aad_options(cfg: dict, container: str, **extra) -> dict[str, str]:
    opts = {"endpoint": cfg["endpoint"], "database": cfg["database"], "container": container, "auth_type": "default"}
    opts.update({k: str(v) for k, v in extra.items()})
    return opts


def count(db, container: str) -> int:
    c = db.get_container_client(container)
    return next(iter(c.query_items("SELECT VALUE COUNT(1) FROM c", enable_cross_partition_query=True)))


def feed_range_count(db, container: str) -> int:
    return len(list(db.get_container_client(container).read_feed_ranges(force_refresh=True)))


def parallel_batch(options: dict[str, str], schema=None, workers: int = 8) -> tuple[list[tuple], list, list[int]]:
    """Like harness.run_batch, but executes partitions concurrently (one 'executor' per thread)."""
    ds = make_source(options)
    schema = schema or ds.schema()
    reader = ds.reader(schema)
    shipped = cloudpickle.dumps(reader)
    parts = reader.partitions()

    def run(part):
        rows = list(cloudpickle.loads(shipped).read(part))
        validate_rows(rows, schema, ds)
        return rows

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(run, parts))
    return [r for rows in results for r in rows], parts, [len(rows) for rows in results]


def assert_count(db, container: str, before: int, rows: list[tuple]) -> None:
    after = count(db, container)
    ids = [r[0] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate documents in batch read"
    if before == after:
        assert len(ids) == before
    else:  # someone mutated the shared container during the read
        pytest.skip(f"{container} changed during the read ({before} -> {after}); exact count not checkable")


# -- batch: shared containers (read-only) -----------------------------------------------------


def test_batch_consults_key_auth(cfg, account_key, sdk_db) -> None:
    before = count(sdk_db, "consults")
    rows, parts = run_batch(key_options(cfg, account_key, "consults"))
    assert len(parts) == feed_range_count(sdk_db, "consults")
    assert_count(sdk_db, "consults", before, rows)
    sample = rows[0]
    body = json.loads(sample[4])
    assert sample[0] == body["id"] and sample[1] == body["patientId"] and sample[2] == body["_ts"]


def test_batch_consults_sub_split_v1(cfg, account_key, sdk_db) -> None:
    before = count(sdk_db, "consults")
    rows, parts, sizes = parallel_batch(key_options(cfg, account_key, "consults", partitions_per_feed_range=4))
    assert len(parts) == 4 * feed_range_count(sdk_db, "consults")
    assert_count(sdk_db, "consults", before, rows)
    mean = sum(sizes) / len(sizes)
    assert all(abs(s - mean) / mean < 0.5 for s in sizes), f"unbalanced V1 sub-split: {sizes}"


def test_batch_consults_aad_default_credential(cfg, sdk_db) -> None:
    pytest.importorskip("azure.identity")
    before = count(sdk_db, "consults")
    try:
        rows, parts = run_batch(aad_options(cfg, "consults"))
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"AAD auth not available here: {type(exc).__name__}: {exc}")
    assert parts[0].token is not None and "redacted" in repr(parts[0].token)
    assert_count(sdk_db, "consults", before, rows)


def test_batch_consults_user_schema(cfg, account_key) -> None:
    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("patientId", StringType()),
            StructField("seq", LongType()),
            StructField("createdAt", TimestampType()),
            StructField("tags", ArrayType(StringType())),
            StructField("_corrupt_record", StringType()),
        ]
    )
    rows, _ = run_batch(key_options(cfg, account_key, "consults", page_size=500), schema)
    assert rows and all(r[5] is None for r in rows), "unexpected corrupt records"
    assert all(isinstance(r[2], int) and r[3] is not None for r in rows)


def test_batch_events_all_feed_ranges(cfg, account_key, sdk_db) -> None:
    ranges = feed_range_count(sdk_db, "events")
    assert ranges >= 4
    before = count(sdk_db, "events")
    opts = key_options(
        cfg,
        account_key,
        "events",
        query="SELECT c.id FROM c",
        page_size=5000,
        partitions_per_feed_range=2,
        throttle_max_wait_seconds=300,
        log_metrics="true",
    )
    rows, parts, _sizes = parallel_batch(opts, workers=8)
    assert len(parts) == 2 * ranges
    assert_count(sdk_db, "events", before, rows)


def test_batch_events_auto_partitioning(cfg, account_key, sdk_db) -> None:
    ds = make_source(key_options(cfg, account_key, "events", partitions_per_feed_range="auto"))
    parts = ds.reader(ds.schema()).partitions()
    ranges = feed_range_count(sdk_db, "events")
    assert len(parts) % ranges == 0  # ~1 GB container -> 1 split per range with the 1 GiB target


def test_stream_offsets_on_events_are_cheap_and_stable(cfg, account_key, sdk_db) -> None:
    h = StreamHarness(key_options(cfg, account_key, "events", change_feed_mode="LatestVersion", start_from="Now"))
    start = json.loads(h.start())
    assert len(start["ranges"]) == feed_range_count(sdk_db, "events")
    first = h.run_batch()  # batch 0 always runs for a new query
    if first:  # concurrent mutations by datagen are possible
        pytest.skip("events changed while probing offsets")
    assert h.run_batch() is None


def test_stream_consults_from_beginning(cfg, account_key, sdk_db) -> None:
    before = count(sdk_db, "consults")
    h = StreamHarness(key_options(cfg, account_key, "consults", change_feed_mode="LatestVersion", page_size=1000))
    h.start()
    rows = h.run_batch()
    ids = h.column(rows, "id")
    assert len(ids) == len(set(ids))
    if count(sdk_db, "consults") == before:
        assert len(ids) == before
    assert set(h.column(rows, "operation")) == {"upsert"}


# -- streaming: dedicated test container ------------------------------------------------------


@pytest.fixture(scope="module")
def test_container(sdk_db):
    import time

    from azure.cosmos import PartitionKey
    from azure.cosmos.exceptions import CosmosResourceNotFoundError

    name = f"it_{uuid.uuid4().hex[:8]}"
    try:
        container = None
        for attempt in range(4):
            try:
                # idempotent: a client-side timeout may still have created the container server-side
                container = sdk_db.create_container_if_not_exists(
                    name, partition_key=PartitionKey(path="/pk", kind="Hash", version=1), offer_throughput=400
                )
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))
        yield name, container
    finally:  # always clean up, even if creation raised after the service created the container
        with contextlib.suppress(CosmosResourceNotFoundError):
            sdk_db.delete_container(name)


def write(container, start: int, n: int, keys: int = 13, **fields) -> None:
    for i in range(start, start + n):
        container.upsert_item({"id": f"d{i}", "pk": f"p{i % keys}", "v": i, **fields})


def test_stream_latest_version_incremental_and_restart(cfg, account_key, test_container) -> None:
    name, container = test_container
    write(container, 0, 120)
    container.execute_item_batch(
        [("upsert", ({"id": f"tb{i}", "pk": "batch", "v": i},)) for i in range(30)], partition_key="batch"
    )
    h = StreamHarness(key_options(cfg, account_key, name, change_feed_mode="LatestVersion", page_size=25))
    h.start()
    first = h.run_batch()
    ids = sorted(h.column(first, "id"))
    assert ids == sorted([f"d{i}" for i in range(120)] + [f"tb{i}" for i in range(30)])
    tb_lsns = {lsn for i, lsn in zip(h.column(first, "id"), h.column(first, "lsn"), strict=True) if i.startswith("tb")}
    assert len(tb_lsns) == 1, "a transactional batch shares one LSN"
    assert h.run_batch() is None
    write(container, 120, 10)
    write(container, 0, 5, updated=True)
    second = h.run_batch()
    assert sorted(h.column(second, "id")) == sorted([f"d{i}" for i in range(120, 130)] + [f"d{i}" for i in range(5)])
    h.restart()
    assert h.run_batch() is None
    write(container, 130, 3)
    assert sorted(h.column(h.run_batch(), "id")) == ["d130", "d131", "d132"]


def test_stream_all_versions_and_deletes(cfg, account_key, test_container) -> None:
    name, container = test_container
    h = StreamHarness(key_options(cfg, account_key, name, change_feed_mode="AllVersionsAndDeletes", page_size=10))
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
    # replay of the checkpointed range after further writes returns identical rows
    write(container, 515, 5)
    container.delete_item("d500", partition_key=f"p{500 % 13}")
    assert h.read_range(*h.offset_log[0]) == rows
    nxt = h.run_batch()
    assert sorted((r[0], r[2]) for r in nxt) == sorted(
        [(f"d{i}", "create") for i in range(515, 520)] + [("d500", "delete")]
    )


def test_stream_cap_and_sub_split(cfg, account_key, test_container) -> None:
    name, container = test_container
    write(container, 1000, 150)  # self-contained: do not depend on data from other tests
    capped = StreamHarness(
        key_options(cfg, account_key, name, change_feed_mode="LatestVersion", max_items_per_trigger=40)
    )
    split = StreamHarness(
        key_options(cfg, account_key, name, change_feed_mode="LatestVersion", partitions_per_feed_range=4)
    )
    capped.start(), split.start()
    all_capped = capped.run_until_idle()
    all_split = split.run_until_idle()
    assert len(capped.batches) > 1
    assert sorted(capped.column(all_capped, "id")) == sorted(split.column(all_split, "id"))
    assert len(set(capped.column(all_capped, "id"))) == len(all_capped)


# -- initial snapshot under concurrent writes: dedicated container ----------------------------


@pytest.fixture
def snapshot_container(sdk_db):
    import time

    from azure.cosmos import PartitionKey
    from azure.cosmos.exceptions import CosmosResourceNotFoundError

    name = f"it_snap_{uuid.uuid4().hex[:8]}"
    try:
        container = None
        for attempt in range(4):
            try:
                container = sdk_db.create_container_if_not_exists(
                    name, partition_key=PartitionKey(path="/pk"), offer_throughput=400
                )
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(5 * (attempt + 1))
        yield name, container
    finally:  # always clean up, including on failure
        with contextlib.suppress(CosmosResourceNotFoundError):
            sdk_db.delete_container(name)


def _merge_latest(rows: list[dict]) -> dict[tuple, int]:
    """README rule: latest row per (pk, id) by struct(operation <> 'snapshot', _ts, lsn); delete = absent."""
    best: dict[tuple, dict] = {}
    for r in rows:
        key = (r["pk"], r["id"])
        rank = (r["operation"] != "snapshot", r["_ts"], r["lsn"])
        if key not in best or rank > best[key]["rank"]:
            best[key] = {**r, "rank": rank}
    return {k: r["_ts"] for k, r in best.items() if r["operation"] != "delete"}


def test_initial_snapshot_with_concurrent_writes_converges(cfg, account_key, sdk_db, snapshot_container) -> None:
    """batch 0 = snapshot read while another thread inserts, updates and deletes; then AvailableNow drains.

    At 400 RU/s the container has one physical partition, so the snapshot is spread over four EPK
    sub-ranges (partitions_per_feed_range=4). The account's default consistency (Session) makes the
    session-token pinning observable: every snapshot query must carry the token recorded in batch 0's end
    offset, and a token ahead of the service must be rejected with 404/1002.
    """
    import pickle
    import random
    import threading
    import time

    from azure.cosmos import _base as sdk_base
    from azure.cosmos.exceptions import CosmosHttpResponseError

    from cosmosdb_datasource import client as client_module
    from cosmosdb_datasource.backend import feed_range_dict
    from cosmosdb_datasource.offsets import Offset
    from cosmosdb_datasource.stream import CosmosSnapshotPartition

    name, container = snapshot_container
    seeded = 2000
    for start in range(0, seeded, 50):  # transactional batches of 50 documents per partition key
        pk = f"p{(start // 50) % 20}"
        ops = [("upsert", ({"id": f"s{i}", "pk": pk, "v": 0, "pad": "x" * 200},)) for i in range(start, start + 50)]
        container.execute_item_batch(ops, partition_key=pk)

    opts = key_options(
        cfg,
        account_key,
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
    as_dict = lambda row: dict(zip(names, row, strict=True))  # noqa: E731
    limit = h.planner.getDefaultReadLimit()

    # AvailableNow run 1: target, P0, batch 0 (the snapshot) under concurrent writes.
    h.planner.prepareForTriggerAvailableNow()
    start = h.start()
    end = h.latest(limit)
    p0, e0 = Offset.from_dict(json.loads(start)), Offset.from_dict(json.loads(end))
    assert e0.ranges == p0.ranges and e0.init == p0.ranges and e0.snapshot == p0.snapshot
    tokens = dict(e0.snapshot.session)
    assert tokens, "Session consistency: the start probe must return a session token"
    for r in p0.ranges:  # verified relation: the session token's global LSN is the probe's etag LSN
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
    snapshot_rows: list[tuple] = []
    sdk_base.set_session_token_header = spy
    try:
        h.offset_log.append((start, end))  # Spark logs the batch before running it
        parts = h.planner.partitions(json.loads(start), json.loads(end))
        assert len(parts) == 4 and all(isinstance(p, CosmosSnapshotPartition) for p in parts)
        thread.start()
        for part in parts:
            # A fresh client per task, as in a separate executor process: its session container is empty,
            # so a session token on the wire can only come from the partition (recorded in batch 0's end).
            client_module.close_all()
            executor = cloudpickle.loads(h._executor_blob)
            for i, row in enumerate(executor.read(pickle.loads(pickle.dumps(part)))):
                snapshot_rows.append(row)
                seen.append((row[1], row[0]))
                if i % 50 == 49:
                    time.sleep(0.3)  # let the writer interleave with the page reads
    finally:
        stop.set()
        thread.join(timeout=60)
        sdk_base.set_session_token_header = original
    validate_rows(snapshot_rows, h.schema, h.ds)
    h.planner.commit(json.loads(end))
    h.committed = end
    h.batches.append(snapshot_rows)
    rows = [as_dict(r) for r in snapshot_rows]
    assert {r["operation"] for r in rows} == {"snapshot"} and {r["lsn"] for r in rows} == {0}
    assert sent and all(token == tokens["-FF"] for _path, token in sent), "every snapshot query is pinned"
    rows += [as_dict(r) for r in h.run_until_idle(limit)]  # run 1 ends at its target (captured before P0)

    # Later AvailableNow runs drain the feed (repeated only if a probe lagged behind the last write).
    last_write = max(write_lsns)
    for _ in range(5):
        time.sleep(1)
        h.restart()
        h.planner.prepareForTriggerAvailableNow()
        rows += [as_dict(r) for r in h.run_until_idle(limit)]
        if min(json.loads(h.committed)["ranges"].values()) >= last_write:
            break
    assert min(json.loads(h.committed)["ranges"].values()) >= last_write

    merged = _merge_latest(rows)
    actual = {
        (d["pk"], d["id"]): d["_ts"]
        for d in container.query_items("SELECT c.id, c.pk, c._ts FROM c", enable_cross_partition_query=True)
    }
    assert merged == actual

    # The race was exercised: changes overtook rows already read by the snapshot.
    snap = {(r["pk"], r["id"]): r["_ts"] for r in rows if r["operation"] == "snapshot"}
    changes = [r for r in rows if r["operation"] != "snapshot"]
    assert any((r["pk"], r["id"]) in snap and r["_ts"] > snap[(r["pk"], r["id"])] for r in changes)
    assert any(r["operation"] == "delete" and (r["pk"], r["id"]) in snap for r in changes)
    assert len(snapshot_rows) >= seeded and counts["delete"] and counts["update"] and counts["insert"]

    # Enforcement: a session token ahead of the service is refused (404/1002), not silently ignored.
    pk_range, _, vector = tokens["-FF"].partition(":")
    version, global_lsn = vector.split("#")[:2]
    future = f"{pk_range}:{version}#{int(global_lsn) + 10**7}"
    with pytest.raises(CosmosHttpResponseError) as info:
        list(container.query_items("SELECT * FROM c", feed_range=feed_range_dict("", "FF"), session_token=future))
    assert (info.value.status_code, info.value.sub_status) == (404, 1002)
    print(
        f"initial snapshot live test: snapshot rows={len(snapshot_rows)} change rows={len(changes)} "
        f"writes={counts} batches={len(h.batches)} pinned queries={len(sent)} token={tokens['-FF']} "
        f"merged keys={len(merged)}"
    )
