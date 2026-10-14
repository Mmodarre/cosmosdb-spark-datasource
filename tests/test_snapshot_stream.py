"""``snapshot_stream=true`` end to end against the fake service, via the PySpark call harness.

Both PySpark protocol paths are replayed: ``admission_control=False`` is PySpark 4.0/4.1 (``latestOffset()``
without arguments, ``partitions(offset, offset)`` on restart), ``True`` is 4.2+ / Databricks
(``latestOffset(start, limit)``, ``prepareForTriggerAvailableNow``). The Databricks engine itself (engine
PySpark driving a client-4.0 reader) is replayed in a subprocess at the end.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

try:  # PySpark >= 4.2 only
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows
except ImportError:  # pragma: no cover - exercised on PySpark 4.0/4.1
    ReadAllAvailable = ReadMaxRows = None
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from cosmosdb_datasource import snapshot_stream as ss
from cosmosdb_datasource.errors import CosmosConfigError, CosmosOffsetError
from cosmosdb_datasource.offsets import Offset, SnapshotStreamOffset
from cosmosdb_datasource.options import SNAPSHOT_STREAM_EXCLUSIVE_OPTIONS, TEST_HOOKS_ENV
from cosmosdb_datasource.schema import batch_schema, snapshot_stream_schema
from cosmosdb_datasource.snapshot_stream import CosmosSnapshotStreamPartition, CosmosSnapshotStreamReader
from tests.conftest import fake_options
from tests.harness import StreamHarness, make_source, run_batch
from tests.test_databricks_engine_compat import _ENGINE_STREAMING_SOURCE, ROOT

needs_42 = pytest.mark.skipif(ReadMaxRows is None, reason="admission control API needs PySpark >= 4.2")
both_paths = pytest.mark.parametrize("ac", [False, pytest.param(True, marks=needs_42)])
T0 = datetime(2026, 10, 15, 1, 0, 0, 123456, tzinfo=timezone.utc)


def load(store, n: int, start: int = 0, keys: int = 41) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i})


def sstream(store, *, ac: bool = False, schema: StructType | None = None, **extra) -> StreamHarness:
    extra.setdefault("snapshot_stream", "true")
    return StreamHarness(fake_options(store, **extra), schema, admission_control=ac)


def col(h: StreamHarness, rows, name: str) -> list:
    return h.column(rows, name)


def live(store) -> list[str]:
    return sorted(d["id"] for d in store.live_documents())


def end_offset(h: StreamHarness) -> SnapshotStreamOffset:
    assert h.committed is not None
    return SnapshotStreamOffset.from_dict(json.loads(h.committed))


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    now = [T0]
    monkeypatch.setattr(ss, "utcnow", lambda: now[0])
    return now


def test_default_schema_is_the_batch_schema_plus_snapshot_ts(make_store) -> None:
    store = make_store()
    ds = make_source(fake_options(store, snapshot_stream="true"))
    schema = ds.schema()
    assert schema == snapshot_stream_schema()
    assert schema.fieldNames() == [*batch_schema().fieldNames(), "snapshot_ts"]
    assert schema["snapshot_ts"].dataType == TimestampType()
    assert isinstance(ds.streamReader(schema), CosmosSnapshotStreamReader)


@pytest.mark.parametrize("name", SNAPSHOT_STREAM_EXCLUSIVE_OPTIONS)
def test_change_feed_options_are_rejected(make_store, name: str) -> None:
    value = {"change_feed_mode": "LatestVersion", "start_from": "Now", "initial_snapshot": "true"}.get(name, "5")
    with pytest.raises(CosmosConfigError, match=name):
        make_source(fake_options(make_store(), snapshot_stream="true", **{name: value}))


def test_interval_needs_snapshot_stream_and_is_not_negative(make_store) -> None:
    store = make_store()
    with pytest.raises(CosmosConfigError, match="only applies with snapshot_stream=true"):
        make_source(fake_options(store, snapshot_interval_seconds="60"))
    with pytest.raises(CosmosConfigError, match="snapshot_interval_seconds"):
        make_source(fake_options(store, snapshot_stream="true", snapshot_interval_seconds="-1"))


def test_batch_read_with_snapshot_stream_fails(make_store) -> None:
    with pytest.raises(CosmosConfigError, match=r"only apply to spark\.readStream"):
        run_batch(fake_options(make_store(), snapshot_stream="true"), batch_schema())


def test_snapshot_stream_false_is_a_normal_batch_read(make_store) -> None:
    store = make_store()
    load(store, 3)
    rows, _ = run_batch(fake_options(store, snapshot_stream="false"))
    assert len(rows) == 3


@both_paths
def test_offsets_shape(make_store, clock, ac: bool) -> None:
    store = make_store()
    load(store, 5)
    h = sstream(store, ac=ac)
    assert h.start() == '{"v": 1, "kind": "snapshot", "seq": 0}'
    h.run_batch()
    raw = json.loads(h.committed)
    assert list(raw) == ["v", "kind", "seq", "snapshot_ts", "rid"]
    assert raw == {"v": 1, "kind": "snapshot", "seq": 1, "snapshot_ts": "2026-10-15T01:00:00.123000Z", "rid": store.rid}


def test_offset_validation() -> None:
    for bad in (
        {"v": 2, "kind": "snapshot", "seq": 0},
        {"v": 1, "kind": "other", "seq": 0},
        {"v": 1, "kind": "snapshot", "seq": -1},
        {"v": 1, "kind": "snapshot", "seq": True},
        {"v": 1, "kind": "snapshot", "seq": 1},  # needs snapshot_ts and rid
        {"v": 1, "kind": "snapshot", "seq": 1, "snapshot_ts": "2026-10-15T01:00:00Z", "rid": "r"},  # not 6 digits
        {"v": 1, "kind": "snapshot", "seq": 1, "snapshot_ts": "2026-10-15T01:00:00.000000Z", "rid": 5},
    ):
        with pytest.raises(CosmosOffsetError):
            SnapshotStreamOffset.from_dict(bad)


def test_checkpoints_of_the_other_mode_are_rejected(make_store, clock) -> None:
    store = make_store()
    load(store, 3)
    cf = StreamHarness(fake_options(store, change_feed_mode="LatestVersion"))
    cf.start()
    cf.run_batch()
    with pytest.raises(CosmosOffsetError, match="change-feed stream"):
        SnapshotStreamOffset.from_dict(json.loads(cf.committed))
    h = sstream(store)
    h.start()
    h.run_batch()
    with pytest.raises(CosmosOffsetError, match="snapshot_stream read"):
        Offset.from_dict(json.loads(h.committed))
    with pytest.raises(CosmosOffsetError, match="change-feed stream"):
        h.planner.latestOffset(json.loads(cf.committed), None)


@both_paths
def test_every_batch_is_one_complete_snapshot_stamped_with_its_end_offset(make_store, clock, ac: bool) -> None:
    store = make_store(partitions=3)
    load(store, 60)
    h = sstream(store, ac=ac)
    h.start()
    first = h.run_batch()
    one = end_offset(h)
    assert sorted(col(h, first, "id")) == live(store) and len(first) == 60
    assert set(col(h, first, "snapshot_ts")) == {one.snapshot_datetime}
    # mutate: updates, deletes, inserts
    for i in range(5):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 41}", "v": -i})
    for i in range(10, 13):
        store.delete(f"p{i % 41}", f"d{i}")
    load(store, 2, start=100)
    clock[0] += timedelta(hours=1)
    second = h.run_batch()
    two = end_offset(h)
    assert sorted(col(h, second, "id")) == live(store) and len(second) == 59
    assert set(col(h, second, "snapshot_ts")) == {two.snapshot_datetime} and two.seq == one.seq + 1
    assert two.snapshot_datetime == (T0 + timedelta(hours=1)).replace(microsecond=123000) > one.snapshot_datetime
    bodies = {i: json.loads(b) for i, b in zip(col(h, second, "id"), col(h, second, "body"), strict=True)}
    assert bodies["d0"]["v"] == 0 and bodies["d4"]["v"] == -4 and "d10" not in bodies


def test_empty_container_advances_the_offset_without_rows(make_store, clock) -> None:
    store = make_store()
    h = sstream(store)
    h.start()
    assert h.run_batch() == [] and end_offset(h).seq == 1


def test_user_schema_with_and_without_snapshot_ts(make_store, clock, caplog) -> None:
    store = make_store()
    load(store, 4)
    schema = StructType([StructField("id", StringType()), StructField("snapshot_ts", StringType())])
    h = sstream(store, schema=schema)
    h.start()
    rows = h.run_batch()
    assert set(col(h, rows, "snapshot_ts")) == {"2026-10-15T01:00:00.123000Z"}
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        sstream(store, schema=StructType([StructField("id", StringType())]))
    assert any("no 'snapshot_ts' column" in r.getMessage() for r in caplog.records)


@needs_42
def test_one_snapshot_per_available_now_run(make_store, clock) -> None:
    store = make_store(partitions=2)
    load(store, 30)
    h = sstream(store, ac=True)
    assert h.planner.getDefaultReadLimit() == ReadAllAvailable()
    for run in range(1, 4):
        if run > 1:
            h.restart()
            load(store, 1, start=100 + run)
            clock[0] += timedelta(minutes=5)
        h.planner.prepareForTriggerAvailableNow()
        if run == 1:
            h.start()
        rows = h.run_until_idle(ReadMaxRows(3))  # limits are ignored: a snapshot is never split
        assert len(rows) == len(live(store)) and end_offset(h).seq == run
    assert len(h.batches) == 3


@needs_42
def test_available_now_plans_nothing_while_the_interval_has_not_passed(make_store, clock) -> None:
    store = make_store()
    load(store, 3)
    h = sstream(store, ac=True, snapshot_interval_seconds="3600")
    h.planner.prepareForTriggerAvailableNow()
    h.start()
    assert len(h.run_until_idle(ReadAllAvailable())) == 3
    for minutes, expected in ((30, 0), (61, 1)):
        clock[0] = T0 + timedelta(minutes=minutes)
        h.restart()
        h.planner.prepareForTriggerAvailableNow()
        h.run_until_idle(ReadAllAvailable())
        assert end_offset(h).seq == 1 + expected


@both_paths
def test_interval_gates_new_snapshots(make_store, clock, ac: bool) -> None:
    store = make_store()
    load(store, 3)
    h = sstream(store, ac=ac, snapshot_interval_seconds="3600")
    h.start()
    assert len(h.run_batch()) == 3
    clock[0] += timedelta(seconds=3599)
    assert h.run_batch() is None  # Spark sees the unchanged offset: no new data
    clock[0] += timedelta(seconds=1)
    assert len(h.run_batch()) == 3 and end_offset(h).seq == 2


def test_without_admission_control_every_trigger_plans_a_snapshot(make_store, clock, caplog) -> None:
    store = make_store()
    load(store, 2)
    h = sstream(store)
    h.start()  # PySpark 4.0/4.1: latestOffset() before initialOffset() on a new query
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        for _ in range(3):
            clock[0] += timedelta(seconds=1)
            assert len(h.run_batch()) == 2
    assert end_offset(h).seq == 3
    assert any("two snapshots planned" in r.getMessage() for r in caplog.records)  # continuous full reads
    h.restart()  # setLatestSeenOffset -> partitions(offset, offset)
    clock[0] += timedelta(seconds=1)
    h.run_batch()
    assert end_offset(h).seq == 4


def test_new_query_uses_the_snapshot_planned_by_its_first_latest_offset(make_store, clock) -> None:
    store = make_store()
    h = sstream(store)
    first = h.planner.latestOffset()  # 4.0/4.1: before initialOffset()
    h.committed = json.dumps(h.planner.initialOffset())
    clock[0] += timedelta(minutes=1)
    assert h.latest() == json.dumps(first)


@both_paths
def test_replay_keeps_seq_and_snapshot_ts(make_store, clock, ac: bool) -> None:
    store = make_store(partitions=2)
    load(store, 20)
    h = sstream(store, ac=ac)
    start = h.start()
    end = h.latest()
    h.offset_log.append((start, end))  # Spark logs the offsets, then the batch fails before its commit
    h.read_range(start, end)  # the tasks ran, but the batch never committed
    # restart later: the container changed meanwhile, and the clock moved on
    clock[0] += timedelta(hours=2)
    load(store, 3, start=50)
    store.delete("p0", "d0")
    h.restart()
    if not ac:
        h.planner.partitions(json.loads(end), json.loads(end))  # setLatestSeenOffset with the logged end
    rows = h.read_range(start, end)  # replay of the logged batch
    assert set(col(h, rows, "snapshot_ts")) == {SnapshotStreamOffset.from_dict(json.loads(end)).snapshot_datetime}
    assert sorted(col(h, rows, "id")) == live(store)  # re-read: the content is the container at replay time
    h.planner.commit(json.loads(end))
    h.committed = end
    nxt = json.loads(h.latest())
    assert nxt["seq"] == 2 and nxt["snapshot_ts"] == "2026-10-15T03:00:00.123000Z"


@both_paths
def test_asking_twice_for_the_same_start_plans_one_snapshot(make_store, clock, ac: bool) -> None:
    store = make_store()
    h = sstream(store, ac=ac)
    h.start()
    a = h.latest()
    clock[0] += timedelta(seconds=30)
    assert h.latest() == a


def test_snapshot_ts_strictly_increases_when_the_clock_goes_back(make_store, clock) -> None:
    store = make_store()
    h = sstream(store)
    h.start()
    h.run_batch()
    clock[0] = T0 - timedelta(seconds=5)
    h.run_batch()
    assert end_offset(h).snapshot_ts == "2026-10-15T01:00:00.124000Z"  # previous + 1 ms


@both_paths
def test_recreated_container_fails_until_the_checkpoint_is_reset(make_store, clock, ac: bool) -> None:
    store = make_store()
    load(store, 4)
    h = sstream(store, ac=ac)
    h.start()
    h.run_batch()
    old_end = h.committed
    recreated = make_store(name=store.name)  # same name, new _rid
    load(recreated, 2)
    clock[0] += timedelta(seconds=1)
    with pytest.raises(CosmosOffsetError, match="re-created"):
        h.run_batch()
    fresh_end = SnapshotStreamOffset(2, "2026-10-15T01:00:01.123000Z", store.rid).to_dict()
    with pytest.raises(CosmosOffsetError, match="re-created"):
        h.planner.partitions(json.loads(old_end), fresh_end)  # a batch planned for the old instance
    reset = sstream(recreated, ac=ac)  # new checkpoint
    reset.start()
    assert sorted(col(reset, reset.run_batch(), "id")) == live(recreated)


def test_partitions_follow_partitions_per_feed_range_and_carry_session_tokens(make_store, clock) -> None:
    store = make_store(partitions=2)
    load(store, 40)
    h = sstream(store, partitions_per_feed_range="3")
    start = h.start()
    end = h.latest()
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    assert len(parts) == 6 and all(isinstance(p, CosmosSnapshotStreamPartition) for p in parts)
    assert {p.feed_range for p in parts} == {f"{p.lo}-{p.hi}" for p in store.partitions}
    assert all(p.session_token and p.snapshot_ts == json.loads(end)["snapshot_ts"] for p in parts)
    rows = h.read_range(start, end)
    assert len(rows) == 40
    tokens = [e[3] for e in store.call_log if e[0] == "query_page"]
    assert tokens and all(tokens)


def test_session_token_keeps_a_lagging_replica_from_serving_an_older_state(make_store, no_sleep, monkeypatch) -> None:
    def scenario(pinned: bool) -> tuple[int, int, int]:
        store = make_store(partitions=2)
        load(store, 80)
        h = sstream(store)
        h.start()
        store.query_lag_requests, store.query_lag_delta = 3, 30
        no_sleep.clear()
        with monkeypatch.context() as m:
            if not pinned:
                m.setattr(CosmosSnapshotStreamReader, "_session_tokens", lambda self, backend, ranges: {})
            rows = h.run_batch()
        return len(rows), len(live(store)), len(no_sleep)

    got, expected, retries = scenario(pinned=True)
    assert got == expected and retries >= 2  # 404/1002 until the replica caught up
    got, expected, retries = scenario(pinned=False)
    assert retries == 0 and got < expected


def test_split_between_snapshots_and_during_a_scan(make_store, clock) -> None:
    store = make_store(partitions=1)
    load(store, 60)
    h = sstream(store, page_size=7)
    h.start()
    h.run_batch()
    store.split(store.partitions[0].id)
    clock[0] += timedelta(seconds=1)
    rows = h.run_batch()
    assert sorted(col(h, rows, "id")) == live(store) and len(store.partitions) == 2
    pages = [0]

    def split_once() -> None:
        pages[0] += 1
        if pages[0] == 2:
            store.split(store.partitions[0].id)

    store._before_query_page = split_once
    clock[0] += timedelta(seconds=1)
    rows = h.run_batch()
    store._before_query_page = None
    assert sorted(col(h, rows, "id")) == live(store) and len(rows) == len(set(col(h, rows, "id")))


_SNAPSHOT_SIMULATION = r"""
import dataclasses, importlib.util, inspect, json, pickle, sys

from pyspark.sql import datasource as client_ds
sys.modules["pyspark.sql.streaming.datasource"] = None
for name in ("getDefaultReadLimit", "reportLatestOffset"):
    if name in vars(client_ds.DataSourceStreamReader):
        delattr(client_ds.DataSourceStreamReader, name)

import _engine_pyspark.sql.streaming.datasource as engine
import pyspark.sql.types as client_types
spec = importlib.util.spec_from_file_location("_engine_types", client_types.__file__)
engine_types = importlib.util.module_from_spec(spec); spec.loader.exec_module(engine_types)

from pyspark import cloudpickle
from pyspark.sql.datasource import CaseInsensitiveDict
from cosmosdb_datasource import CosmosDBDataSource
from cosmosdb_datasource.schema import snapshot_stream_schema
from tests.fake_cosmos import FakeCosmosStore
from tests.conftest import fake_options

store = FakeCosmosStore("engine_snap_sim", partitions=3).register()
for i in range(120):
    store.upsert({"id": f"d{i}", "pk": f"p{i % 17}", "v": i})
ds = cloudpickle.loads(cloudpickle.dumps(CosmosDBDataSource(CaseInsensitiveDict(
    fake_options(store, snapshot_stream="true")))))
engine_schema = engine_types._parse_datatype_json_string(snapshot_stream_schema().json())
registry = {c.__name__: c for c in (engine.ReadAllAvailable, engine.ReadMaxRows)}

def run(available_now):
    reader = ds.streamReader(schema=engine_schema)
    executor_blob = cloudpickle.dumps(ds.streamReader(schema=engine_schema))
    out = {"admission_control": len(inspect.signature(reader.latestOffset).parameters) > 0,
           "available_now": isinstance(reader, engine.SupportsTriggerAvailableNow)}
    if available_now:
        reader.prepareForTriggerAvailableNow()
    start = json.dumps(reader.initialOffset() if run.committed is None else json.loads(run.committed))
    batches, stamps, ids = 0, set(), []
    while True:
        limit = reader.getDefaultReadLimit()
        wire = json.loads(json.dumps(dataclasses.asdict(limit) | {"_type": limit.__class__.__name__}))
        limit = registry[wire.pop("_type")](**wire)
        end = json.dumps(reader.latestOffset(json.loads(start), limit))
        json.dumps(reader.reportLatestOffset())
        if end == start or batches == 3:
            break
        for p in reader.partitions(json.loads(start), json.loads(end)):
            executor = cloudpickle.loads(executor_blob)
            for row in executor.read(pickle.loads(pickle.dumps(p))):
                ids.append(row[0]); stamps.add(row[5].isoformat())
        reader.commit(json.loads(end))
        start = end
        batches += 1
    run.committed = start
    reader.stop()
    out.update(batches=batches, rows=len(ids), unique=len(set(ids)), stamps=len(stamps),
               seq=json.loads(start)["seq"], limit=type(limit).__name__)
    return out

run.committed = None
first = run(True)
store.upsert({"id": "late", "pk": "p1", "v": -1})
second = run(True)
print(json.dumps({"first": first, "second": second}))
"""


def test_engine_protocol_one_snapshot_per_available_now_run(tmp_path: Path) -> None:
    pkg = tmp_path / "_engine_pyspark"
    (pkg / "sql" / "streaming").mkdir(parents=True)
    for init in (pkg, pkg / "sql", pkg / "sql" / "streaming"):
        (init / "__init__.py").write_text("")
    (pkg / "sql" / "streaming" / "datasource.py").write_text(textwrap.dedent(_ENGINE_STREAMING_SOURCE))
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(tmp_path), str(ROOT)]), **{TEST_HOOKS_ENV: "1"})
    proc = subprocess.run(
        [sys.executable, "-c", _SNAPSHOT_SIMULATION], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    assert "SupportsTriggerAvailableNow class was found" not in proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    first, second = result["first"], result["second"]
    assert first["admission_control"] is True and first["available_now"] is True
    assert first["limit"] == "ReadAllAvailable"
    assert (first["batches"], first["rows"], first["unique"], first["stamps"], first["seq"]) == (1, 120, 120, 1, 1)
    assert (second["batches"], second["rows"], second["stamps"], second["seq"]) == (1, 121, 1, 2)
