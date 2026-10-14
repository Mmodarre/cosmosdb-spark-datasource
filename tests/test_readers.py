"""Batch and streaming readers end to end against the fake backend, via the PySpark call harness."""

from __future__ import annotations

import collections
import json
import logging
import time
from datetime import datetime, timezone

import pytest

try:  # PySpark >= 4.2 only
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows
except ImportError:  # pragma: no cover - exercised on PySpark 4.0/4.1
    ReadAllAvailable = ReadMaxRows = None
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from cosmosdb_datasource import _base
from cosmosdb_datasource.errors import CosmosConfigError, CosmosOffsetError
from cosmosdb_datasource.offsets import Offset, RangeState
from tests.conftest import fake_options
from tests.harness import StreamHarness, make_source, run_batch


def load(store, n: int, start: int = 0, keys: int = 41, **fields) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i, **fields})


@pytest.mark.parametrize(("ranges", "factor"), [(1, 1), (1, 5), (3, 1), (4, 4), (7, 3)])
def test_batch_reads_every_document_once(make_store, ranges: int, factor: int) -> None:
    store = make_store(partitions=ranges)
    load(store, 700)
    rows, parts = run_batch(fake_options(store, partitions_per_feed_range=factor, page_size=37))
    assert len(parts) == ranges * factor
    ids = [r[0] for r in rows]
    assert len(ids) == 700 and len(set(ids)) == 700
    assert {r[1] for r in rows} == {f"p{i}" for i in range(41)}
    assert all(json.loads(r[4])["id"] == r[0] for r in rows)


def test_batch_hash_v1_container(make_store) -> None:
    store = make_store(partitions=2, version=1)
    load(store, 300)
    rows, parts = run_batch(fake_options(store, partitions_per_feed_range=6))
    assert len(parts) == 12 and sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(300))
    sizes = collections.Counter(p.index for p in parts)
    assert len(sizes) == 12


def test_auto_reads_each_physical_partition_as_one_task(make_store, caplog) -> None:
    store = make_store(partitions=3)
    load(store, 400, payload="x" * 5000)
    rows, parts = run_batch(fake_options(store, partitions_per_feed_range="auto"))
    assert len(parts) == 3 and len({r[0] for r in rows}) == 400


def test_sub_splitting_warns_about_the_ru_cost_once(make_store, caplog, monkeypatch) -> None:
    monkeypatch.setattr(_base, "_warned_sub_split", False)
    store = make_store(partitions=1)
    load(store, 20)
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        run_batch(fake_options(store, partitions_per_feed_range=4))
        run_batch(fake_options(store, partitions_per_feed_range=4))
    warnings = [r for r in caplog.records if "times the RU" in r.getMessage()]
    assert len(warnings) == 1 and "about 4 times" in warnings[0].getMessage()


def test_batch_empty_container(make_store) -> None:
    store = make_store(partitions=3)
    rows, parts = run_batch(fake_options(store))
    assert rows == [] and len(parts) == 3


def test_batch_retries_throttling_without_duplicates(make_store, no_sleep) -> None:
    store = make_store(partitions=1)
    load(store, 120)
    store.throttle_next = 3
    rows, _ = run_batch(fake_options(store, page_size=25))
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(120))
    assert len(no_sleep) == 3


def test_batch_user_schema_and_hierarchical_keys(make_store) -> None:
    store = make_store(partitions=2, pk_paths=("/tenant", "/user"), kind="MultiHash")
    for i in range(50):
        store.upsert({"id": f"d{i}", "tenant": f"t{i % 3}", "user": i, "age": str(20 + i), "name": f"n{i}"})
    store.upsert({"id": "bad", "tenant": "t0", "user": 999, "age": "old"})
    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("pk", StringType()),
            StructField("age", IntegerType()),
            StructField("Name", StringType()),
            StructField("feed_range", StringType()),
            StructField("_corrupt_record", StringType()),
        ]
    )
    rows, _ = run_batch(fake_options(store, partitions_per_feed_range=2), schema)
    by_id = {r[0]: r for r in rows}
    assert by_id["d4"][1:4] == ('["t1",4]', 24, "n4")
    assert by_id["bad"][2] is None and json.loads(by_id["bad"][5])["age"] == "old"
    assert all(r[4] for r in rows)


def test_batch_partitions_carry_redacted_tokens(make_store) -> None:
    store = make_store()
    load(store, 5)
    opts = fake_options(
        store,
        auth_type="access_token",
        access_token="opaque-secret-token",
        access_token_expires_on=str(int(time.time()) + 3600),
    )
    opts.pop("account_key")
    rows, parts = run_batch(opts)
    assert len(rows) == 5
    assert parts[0].token.value == "opaque-secret-token"
    assert "opaque-secret-token" not in repr(parts[0])


def test_mode_selection_errors(make_store) -> None:
    store = make_store()
    ds = make_source(fake_options(store, change_feed_mode="LatestVersion", initial_snapshot="true"))
    with pytest.raises(CosmosConfigError, match=r"only apply to spark\.readStream"):
        ds.reader(ds.schema())
    batch_ds = make_source(fake_options(store))
    with pytest.raises(CosmosConfigError, match="set option change_feed_mode"):
        batch_ds.streamReader(batch_ds.schema())
    custom = StructType([StructField("id", StringType()), StructField("lsn", StringType())])
    assert batch_ds.streamReader(custom) is not None  # explicit schema: LatestVersion default
    with pytest.raises(CosmosConfigError, match="not supported for streaming"):
        q = make_source(fake_options(store, change_feed_mode="LatestVersion", query="SELECT * FROM c"))
        q.streamReader(q.schema())


def stream(store, **extra) -> StreamHarness:
    extra.setdefault("change_feed_mode", "LatestVersion")
    return StreamHarness(fake_options(store, **extra), admission_control=extra.pop("_ac", False))


def ids(h: StreamHarness, rows) -> list[str]:
    return sorted(h.column(rows, "id"))


def test_stream_from_beginning_then_incremental(make_store) -> None:
    store = make_store(partitions=3)
    load(store, 300)
    h = stream(store, page_size=40)
    h.start()
    first = h.run_batch()
    assert ids(h, first) == sorted(f"d{i}" for i in range(300))
    assert set(h.column(first, "operation")) == {"upsert"}
    assert h.run_batch() is None  # unchanged offset JSON -> Spark runs no batch
    load(store, 5, start=300)
    load(store, 3, start=0)  # updates
    second = h.run_batch()
    assert ids(h, second) == sorted(["d0", "d1", "d2", *[f"d{i}" for i in range(300, 305)]])
    lsns = h.column(second, "lsn")
    assert all(isinstance(x, int) for x in lsns)


def test_replaying_a_batch_is_deterministic(make_store) -> None:
    store = make_store(partitions=2, name="replay")
    load(store, 100)
    h = stream(store, change_feed_mode="AllVersionsAndDeletes", page_size=7)
    h.start()
    load(store, 50, start=100)
    for i in range(10):
        store.delete(f"p{i % 41}", f"d{i}")
    first = h.run_batch()
    load(store, 20, start=0)  # newer versions of already-captured documents
    store.delete("p20", "d20")
    replay = h.read_range(*h.offset_log[0])
    assert replay == first  # AVAD: identical rows, identical order
    second = h.run_batch()
    # (range, LSN) pairs never repeat across batches (LSNs are per physical partition)
    fr = h.schema.fieldNames().index("feed_range")
    assert {(r[fr], r[4]) for r in second}.isdisjoint({(r[fr], r[4]) for r in first})
    assert len(second) == 21


def test_latest_version_replay_semantics(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 20)
    h = stream(store)
    h.start()
    first = h.run_batch()
    load(store, 1, start=5)  # d5 updated after the batch was planned
    replay = h.read_range(*h.offset_log[0])
    # LatestVersion keeps only the newest version: the replay omits d5 (its newest LSN is beyond the
    # batch end) and the next batch delivers d5's newest version. Nothing else changes.
    assert ids(h, replay) == sorted(set(ids(h, first)) - {"d5"})
    assert ids(h, h.run_batch()) == ["d5"]


def test_restart_continues_without_gaps_or_duplicates(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 80)
    h = stream(store, page_size=9)
    h.start()
    seen = h.run_until_idle()
    load(store, 30, start=80)
    h.restart()
    seen += h.run_until_idle()
    load(store, 10, start=110)
    h.restart()
    seen += h.run_until_idle()
    assert ids(h, seen) == sorted(f"d{i}" for i in range(120))


def test_restart_replays_planned_but_uncommitted_batch(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 40)
    h = stream(store)
    start = h.start()
    end = h.latest()
    planned = h.read_range(start, end)  # batch ran, query crashed before commit
    load(store, 10, start=40)
    h.restart()
    again = h.read_range(start, end)  # Spark re-executes the logged batch
    assert again == planned
    h.planner.commit(json.loads(end))
    h.committed = end
    assert ids(h, h.run_until_idle()) == sorted(f"d{i}" for i in range(40, 50))


def test_split_between_batches_rekeys_offsets(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 100)
    h = stream(store)
    h.start()
    h.run_batch()
    children = store.split("0")
    load(store, 60, start=100)
    store.write_child_lsn_bump(children[1].id, 7)  # children progress independently
    rows = h.run_until_idle()
    assert ids(h, rows) == sorted(f"d{i}" for i in range(100, 160))
    keys = json.loads(h.committed)["ranges"]
    assert len(keys) == 3
    assert {f"{c.lo}-{c.hi}" for c in children} <= set(keys)


def test_split_after_planning_before_read(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 60)
    h = stream(store, page_size=5)
    start = h.start()
    load(store, 40, start=60)
    end = h.latest()
    store.split("0")  # the planned range now spans two physical partitions
    load(store, 25, start=100)  # beyond the planned end
    rows = h.read_range(start, end)
    assert ids(h, rows) == sorted(f"d{i}" for i in range(100))


def test_replica_lag_is_waited_out(make_store, no_sleep) -> None:
    store = make_store(partitions=1)
    load(store, 30)
    h = stream(store, page_size=10)
    start = h.start()
    load(store, 10, start=30)
    end = h.latest()
    store.lag_requests, store.lag_delta = 5, 8  # outlasts the pages below the stale LSN
    rows = h.read_range(start, end)
    assert ids(h, rows) == sorted(f"d{i}" for i in range(40))
    assert no_sleep  # waited at least once


def test_avad_deletes_and_ttl(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 10)  # before the stream starts: not visible with start_from=Now
    h = stream(store, change_feed_mode="AllVersionsAndDeletes")
    h.start()
    store.upsert({"id": "a", "pk": "x", "v": 1})
    store.upsert({"id": "a", "pk": "x", "v": 2})
    store.delete("x", "a")
    store.delete("p3", "d3", ttl_expired=True)
    rows = h.run_batch()
    events = sorted((r[0], r[2], r[4]) for r in rows)
    ops = [(e[0], e[1]) for e in sorted(events, key=lambda e: e[2]) if e[0] == "a"]
    assert ops == [("a", "create"), ("a", "replace"), ("a", "delete")]
    deletes = [r for r in rows if r[2] == "delete"]
    assert all(r[h.schema.fieldNames().index("body")] is None for r in deletes)
    col = {name: i for i, name in enumerate(h.schema.fieldNames())}
    assert {(r[0], r[1], r[col["ttl_expired"]]) for r in deletes} == {("a", "x", False), ("d3", "p3", True)}
    assert all(r[col["_ts"]] == r[col["crts"]] for r in deletes)  # _ts falls back to crts for deletes


def test_avad_rejects_beginning() -> None:
    from cosmosdb_datasource.options import parse_options

    with pytest.raises(CosmosConfigError):
        parse_options(
            {
                "endpoint": "https://a.documents.azure.com/",
                "database": "d",
                "container": "c",
                "account_key": "k",
                "change_feed_mode": "AllVersionsAndDeletes",
                "start_from": "Beginning",
            }
        )


def test_start_from_now_and_timestamp(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 20)
    now = stream(store, start_from="Now")
    now.start()
    assert now.run_batch() == []  # batch 0 always runs for a new query (empty here)
    assert now.run_batch() is None
    store.clock = int(datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp())
    load(store, 5, start=20)
    assert ids(now, now.run_batch()) == sorted(f"d{i}" for i in range(20, 25))
    ts = stream(store, start_from="2030-01-01T00:00:00Z")
    ts.start()
    assert ids(ts, ts.run_until_idle()) == sorted(f"d{i}" for i in range(20, 25))


@pytest.mark.parametrize("admission_control", [False, True])
def test_start_from_in_the_future_reads_changes_from_that_time_on(make_store, admission_control) -> None:
    """The service has no position for a future time: the stream starts at "now", drops earlier changes."""
    if admission_control and ReadMaxRows is None:
        pytest.skip("admission control API needs PySpark >= 4.2")
    store = make_store(partitions=2)
    load(store, 10)
    t = store.clock + 100
    start_from = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    h = StreamHarness(fake_options(store, start_from=start_from), admission_control=admission_control)
    h.start()
    assert not h.run_batch()
    load(store, 5, start=10)  # before start_from
    assert not h.run_until_idle()
    store.clock = t - 1
    load(store, 3, start=20)  # from start_from on
    load(store, 1, start=1)  # a document changed again after start_from is delivered in its new version
    rows = h.run_until_idle()
    assert ids(h, rows) == ["d1", "d20", "d21", "d22"]
    assert all(json.loads(e)["not_before"] == t for _, e in h.offset_log)
    assert h.read_range(*h.offset_log[-1]) == rows  # a replay applies the same bound
    h.restart()
    load(store, 2, start=30)
    assert ids(h, h.run_until_idle()) == ["d30", "d31"]


def test_offset_not_before_round_trips_and_is_validated() -> None:
    off = Offset("rid", "LatestVersion", (RangeState("", "FF", 3),), not_before=1_800_000_000)
    assert Offset.from_dict(off.to_dict()) == off and off.to_dict()["not_before"] == 1_800_000_000
    assert "not_before" not in Offset("rid", "LatestVersion", (RangeState("", "FF", 3),)).to_dict()
    with pytest.raises(CosmosOffsetError, match="not_before"):
        Offset.from_dict({**off.to_dict(), "not_before": "soon"})


def test_max_items_per_trigger_without_admission_control(make_store) -> None:
    store = make_store(partitions=3)
    load(store, 300)
    h = stream(store, max_items_per_trigger=50)
    h.start()
    rows = h.run_until_idle()
    assert ids(h, rows) == sorted(f"d{i}" for i in range(300))
    assert len(h.batches) >= 6
    assert all(len(b) <= 50 + 3 for b in h.batches)


needs_42 = pytest.mark.skipif(ReadMaxRows is None, reason="admission control API needs PySpark >= 4.2")


@needs_42
def test_admission_control_api(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 200)
    h = stream(store, max_items_per_trigger=60, _ac=True)
    h.start()
    assert h.planner.getDefaultReadLimit() == ReadMaxRows(60)
    rows = h.run_until_idle(limit=ReadMaxRows(60))
    assert len(rows) == 200 and all(len(b) <= 62 for b in h.batches)
    load(store, 30, start=200)
    assert len(h.run_batch(limit=ReadAllAvailable())) == 30


@needs_42
def test_trigger_available_now_target(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 100)
    h = stream(store, max_items_per_trigger=30, _ac=True)
    h.start()
    h.planner.prepareForTriggerAvailableNow()
    load(store, 50, start=100)  # arrives after the AvailableNow target was captured
    rows = h.run_until_idle(limit=ReadMaxRows(30))
    assert ids(h, rows) == sorted(f"d{i}" for i in range(100))
    assert len(h.batches) > 1


def test_stream_sub_splitting_matches_unsplit(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 150)
    plain, split = stream(store), stream(store, partitions_per_feed_range=4)
    plain.start(), split.start()
    assert sorted(plain.run_batch()) == sorted(split.run_batch())
    load(store, 40, start=150)
    end = split.latest()
    assert len(split.planner.partitions(json.loads(split.committed), json.loads(end))) == 8


def test_transactional_batch_same_lsn_stays_together(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 3)
    h = stream(store)
    h.start()
    h.run_batch()
    store.upsert({"id": "t1", "pk": "same", "v": 1})
    store.upsert({"id": "t2", "pk": "same", "v": 2}, same_lsn_as_previous=True)
    rows = h.run_batch()
    assert ids(h, rows) == ["t1", "t2"] and len(set(h.column(rows, "lsn"))) == 1


def test_lsn_gaps_without_visible_changes(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 10)
    h = stream(store)
    h.start()
    h.run_batch()
    store.bump_lsn(5)
    assert h.run_batch() == []  # offsets advance, nothing to emit
    assert h.run_batch() is None


def test_checkpoint_incompatibilities(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 5)
    h = stream(store)
    h.start()
    h.run_batch()
    avad = stream(store, change_feed_mode="AllVersionsAndDeletes")
    avad.committed = h.committed
    avad.planner.partitions(json.loads(h.committed), json.loads(h.committed))
    with pytest.raises(CosmosOffsetError, match="switch modes"):
        avad.latest()
    store.rid = "recreated=="
    with pytest.raises(CosmosOffsetError, match="re-created"):
        h.latest()


def test_fresh_query_call_order_of_pyspark_40(make_store) -> None:
    """PySpark 4.0/4.1: latestOffset() precedes initialOffset() on a new query; both must agree."""
    store = make_store(partitions=2)
    load(store, 30)
    h = stream(store, start_from="Now")
    end = json.dumps(h.planner.latestOffset())  # first trigger: no start known yet
    load(store, 4, start=30)  # arrives between latestOffset() and Spark's initialOffset() call
    start = json.dumps(h.planner.initialOffset())
    # cached: "Now" is resolved once, so start and end describe the same position
    assert Offset.from_dict(json.loads(start)).same_position(Offset.from_dict(json.loads(end)))
    assert h.read_range(start, end) == []
    h.planner.commit(json.loads(end))
    h.committed = end
    assert ids(h, h.run_batch()) == [f"d{i}" for i in range(30, 34)]
    beginning = stream(store)
    first_end = json.dumps(beginning.planner.latestOffset())
    first_start = json.dumps(beginning.planner.initialOffset())
    assert len(beginning.read_range(first_start, first_end)) == 34


def test_stream_schema_is_validated_through_arrow(make_store) -> None:
    store = make_store(partitions=2, pk_paths=("/tenant", "/user"), kind="MultiHash")
    for i in range(20):
        store.upsert({"id": f"d{i}", "tenant": "t", "user": i})
    h = stream(store)
    h.start()
    rows = h.run_batch()
    assert {r[1] for r in rows} == {f'["t",{i}]' for i in range(20)}
    assert all(r[h.schema.fieldNames().index("feed_range")] for r in rows)


def test_file_backed_fake_used_by_local_spark_tests(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tests import fake_cosmos

    monkeypatch.setenv("FAKE_COSMOS_DIR", str(tmp_path))
    store = fake_cosmos.FakeCosmosStore("filebacked", partitions=2).register()
    load(store, 50)
    fake_cosmos._REGISTRY.pop("filebacked")  # force the cross-process (JSON file) path
    rows, _ = run_batch(fake_options(store))
    assert len(rows) == 50
    h = stream(store)
    h.start()
    assert len(h.run_batch()) == 50
    load(store, 5, start=50)
    assert ids(h, h.run_batch()) == [f"d{i}" for i in range(50, 55)]


def test_close_all_supports_clients_without_close() -> None:
    from cosmosdb_datasource import client as client_module

    calls: list[str] = []

    class OldClient:  # azure-cosmos 4.14 has no close()
        def __exit__(self, *args):
            calls.append("exit")

    class NewClient:
        def close(self):
            calls.append("close")

    client_module._CLIENTS[("old",)] = client_module._Entry(OldClient(), None)
    client_module._CLIENTS[("new",)] = client_module._Entry(NewClient(), None)
    client_module.close_all()
    assert sorted(calls) == ["close", "exit"] and not client_module._CLIENTS


def test_split_during_sub_split_read_has_no_duplicates(make_store) -> None:
    # The SDK widens a sub-range token to the full child partitions after a split; the reader
    # must re-scope instead of re-reading the neighbouring sub-ranges.
    store = make_store(partitions=1)
    load(store, 200)
    h = stream(store, partitions_per_feed_range=3, page_size=15)  # a piece straddles the split point
    start = h.start()
    load(store, 100, start=200)
    end = h.latest()
    store.split("0")
    rows = h.read_range(start, end)
    got = h.column(rows, "id")
    assert len(got) == len(set(got)) == 300
    assert sorted(got) == sorted(f"d{i}" for i in range(300))


@pytest.mark.parametrize("admission_control", [False, True])
def test_restart_before_first_commit_replays_batch_zero_exactly(make_store, admission_control: bool) -> None:
    store = make_store(partitions=2)
    load(store, 10)
    h = StreamHarness(
        fake_options(store, change_feed_mode="AllVersionsAndDeletes"), admission_control=admission_control
    )
    if admission_control:
        start = h.committed = json.dumps(h.planner.initialOffset())
        load(store, 7, start=10)
        end = h.latest()
    else:
        end = json.dumps(h.planner.latestOffset())  # PySpark 4.0/4.1 order for a new query
        start = json.dumps(h.planner.initialOffset())
        load(store, 7, start=10)
        end = json.dumps(h.planner.latestOffset())
    first_attempt = h.read_range(start, end)
    # crash before commit; changes keep arriving; the new process re-resolves "Now"
    load(store, 5, start=17)
    h.committed = end
    h.restart()
    replay_start = json.dumps(h.planner.initialOffset())
    assert not Offset.from_dict(json.loads(replay_start)).same_position(Offset.from_dict(json.loads(start)))
    assert h.read_range(replay_start, end) == first_attempt
    h.planner.commit(json.loads(end))
    assert ids(h, h.run_until_idle()) == sorted(f"d{i}" for i in range(17, 22))


def test_mode_change_detected_when_replaying(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 5)
    lv = stream(store)
    start = lv.start()
    end = lv.latest()
    avad = stream(store, change_feed_mode="AllVersionsAndDeletes")
    with pytest.raises(CosmosOffsetError, match="switch modes"):
        avad.read_range(start, end)
