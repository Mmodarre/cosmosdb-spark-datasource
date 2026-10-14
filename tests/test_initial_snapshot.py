"""``initial_snapshot=true`` end to end against the fake service, via the PySpark call harness."""

from __future__ import annotations

import collections
import json
import logging
import random

import pytest

try:  # PySpark >= 4.2 only
    from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxRows
except ImportError:  # pragma: no cover - exercised on PySpark 4.0/4.1
    ReadAllAvailable = ReadMaxRows = None
from pyspark.sql.types import LongType, StringType, StructField, StructType

from cosmosdb_datasource import retry
from cosmosdb_datasource import stream as stream_module
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.offsets import Offset, SnapshotState
from cosmosdb_datasource.schema import change_feed_schema
from cosmosdb_datasource.stream import CosmosChangeFeedPartition, CosmosSnapshotPartition
from tests.conftest import fake_options
from tests.fake_cosmos import FakeBackend, FakeHttpError
from tests.harness import StreamHarness, make_source, run_batch

AVAD = "AllVersionsAndDeletes"
LV = "LatestVersion"
needs_42 = pytest.mark.skipif(ReadMaxRows is None, reason="admission control API needs PySpark >= 4.2")


def load(store, n: int, start: int = 0, keys: int = 41, **fields) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i, **fields})


def snap(store, *, ac: bool = False, **extra) -> StreamHarness:
    extra.setdefault("change_feed_mode", AVAD)
    extra.setdefault("initial_snapshot", "true")
    extra.setdefault("initial_snapshot_delay_seconds", "0")  # the delay has its own test
    return StreamHarness(fake_options(store, **extra), admission_control=ac)


def dicts(h: StreamHarness, rows) -> list[dict]:
    names = h.schema.fieldNames()
    return [dict(zip(names, r, strict=True)) for r in rows]


def ops(h: StreamHarness, rows) -> set[str]:
    return set(h.column(rows, "operation"))


def ids(h: StreamHarness, rows) -> list[str]:
    return sorted(h.column(rows, "id"))


def offset(text: str) -> Offset:
    return Offset.from_dict(json.loads(text))


def live_ids(store) -> list[str]:
    return sorted(d["id"] for d in store.live_documents())


def merge(rows: list[dict], sabotage: str | None = None) -> dict[tuple, dict]:
    """Apply the merge rule documented in the README.

    Latest row per (pk, id) by ``struct(operation <> 'snapshot', _ts, lsn)``; deletes mean absence.
    """
    if sabotage == "drop_snapshot":
        rows = [r for r in rows if r["operation"] != "snapshot"]
    elif sabotage == "drop_changes":
        rows = [r for r in rows if r["operation"] == "snapshot"]

    def rank(r: dict) -> tuple:
        change = r["operation"] != "snapshot"
        if sabotage == "snapshot_wins":
            change = not change
        return (change, r["_ts"], r["lsn"])

    best: dict[tuple, dict] = {}
    for r in rows:
        key = (r["pk"], r["id"])
        if key not in best or rank(r) > rank(best[key]):
            best[key] = r
    out = {}
    for key, r in best.items():
        if r["operation"] != "delete":
            body = json.loads(r["body"])
            body.pop("_lsn", None)  # LatestVersion change-feed items carry _lsn, documents do not
            out[key] = body
    return out


def truth(store) -> dict[tuple, dict]:
    return {(d["pk"], d["id"]): d for d in store.live_documents()}


@pytest.mark.parametrize("mode", [LV, AVAD])
@pytest.mark.parametrize("ac", [False, True])
def test_snapshot_rows_only_in_batch_zero(make_store, mode: str, ac: bool) -> None:
    store = make_store(partitions=3)
    load(store, 120)
    h = snap(store, ac=ac, change_feed_mode=mode, start_from="Now", page_size=13)
    h.start()
    first = h.run_batch()
    assert ops(h, first) == {"snapshot"}
    assert ids(h, first) == live_ids(store)
    load(store, 10, start=120)
    load(store, 5, start=0, updated=True)
    rest = h.run_until_idle()
    assert "snapshot" not in ops(h, rest)
    assert ids(h, rest) == sorted([f"d{i}" for i in range(120, 130)] + [f"d{i}" for i in range(5)])
    h.restart()
    load(store, 3, start=130)
    again = h.run_until_idle()
    assert ids(h, again) == ["d130", "d131", "d132"] and "snapshot" not in ops(h, again)


def test_snapshot_row_schema_and_values(make_store) -> None:
    store = make_store(partitions=2)
    version = store.upsert({"id": "a", "pk": "x", "n": 1, "nested": {"k": [1, 2]}})
    h = snap(store, partitions_per_feed_range=4)
    assert h.schema == change_feed_schema()
    h.start()
    (row,) = dicts(h, h.run_batch())
    owner = store._owner(store._epk("x"))
    assert row == {
        "id": "a",
        "pk": "x",
        "operation": "snapshot",
        "_ts": version.ts,
        "lsn": 0,
        "crts": None,
        "ttl_expired": False,
        "body": row["body"],
        "feed_range": f"{owner.lo}-{owner.hi}",  # the feed range the partition was planned from
    }
    assert json.loads(row["body"]) == version.doc  # the document as stored, system properties included


def test_snapshot_rows_with_user_schema_and_hierarchical_keys(make_store) -> None:
    store = make_store(partitions=2, pk_paths=("/tenant", "/user"), kind="MultiHash")
    for i in range(10):
        store.upsert({"id": f"d{i}", "tenant": "t", "user": i, "age": str(30 + i)})
    schema = StructType(
        [
            StructField("id", StringType()),
            StructField("pk", StringType()),
            StructField("operation", StringType()),
            StructField("lsn", LongType()),
            StructField("age", LongType()),
            StructField("feed_range", StringType()),
        ]
    )
    opts = fake_options(store, change_feed_mode=LV, initial_snapshot="true", initial_snapshot_delay_seconds="0")
    h = StreamHarness(opts, schema=schema)
    h.start()
    rows = {r[0]: r for r in h.run_batch()}
    assert rows["d3"][1:5] == ('["t",3]', "snapshot", 0, 33)
    assert all(r[5] for r in rows.values())


@pytest.mark.parametrize("mode", [LV, AVAD])
def test_batch_zero_runs_and_emits_the_snapshot_without_any_change(make_store, mode: str) -> None:
    store = make_store(partitions=3)
    load(store, 45)
    h = snap(store, change_feed_mode=mode, start_from="Now")
    start = h.start()
    end = h.latest()
    assert offset(end).ranges == offset(start).ranges  # nothing changed since P0
    # Spark runs batch 0 whenever no offset is committed yet; the JSON differs from the start anyway
    assert end != start and h.latest() == end
    rows = h.run_batch()
    assert ops(h, rows) == {"snapshot"} and len(rows) == 45
    assert h.run_batch() is None


def test_batch_zero_of_an_empty_container(make_store) -> None:
    store = make_store(partitions=2)
    h = snap(store)
    h.start()
    assert h.run_batch() == []  # batch 0 still runs (and commits) with an empty snapshot
    store.upsert({"id": "late", "pk": "x"})
    assert [(r["id"], r["operation"]) for r in dicts(h, h.run_batch())] == [("late", "create")]


def test_start_is_resolved_and_logged_before_any_snapshot_page(make_store) -> None:
    store = make_store(partitions=3)
    load(store, 90)
    h = snap(store, page_size=10)
    logged_at_first_page: list[int] = []

    def before_page() -> None:
        if not logged_at_first_page:
            logged_at_first_page.append(len(h.offset_log))

    store._before_query_page = before_page
    h.start()
    rows = h.run_batch()
    assert len(rows) == 90
    kinds = [entry[0] for entry in store.call_log]
    first_page = kinds.index("query_page")
    resolves = [i for i, k in enumerate(kinds) if k == "resolve_start"]
    assert len(resolves) == 3 and max(resolves) < first_page
    assert "resolve_start" not in kinds[first_page:]
    assert logged_at_first_page == [1]  # batch 0's (start, end) was in the offset log already
    start, end = h.offset_log[0]
    assert offset(end).init == offset(start).ranges


def test_pyspark_40_call_order_latest_before_initial(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 25)
    h = snap(store)
    end = json.dumps(h.planner.latestOffset())  # no start known yet: resolves and caches P0
    load(store, 3, start=25)  # lands between latestOffset() and Spark's initialOffset() call
    start = json.dumps(h.planner.initialOffset())
    p0, e0 = offset(start), offset(end)
    assert p0.initial and p0.snapshot is not None
    assert e0.ranges == p0.ranges == e0.init and e0.snapshot == p0.snapshot
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    assert parts and all(isinstance(p, CosmosSnapshotPartition) for p in parts)
    rows = h.read_range(start, end)
    assert ops(h, rows) == {"snapshot"} and ids(h, rows) == sorted(f"d{i}" for i in range(28))
    h.planner.commit(json.loads(end))
    h.committed = end
    rest = dicts(h, h.run_until_idle())
    assert sorted((r["id"], r["operation"]) for r in rest) == [(f"d{i}", "create") for i in (25, 26, 27)]


@pytest.mark.parametrize("ac", [False, True])
def test_restart_before_batch_zero_commits_rereads_snapshot_from_original_start(make_store, ac: bool) -> None:
    store = make_store(partitions=2)
    load(store, 40)
    h = snap(store, ac=ac)
    if ac:  # PySpark 4.2: initialOffset() first
        start = h.committed = json.dumps(h.planner.initialOffset())
        end = h.latest()
    else:  # PySpark 4.0/4.1: latestOffset() first
        end = json.dumps(h.planner.latestOffset())
        start = json.dumps(h.planner.initialOffset())
    p0, e0 = offset(start), offset(end)
    original_tokens = dict(p0.snapshot.session)
    assert e0.init == p0.ranges and e0.ranges == p0.ranges and dict(e0.snapshot.session) == original_tokens
    first_attempt = h.read_range(start, end)
    assert ops(h, first_attempt) == {"snapshot"} and len(first_attempt) == 40

    # The query dies before committing batch 0; changes keep arriving.
    load(store, 5, start=40)
    load(store, 3, start=0, updated=True)
    store.delete("p1", "d1")
    h.committed = end  # E0 is in the offset log, nothing is committed
    h.restart()
    replay_start = json.dumps(h.planner.initialOffset())
    assert not offset(replay_start).same_position(p0)  # "Now" re-resolved to a later position
    store.call_log.clear()
    replay = h.read_range(replay_start, end)
    assert ops(h, replay) == {"snapshot"}
    assert ids(h, replay) == live_ids(store)  # re-read: the current state
    sent = {entry[3] for entry in store.call_log if entry[0] == "query_page"}
    assert sent == set(original_tokens.values())  # the session tokens captured at the ORIGINAL P0

    h.planner.commit(json.loads(end))
    rest = dicts(h, h.run_until_idle())
    assert "snapshot" not in {r["operation"] for r in rest}
    # the change feed starts at the original P0: the changes made between the attempts are delivered
    expected = [(f"d{i}", "create") for i in range(40, 45)]
    expected += [("d0", "replace"), ("d1", "replace"), ("d2", "replace"), ("d1", "delete")]
    assert sorted((r["id"], r["operation"]) for r in rest) == sorted(expected)
    assert merge(dicts(h, replay) + rest) == truth(store)


@pytest.mark.parametrize("ac", [False, True])
def test_no_second_snapshot_after_batch_zero_commits(make_store, ac: bool) -> None:
    store = make_store(partitions=2)
    load(store, 30)
    h = snap(store, ac=ac)
    h.start()
    assert ops(h, h.run_batch()) == {"snapshot"}
    for round_ in range(2):
        load(store, 4, start=30 + 4 * round_)
        h.restart()
        rows = h.run_until_idle()
        assert ids(h, rows) == sorted(f"d{i}" for i in range(30 + 4 * round_, 34 + 4 * round_))
        assert "snapshot" not in ops(h, rows)
    h.restart()
    assert h.run_batch() is None
    assert all("snapshot" not in end for _start, end in h.offset_log[1:])  # only E0 carries the field


def test_replay_keeps_the_snapshot_decision_of_the_logged_batch(make_store, caplog) -> None:
    store = make_store(partitions=2)
    load(store, 20)
    h = snap(store)
    h.start()
    end = h.latest()  # batch 0 planned (and logged) with a snapshot
    plain = StreamHarness(fake_options(store, change_feed_mode=AVAD))  # restarted without the option
    plain.committed = end
    plain.restart()
    start = json.dumps(plain.planner.initialOffset())
    assert '"snapshot"' not in start
    with caplog.at_level(logging.INFO, logger="cosmosdb_datasource"):
        parts = plain.planner.partitions(json.loads(start), json.loads(end))
    assert parts and all(isinstance(p, CosmosSnapshotPartition) for p in parts)
    assert "planned with" in caplog.text
    assert len(plain.read_range(start, end)) == 20


def test_offsets_without_the_snapshot_field_mean_no_snapshot(make_store, caplog) -> None:
    store = make_store(partitions=2)
    load(store, 20)
    old = StreamHarness(fake_options(store, change_feed_mode=AVAD))  # offsets without the snapshot field
    old_start = old.start()
    old_end = old.latest()
    assert '"snapshot"' not in old_start and '"snapshot"' not in old_end
    parsed = offset(old_end)
    assert parsed.snapshot is None and json.dumps(parsed.to_dict()) == old_end  # byte-stable
    # An uncommitted batch 0 without the field, replayed with initial_snapshot=true: no snapshot, and a warning.
    h = snap(store)
    h.committed = old_end
    h.restart()
    replay_start = json.dumps(h.planner.initialOffset())
    assert offset(replay_start).snapshot is not None  # pending in this process...
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        parts = h.planner.partitions(json.loads(replay_start), json.loads(old_end))
    assert not any(isinstance(p, CosmosSnapshotPartition) for p in parts)  # ...but not planned
    assert "planned without a snapshot" in caplog.text
    # Committed offsets without the field never produce snapshot rows as batch starts.
    h.planner.commit(json.loads(old_end))
    load(store, 3, start=20)
    rows = h.run_until_idle()
    assert ids(h, rows) == ["d20", "d21", "d22"] and "snapshot" not in ops(h, rows)


@needs_42
@pytest.mark.parametrize("cap", [None, 7])
def test_admission_control_does_not_cap_the_snapshot(make_store, cap: int | None) -> None:
    store = make_store(partitions=2)
    load(store, 60)
    extra = {"max_items_per_trigger": cap} if cap else {}
    h = snap(store, ac=True, **extra)
    limit = ReadMaxRows(cap) if cap else ReadAllAvailable()
    assert h.planner.getDefaultReadLimit() == limit
    h.start()
    first = h.run_batch(limit)
    assert ops(h, first) == {"snapshot"} and len(first) == 60  # not limited by the cap
    load(store, 40, start=60)
    rows = h.run_until_idle(limit)
    assert ids(h, rows) == sorted(f"d{i}" for i in range(60, 100)) and "snapshot" not in ops(h, rows)
    if cap:
        assert len(h.batches) > 3 and all(len(b) <= cap + 2 for b in h.batches[1:])
    else:
        assert len(h.batches) == 2


@needs_42
def test_available_now_target_captured_before_the_start(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 50)
    h = snap(store, ac=True, max_items_per_trigger=5)
    h.planner.prepareForTriggerAvailableNow()  # PySpark 4.2 calls it before the first initialOffset()
    h.start()
    rows = h.run_until_idle(ReadMaxRows(5))  # terminates
    assert len(h.batches) == 1 and ops(h, rows) == {"snapshot"} and len(rows) == 50


@needs_42
def test_available_now_moves_the_target_past_the_snapshot_once_and_terminates(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 50)
    h = snap(store, ac=True, max_items_per_trigger=5)
    h.start()  # P0
    load(store, 20, start=50)  # after P0, before the target Spark captures
    h.planner.prepareForTriggerAvailableNow()
    load(store, 10, start=70)  # after that target, before batch 0 ran
    first = h.run_batch(ReadMaxRows(5))  # batch 0: the snapshot
    assert ops(h, first) == {"snapshot"} and ids(h, first) == sorted(f"d{i}" for i in range(80))
    load(store, 5, start=80)  # after batch 0, before the target is moved
    rows = h.run_until_idle(ReadMaxRows(5))  # terminates
    # The target moved once, to the position after batch 0: every change up to then is in this run.
    assert ids(h, rows) == sorted(f"d{i}" for i in range(50, 85)) and "snapshot" not in ops(h, rows)
    assert len(h.batches) > 3 and all(len(b) <= 5 + 2 for b in h.batches[1:])
    h.restart()  # next AvailableNow run: nothing new
    h.planner.prepareForTriggerAvailableNow()
    assert h.run_until_idle(ReadMaxRows(5)) == []


@needs_42
def test_available_now_moves_the_target_only_once(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 10)
    h = snap(store, ac=True, max_items_per_trigger=3)
    h.planner.prepareForTriggerAvailableNow()
    h.start()
    h.run_batch(ReadMaxRows(3))  # snapshot
    load(store, 4, start=10)
    planned = h.latest(ReadAllAvailable())  # moves the target to "now" (d10..d13)
    load(store, 1, start=30)
    assert h.latest(ReadAllAvailable()) == planned  # asked again from batch 0's end: the target stays
    store.delete("p30", "d30")
    first_changes = h.run_batch(ReadMaxRows(3))
    load(store, 4, start=20)  # after the moved target: not part of this run
    rest = h.run_until_idle(ReadMaxRows(3))
    assert ids(h, first_changes + rest) == sorted(f"d{i}" for i in range(10, 14))
    h.restart()
    h.planner.prepareForTriggerAvailableNow()
    later = h.run_until_idle(ReadMaxRows(3))
    assert sorted(set(ids(h, later))) == sorted(["d30", *(f"d{i}" for i in range(20, 24))])


@needs_42
def test_first_available_now_run_is_complete_with_writes_during_the_snapshot(make_store) -> None:
    """The scenario of a first (or full-refresh) triggered update: writes while batch 0 reads the container.

    One AvailableNow run must leave the merged result equal to the container.
    """
    store = make_store(partitions=2)
    load(store, 120)
    rng = random.Random(7)
    pages = [0]

    def during_snapshot() -> None:  # inserts, updates, deletes and re-creates between snapshot pages
        pages[0] += 1
        live = [d for d in store.live_documents()]
        for _ in range(3):
            doc = rng.choice(live)
            store.upsert({**doc, "v": rng.randint(1000, 9999)})
        victim = rng.choice(live)
        store.delete(victim["pk"], victim["id"])
        store.upsert({"id": f"n{pages[0]}", "pk": f"p{pages[0] % 41}", "v": -pages[0]})

    h = snap(store, ac=True, page_size=10)
    h.planner.prepareForTriggerAvailableNow()  # captured before P0, as Spark does
    h.start()
    store._before_query_page = during_snapshot
    first = h.run_batch(ReadAllAvailable())
    store._before_query_page = None
    assert ops(h, first) == {"snapshot"} and pages[0] > 3
    rows = first + h.run_until_idle(ReadAllAvailable())
    assert len(h.batches) >= 2
    assert merge(dicts(h, rows)) == truth(store)
    assert merge(dicts(h, first)) != truth(store)  # the snapshot alone is not the container state


@needs_42
def test_available_now_target_is_not_moved_without_a_snapshot(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 10)
    h = StreamHarness(fake_options(store, change_feed_mode=AVAD), admission_control=True)
    h.planner.prepareForTriggerAvailableNow()
    h.start()
    load(store, 5, start=10)  # after the target: next run
    assert h.run_until_idle(ReadAllAvailable()) == []
    h.restart()
    h.planner.prepareForTriggerAvailableNow()
    assert ids(h, h.run_until_idle(ReadAllAvailable())) == sorted(f"d{i}" for i in range(10, 15))


def test_without_admission_control_the_first_available_now_run_ends_after_the_snapshot(make_store) -> None:
    """PySpark 4.0/4.1: Spark runs one batch per AvailableNow run, so changes follow in the next run."""
    store = make_store(partitions=1)
    load(store, 10)
    h = snap(store)
    h.start()
    load(store, 3, start=10)
    first = h.run_batch()  # the single batch of this run
    assert ops(h, first) == {"snapshot"}
    h.restart()
    assert ids(h, h.run_batch()) == sorted(f"d{i}" for i in range(10, 13))


def test_snapshot_partitions_follow_partitions_per_feed_range(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 300, payload="x" * 400)
    h = snap(store, partitions_per_feed_range=3)
    start, end = h.start(), h.latest()
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    assert len(parts) == 6 and {p.feed_range for p in parts} == {f"{p.lo}-{p.hi}" for p in store.partitions}
    rows = h.read_range(start, end)
    assert len(rows) == len(set(h.column(rows, "id"))) == 300

    auto = snap(store, partitions_per_feed_range="auto")
    start, end = auto.start(), auto.latest()
    snapshot_parts = auto.planner.partitions(json.loads(start), json.loads(end))
    _rows, batch_parts = run_batch(fake_options(store, partitions_per_feed_range="auto"))
    assert len(snapshot_parts) == len(batch_parts) == 2  # one task per physical partition, like a batch read
    auto.planner.commit(json.loads(end))
    auto.committed = end
    load(store, 10, start=300)
    nxt = auto.latest()
    change_parts = auto.planner.partitions(json.loads(end), json.loads(nxt))
    assert len(change_parts) == 2 and all(isinstance(p, CosmosChangeFeedPartition) for p in change_parts)


def test_split_between_start_resolution_and_snapshot_planning(make_store, no_sleep) -> None:
    store = make_store(partitions=1)
    load(store, 120)
    h = snap(store, page_size=17)
    start = h.start()
    parent_token = dict(offset(start).snapshot.session)["-FF"]
    children = store.split("0")
    end = h.latest()
    parts = h.planner.partitions(json.loads(start), json.loads(end))
    assert [p.feed_range for p in parts] == [f"{c.lo}-{c.hi}" for c in children]  # current feed ranges
    assert all(p.session_token == parent_token for p in parts)
    store.query_lag_requests, store.query_lag_delta = 2, 50  # a stale replica of a child: token enforced
    rows = h.read_range(start, end)
    assert len(rows) == len(set(h.column(rows, "id"))) == 120
    assert set(h.column(rows, "feed_range")) == {f"{c.lo}-{c.hi}" for c in children}
    assert len(no_sleep) == 2  # two 404/1002 answers were retried
    h.planner.commit(json.loads(end))
    h.committed = end
    load(store, 10, start=120)
    assert ids(h, h.run_until_idle()) == sorted(f"d{i}" for i in range(120, 130))
    assert set(json.loads(h.committed)["ranges"]) == {f"{c.lo}-{c.hi}" for c in children}


def test_split_during_the_snapshot_query(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 150)
    h = snap(store, page_size=10, partitions_per_feed_range=2)
    start, end = h.start(), h.latest()
    pages: list[int] = []

    def before_page() -> None:
        pages.append(1)
        if len(pages) == 3:
            store.split("0")

    store._before_query_page = before_page
    rows = h.read_range(start, end)
    assert len(store.partitions) == 2
    assert len(rows) == len(set(h.column(rows, "id"))) == 150


def test_session_token_keeps_a_lagging_replica_from_serving_the_snapshot(make_store, no_sleep, monkeypatch) -> None:
    def scenario(pinned: bool) -> tuple[dict, dict, int]:
        store = make_store(partitions=2)
        load(store, 80)  # all written before P0
        h = snap(store)
        h.start()
        # Replicas serving the next queries have not applied the last 30 LSNs written before P0.
        store.query_lag_requests, store.query_lag_delta = 3, 30
        no_sleep.clear()
        with monkeypatch.context() as m:
            if not pinned:
                m.setattr(SnapshotState, "session_token_for", lambda self, lo, hi: None)
            rows = h.run_until_idle()
        return merge(dicts(h, rows)), truth(store), len(no_sleep)

    merged, expected, retries = scenario(pinned=True)
    assert merged == expected and retries >= 2  # 404/1002 until the replica caught up
    merged, expected, retries = scenario(pinned=False)
    assert retries == 0 and len(merged) < len(expected)  # without the token, pre-P0 writes are lost


def test_snapshot_waits_for_the_delay_after_the_start_was_resolved(make_store, monkeypatch) -> None:
    clock = [1000.0]
    events: list[tuple[str, float]] = []

    class FakeTime:
        @staticmethod
        def monotonic() -> float:
            return clock[0]

    def fake_sleep(seconds: float) -> None:
        events.append(("sleep", seconds))
        clock[0] += seconds

    monkeypatch.setattr(stream_module, "time", FakeTime)
    monkeypatch.setattr(retry, "sleep", fake_sleep)
    store = make_store(partitions=1)
    load(store, 5)
    store._before_query_page = lambda: events.append(("page", clock[0]))
    h = snap(store, initial_snapshot_delay_seconds="5")
    start = h.start()  # resolved at t=1000
    clock[0] = 1001.5
    end = h.latest()
    h.read_range(start, end)
    assert events[0] == ("sleep", 3.5) and events[1] == ("page", 1005.0)  # not before 1000 + 5
    h.planner.partitions(json.loads(start), json.loads(end))  # re-planned: deadline passed already
    assert [e for e in events if e[0] == "sleep"] == [("sleep", 3.5)]
    # Replay in a new process: waits from its own (later) resolution, at most the delay.
    events.clear()
    h.committed = end
    h.restart()
    replay_start = json.dumps(h.planner.initialOffset())
    h.planner.partitions(json.loads(replay_start), json.loads(end))
    assert events == [("sleep", 5.0)]
    events.clear()
    zero = snap(store, initial_snapshot_delay_seconds="0")
    s, e = zero.start(), zero.latest()
    zero.planner.partitions(json.loads(s), json.loads(e))
    assert events == []


def test_initial_snapshot_with_start_from_beginning_is_legal_but_redundant(make_store, caplog) -> None:
    store = make_store(partitions=2)
    load(store, 30)
    h = snap(store, change_feed_mode=LV, start_from="Beginning")
    with caplog.at_level(logging.INFO, logger="cosmosdb_datasource"):
        h.start()
    assert "redundant" in caplog.text
    first = h.run_batch()
    assert ops(h, first) == {"snapshot"} and len(first) == 30
    second = h.run_batch()  # the change feed from the beginning holds every document again
    assert ops(h, second) == {"upsert"} and len(second) == 30


def test_initial_snapshot_options(make_store) -> None:
    store = make_store(partitions=1)
    with pytest.raises(CosmosConfigError, match="true or false"):
        make_source(fake_options(store, change_feed_mode=AVAD, initial_snapshot="sometimes"))
    with pytest.raises(CosmosConfigError, match="initial_snapshot_delay_seconds"):
        make_source(fake_options(store, change_feed_mode=AVAD, initial_snapshot_delay_seconds="-1"))
    batch = make_source(fake_options(store, initial_snapshot="true"))
    with pytest.raises(CosmosConfigError, match=r"only apply to spark\.readStream"):
        batch.reader(batch.schema())
    stream = make_source(fake_options(store, initial_snapshot="true"))  # selects the change-feed schema
    assert stream.schema() == change_feed_schema()
    assert stream._opts.initial_snapshot and stream._opts.initial_snapshot_delay_seconds == 5.0
    assert not make_source(fake_options(store, change_feed_mode=AVAD))._opts.initial_snapshot
    with pytest.raises(CosmosConfigError, match="not supported for streaming"):
        q = make_source(fake_options(store, initial_snapshot="true", query="SELECT * FROM c WHERE c.v > 1"))
        q.streamReader(q.schema())


class Workload:
    """Random inserts, updates (often several per document), deletes and delete-then-recreate."""

    def __init__(self, store, rng: random.Random, *, deletes: bool) -> None:
        self.store, self.rng, self.deletes = store, rng, deletes
        self.live: set[int] = set()
        self.deleted: set[int] = set()
        self.next_id = 0
        self.counts: collections.Counter[str] = collections.Counter()

    def _write(self, i: int) -> None:
        self.store.upsert({"id": f"d{i}", "pk": f"p{i % 7}", "v": self.rng.randrange(10**6)})
        self.live.add(i)
        self.deleted.discard(i)

    def step(self) -> None:
        self.store.clock_step = self.rng.choice((0, 1))  # same-second writes force the lsn tie-break
        r = self.rng.random()
        if r < 0.3 or not self.live:
            self._write(self.next_id)
            self.next_id += 1
            self.counts["insert"] += 1
        elif r < 0.6:
            i = self.rng.choice(sorted(self.live))
            for _ in range(self.rng.randrange(1, 4)):
                self._write(i)
            self.counts["update"] += 1
        elif r < 0.8 and self.deletes:
            i = self.rng.choice(sorted(self.live))
            self.store.delete(f"p{i % 7}", f"d{i}")
            self.live.discard(i)
            self.deleted.add(i)
            self.counts["delete"] += 1
        elif self.deleted:
            self._write(self.rng.choice(sorted(self.deleted)))
            self.counts["recreate"] += 1
        else:
            self._write(self.rng.choice(sorted(self.live)))
            self.counts["update"] += 1

    def burst(self, n: int) -> None:
        for _ in range(n):
            self.step()


def run_scenario(make_store, seed: int, mode: str) -> tuple[list[dict], dict, Workload, int]:
    rng = random.Random(seed)
    store = make_store(partitions=3)
    work = Workload(store, rng, deletes=mode == AVAD)
    work.burst(60)  # before P0
    # start_from=Now in both modes: from Beginning, LatestVersion would not need the snapshot at all
    h = snap(store, change_feed_mode=mode, start_from="Now", partitions_per_feed_range=2, page_size=4)
    h.start()  # P0
    work.burst(5)  # after P0, before the snapshot is read
    split_at = rng.randrange(1, 8) if seed % 3 == 0 else None
    pages = [0]

    def during_snapshot() -> None:
        pages[0] += 1
        if pages[0] == split_at:
            store.split(store.partitions[0].id)
        work.burst(rng.randrange(0, 4))

    store._before_query_page = during_snapshot
    rows = list(h.run_batch())
    store._before_query_page = None
    during = sum(work.counts.values())
    for _ in range(rng.randrange(2, 5)):
        work.burst(rng.randrange(0, 15))
        rows += h.run_until_idle()
    rows += h.run_until_idle()  # writes have stopped: drain
    return dicts(h, rows), truth(store), work, during


SEEDS = range(8)


@pytest.mark.parametrize("mode", [AVAD, LV])
@pytest.mark.parametrize("seed", SEEDS)
def test_merged_stream_converges_to_the_container_state(make_store, seed: int, mode: str) -> None:
    rows, expected, work, during = run_scenario(make_store, seed, mode)
    assert merge(rows) == expected
    snapshot_keys = {(r["pk"], r["id"]) for r in rows if r["operation"] == "snapshot"}
    changed_keys = {(r["pk"], r["id"]) for r in rows if r["operation"] != "snapshot"}
    assert snapshot_keys and changed_keys & snapshot_keys  # both sources contribute to the same keys
    assert during > 65  # writes were interleaved with the snapshot's page reads
    if mode == AVAD:
        assert work.counts["delete"] and work.counts["recreate"]


@pytest.mark.parametrize("mode", [AVAD, LV])
def test_convergence_check_detects_a_sabotaged_merge(make_store, mode: str) -> None:
    results = [run_scenario(make_store, seed, mode) for seed in SEEDS]
    for sabotage in ("snapshot_wins", "drop_snapshot", "drop_changes"):
        mismatches = [
            seed
            for seed, (rows, expected, _w, _d) in zip(SEEDS, results, strict=True)
            if merge(rows, sabotage) != expected
        ]
        assert mismatches, f"sabotage {sabotage!r} went unnoticed for every seed"


def test_driver_metadata_calls_retry_throttling(make_store, no_sleep) -> None:
    store = make_store(partitions=3)
    load(store, 30)
    store.throttle_calls = {"properties": 2, "feed_ranges": 2, "resolve_start": 2, "current_lsn": 3}
    h = snap(store)
    h.start()
    rows = h.run_batch()
    assert len(rows) == 30
    assert len(no_sleep) == 9 and not any(store.throttle_calls.values())
    store.throttle_calls = {"current_lsn": 2, "feed_ranges": 1}
    load(store, 4, start=30)
    assert len(h.run_batch()) == 4 and len(no_sleep) == 12


def test_batch_planning_retries_throttled_metadata(make_store, no_sleep) -> None:
    store = make_store(partitions=2)
    load(store, 10)
    store.throttle_calls = {"properties": 1, "feed_ranges": 2}
    rows, _parts = run_batch(fake_options(store))
    assert len(rows) == 10 and len(no_sleep) == 3


def test_driver_retries_are_bounded_and_transient_only(make_store, no_sleep, monkeypatch) -> None:
    store = make_store(partitions=1)
    store.throttle_calls = {"feed_ranges": 5}
    h = StreamHarness(fake_options(store, change_feed_mode=LV, max_retries=2))
    with pytest.raises(FakeHttpError, match="429"):
        h.start()
    assert len(no_sleep) == 2
    no_sleep.clear()
    store.throttle_calls = {}

    def forbidden(self, rng, mode):
        raise FakeHttpError(403)

    monkeypatch.setattr(FakeBackend, "current_lsn", forbidden)
    other = StreamHarness(fake_options(store, change_feed_mode=LV))
    other.committed = json.dumps(other.planner.initialOffset())
    with pytest.raises(FakeHttpError, match="403"):
        other.latest()
    assert no_sleep == []
