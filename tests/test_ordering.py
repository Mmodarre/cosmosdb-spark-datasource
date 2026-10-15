"""Ordering the versions of a document: ``lsn_seq`` for changes that share an LSN.

The documented rule keeps, per ``(pk, id)``, the row with the largest
``struct(operation <> 'snapshot', lsn, lsn_seq)``; a delete means the document is gone.
"""

from __future__ import annotations

import json

from cosmosdb_datasource.documents import PartitionKeyInfo, to_change_record
from tests.conftest import fake_options
from tests.harness import StreamHarness

AVAD = "AllVersionsAndDeletes"


def rows_of(h: StreamHarness, rows: list[tuple]) -> list[dict]:
    names = h.schema.fieldNames()
    return [dict(zip(names, r, strict=True)) for r in rows]


def rank(r: dict) -> tuple:
    return (r["operation"] != "snapshot", r["lsn"], r["lsn_seq"])


def merged(rows: list[dict]) -> dict[tuple, dict]:
    best: dict[tuple, dict] = {}
    for r in rows:
        key = (r["pk"], r["id"])
        if key not in best or rank(r) > rank(best[key]):
            best[key] = r
    return {k: r for k, r in best.items() if r["operation"] != "delete"}


def avad_stream(store, **extra) -> StreamHarness:
    h = StreamHarness(fake_options(store, change_feed_mode=AVAD, **extra))
    h.start()
    return h


def test_versions_of_one_document_in_a_transactional_batch_are_ordered(make_store) -> None:
    store = make_store(partitions=1)
    h = avad_stream(store)
    store.clock_step = 0  # one transaction: one _ts and one LSN for every operation
    store.upsert({"id": "k", "pk": "tx", "v": 1})
    store.upsert({"id": "k", "pk": "tx", "v": 2}, same_lsn_as_previous=True)
    store.upsert({"id": "k", "pk": "tx", "v": 3}, same_lsn_as_previous=True)
    store.upsert({"id": "other", "pk": "tx", "v": 0}, same_lsn_as_previous=True)
    rows = rows_of(h, h.run_batch())
    mine = [r for r in rows if r["id"] == "k"]
    assert [(r["operation"], r["lsn_seq"]) for r in mine] == [("create", 0), ("replace", 1), ("replace", 2)]
    assert len({(r["_ts"], r["lsn"]) for r in mine}) == 1  # (_ts, lsn) cannot order them
    assert [r["lsn_seq"] for r in rows if r["id"] == "other"] == [0]
    assert json.loads(merged(rows)[("tx", "k")]["body"])["v"] == 3


def test_multi_region_conflict_keeps_the_winning_create(make_store) -> None:
    """A create/create conflict arrives as create (loser), then delete and create (winner) at one LSN."""
    store = make_store(partitions=1)
    h = avad_stream(store)
    store.clock_step = 0
    store.upsert({"id": "cc1", "pk": "k1", "v": "East US 2"})
    store.delete("k1", "cc1")
    store.upsert({"id": "cc1", "pk": "k1", "v": "Central US"}, same_lsn_as_previous=True)
    rows = rows_of(h, h.run_batch())
    assert [(r["operation"], r["lsn_seq"]) for r in rows] == [("create", 0), ("delete", 0), ("create", 1)]
    last_two = rows[1:]
    assert last_two[0]["lsn"] == last_two[1]["lsn"] and last_two[0]["_ts"] == last_two[1]["_ts"]
    assert len({rank(r) for r in rows}) == len(rows)  # a strict order, whatever the tie-break
    assert json.loads(merged(rows)[("k1", "cc1")]["body"])["v"] == "Central US"


def test_lsn_seq_is_stable_across_replays_sub_splits_and_splits(make_store) -> None:
    def scenario(**extra) -> list[tuple]:
        store = make_store(partitions=1)
        h = avad_stream(store, **extra)
        for i in range(30):
            store.upsert({"id": f"d{i}", "pk": f"p{i % 7}", "v": 0})
            store.upsert({"id": f"d{i}", "pk": f"p{i % 7}", "v": 1}, same_lsn_as_previous=True)
        rows = h.run_batch()
        assert h.read_range(*h.offset_log[0]) == rows  # replay: identical rows in identical order
        store.split(store.partitions[0].id)
        for i in range(30):
            store.delete(f"p{i % 7}", f"d{i}")
            store.upsert({"id": f"d{i}", "pk": f"p{i % 7}", "v": 2}, same_lsn_as_previous=True)
        rows += h.run_until_idle()
        return sorted((r["pk"], r["id"], r["operation"], r["lsn"], r["lsn_seq"]) for r in rows_of(h, rows))

    whole = scenario()
    assert {r[4] for r in whole} == {0, 1}
    assert scenario(partitions_per_feed_range=3) == whole


def test_latest_version_and_snapshot_rows_have_lsn_seq_zero(make_store) -> None:
    store = make_store(partitions=2)
    for i in range(10):
        store.upsert({"id": f"d{i}", "pk": f"p{i}", "v": i})
    opts = fake_options(store, change_feed_mode="LatestVersion", initial_snapshot="true")
    h = StreamHarness({**opts, "initial_snapshot_delay_seconds": "0"})
    h.start()
    rows = rows_of(h, h.run_until_idle())
    assert {r["operation"] for r in rows} == {"snapshot", "upsert"}
    assert {r["lsn_seq"] for r in rows} == {0}


def test_change_records_default_to_the_first_position() -> None:
    item = {"current": {"id": "a", "pk": "x", "_ts": 1}, "metadata": {"operationType": "create", "lsn": 3, "crts": 1}}
    assert to_change_record(item, AVAD, PartitionKeyInfo(paths=("/pk",))).lsn_seq == 0
