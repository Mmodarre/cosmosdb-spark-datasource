"""Reads that plan nothing: batch readers never return an empty plan, and every reader reads Spark's substitute."""

from __future__ import annotations

import json
import pickle

import pytest
from pyspark import cloudpickle
from pyspark.sql.datasource import InputPartition

from cosmosdb_datasource import commit_batch_checkpoint
from cosmosdb_datasource._base import CosmosEmptyPartition
from cosmosdb_datasource.errors import CosmosDataSourceError
from tests.conftest import fake_options
from tests.harness import make_source, run_batch

AVAD = "AllVersionsAndDeletes"
SUBSTITUTES = [None, InputPartition(None), CosmosEmptyPartition()]  # PySpark 4.0/4.1, 4.2, this package


def write(store, start: int, n: int) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 5}", "v": i})


def plan(options: dict[str, str]) -> tuple[object, list[object]]:
    ds = make_source(options)
    reader = ds.reader(ds.schema())
    return reader, reader.partitions()


@pytest.mark.parametrize(
    "extra",
    [
        {"change_feed_mode": "LatestVersion", "start_from": "Now"},
        {"change_feed_mode": AVAD},
    ],
    ids=["latest-version-from-now", "avad-first-read"],
)
def test_change_feed_batch_read_without_changes_plans_the_empty_partition(make_store, extra) -> None:
    store = make_store(partitions=3)
    write(store, 0, 10)
    _reader, parts = plan(fake_options(store, **extra))
    assert len(parts) == 1 and isinstance(parts[0], CosmosEmptyPartition)
    assert run_batch(fake_options(store, **extra))[0] == []


def test_first_avad_read_with_a_checkpoint_and_the_idle_read_after_catching_up(make_store, tmp_path) -> None:
    store = make_store(partitions=2)
    write(store, 0, 10)
    location = str(tmp_path / "cf")
    avad = fake_options(store, change_feed_mode=AVAD, batch_checkpoint_location=location)
    rows, parts = run_batch(avad)
    assert rows == [] and isinstance(parts[0], CosmosEmptyPartition)
    assert commit_batch_checkpoint(location)
    store.delete("p1", "d1")
    rows, _ = run_batch(avad)
    assert [(r[0], r[2]) for r in rows] == [("d1", "delete")]
    assert commit_batch_checkpoint(location)
    rows, parts = run_batch(avad)  # nothing new since the committed read
    assert rows == [] and isinstance(parts[0], CosmosEmptyPartition)


def test_idle_latest_version_read_after_catching_up(make_store, tmp_path) -> None:
    store = make_store(partitions=2)
    write(store, 0, 20)
    location = str(tmp_path / "cf")
    options = fake_options(store, change_feed_mode="LatestVersion", batch_checkpoint_location=location)
    assert len(run_batch(options)[0]) == 20
    assert commit_batch_checkpoint(location)
    rows, parts = run_batch(options)
    assert rows == [] and isinstance(parts[0], CosmosEmptyPartition)


def test_pushed_partition_keys_that_exclude_each_other_plan_the_empty_partition(make_store, pushdown_api) -> None:
    from tests.test_pushdown import EqualTo  # Spark's filter classes, also on PySpark 4.0

    store = make_store(partitions=2)
    write(store, 0, 10)
    ds = make_source(fake_options(store, filter_pushdown="true"))
    reader = ds.reader(ds.schema())
    reader.pushFilters([EqualTo(("pk",), "p1"), EqualTo(("pk",), "p2")])
    parts = reader.partitions()
    assert len(parts) == 1 and isinstance(parts[0], CosmosEmptyPartition)
    assert store.call_log == []  # no query was run for it


@pytest.mark.parametrize("substitute", SUBSTITUTES, ids=["none", "input-partition-none", "empty-partition"])
@pytest.mark.parametrize(
    "extra",
    [
        {},
        {"filter_pushdown": "true"},
        {"item_ids": '[["d1", "p1"]]'},
        {"change_feed_mode": "LatestVersion"},
    ],
    ids=["batch", "pushdown", "item-ids", "change-feed-batch"],
)
def test_batch_readers_read_nothing_for_an_empty_plan(make_store, substitute, extra, pushdown_api) -> None:
    store = make_store()
    write(store, 0, 5)
    reader, _parts = plan(fake_options(store, **extra))
    executor_reader = cloudpickle.loads(cloudpickle.dumps(reader))
    assert list(executor_reader.read(pickle.loads(pickle.dumps(substitute)))) == []


@pytest.mark.parametrize("substitute", SUBSTITUTES, ids=["none", "input-partition-none", "empty-partition"])
@pytest.mark.parametrize(
    "extra", [{"change_feed_mode": "LatestVersion"}, {"snapshot_stream": "true"}], ids=["change-feed", "snapshot"]
)
def test_stream_readers_read_nothing_for_an_empty_partition(make_store, substitute, extra) -> None:
    store = make_store()
    write(store, 0, 5)
    ds = make_source(fake_options(store, **extra))
    reader = ds.streamReader(ds.schema())
    assert list(reader.read(substitute)) == []


def test_a_partition_the_reader_did_not_plan_fails(make_store) -> None:
    store = make_store()
    reader, _parts = plan(fake_options(store))
    with pytest.raises(CosmosDataSourceError, match="did not plan"):
        list(reader.read(InputPartition({"some": "value"})))
    stream = make_source(fake_options(store, change_feed_mode="LatestVersion"))
    with pytest.raises(CosmosDataSourceError, match="did not plan"):
        list(stream.streamReader(stream.schema()).read(InputPartition(1)))


def test_stream_offsets_without_changes_plan_no_tasks(make_store) -> None:
    store = make_store()
    ds = make_source(fake_options(store, change_feed_mode="LatestVersion", start_from="Now"))
    reader = ds.streamReader(ds.schema())
    start = reader.initialOffset()
    end = reader.latestOffset()
    assert json.dumps(start) != json.dumps(end)  # batch 0 still runs, with no tasks
    assert reader.partitions(start, end) == []
