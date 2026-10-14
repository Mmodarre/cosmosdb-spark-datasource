"""Change feed as a batch read, with and without a batch checkpoint."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from cosmosdb_datasource import commit_batch_checkpoint
from cosmosdb_datasource.batch_checkpoint import BatchCheckpoint
from cosmosdb_datasource.changefeed_batch import CosmosChangeFeedBatchReader
from cosmosdb_datasource.errors import CosmosConfigError, CosmosOffsetError
from cosmosdb_datasource.schema import change_feed_schema
from tests.conftest import fake_options
from tests.harness import make_source, run_batch

AVAD = "AllVersionsAndDeletes"


def write(store, start: int, n: int, keys: int = 7) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i})


def ids(rows: list[tuple]) -> list[str]:
    return sorted(r[0] for r in rows)


def test_bounded_read_from_beginning_to_now(make_store) -> None:
    store = make_store(partitions=3)
    write(store, 0, 50)
    ds = make_source(fake_options(store, change_feed_mode="LatestVersion"))
    assert ds.schema() == change_feed_schema()
    assert isinstance(ds.reader(ds.schema()), CosmosChangeFeedBatchReader)
    rows, _ = run_batch(fake_options(store, change_feed_mode="LatestVersion"))
    assert ids(rows) == ids([(f"d{i}",) for i in range(50)]) and {r[2] for r in rows} == {"upsert"}
    store.upsert({"id": "d0", "pk": "p0", "v": 100})
    assert len(run_batch(fake_options(store, change_feed_mode="LatestVersion"))[0]) == 50  # latest version only


def test_end_at_bounds_the_read(make_store) -> None:
    store = make_store(partitions=2)
    write(store, 0, 20)
    cut = store.clock + 1
    write(store, 20, 10)
    end_at = datetime.fromtimestamp(cut, timezone.utc).isoformat()
    rows, _ = run_batch(fake_options(store, change_feed_mode="LatestVersion", end_at=end_at))
    assert ids(rows) == ids([(f"d{i}",) for i in range(20)])
    with pytest.raises(CosmosConfigError, match="LatestVersion"):
        make_source(fake_options(store, change_feed_mode=AVAD, end_at=end_at))


def test_checkpoint_continues_and_replays_until_committed(make_store, tmp_path) -> None:
    store = make_store(partitions=3)
    write(store, 0, 30)
    path = str(tmp_path / "chk")
    opts = fake_options(store, change_feed_mode="LatestVersion", batch_checkpoint_location=path)
    first, _ = run_batch(opts)
    assert len(first) == 30
    write(store, 30, 5)
    again, _ = run_batch(opts)  # not committed: the same range again
    assert ids(again) == ids(first)
    assert commit_batch_checkpoint(path) is True
    second, _ = run_batch(opts)
    assert ids(second) == ids([(f"d{i}",) for i in range(30, 35)])
    assert commit_batch_checkpoint(path) is True
    assert run_batch(opts)[0] == []
    assert commit_batch_checkpoint(path) is True


def test_capped_reads_loop_until_caught_up(make_store, tmp_path) -> None:
    store = make_store(partitions=2)
    write(store, 0, 60)
    path = str(tmp_path / "chk")
    opts = fake_options(
        store, change_feed_mode="LatestVersion", batch_checkpoint_location=path, max_items_per_trigger="25"
    )
    seen: list[str] = []
    reads = 0
    while reads < 10:
        rows, _ = run_batch(opts)
        reads += 1
        assert len(rows) <= 26
        seen.extend(r[0] for r in rows)
        if commit_batch_checkpoint(path):
            break
    assert reads >= 3 and sorted(seen) == ids([(f"d{i}",) for i in range(60)])


def test_avad_batch_reads_deletes_after_the_first_position(make_store, tmp_path) -> None:
    store = make_store(partitions=1)
    write(store, 0, 10)
    path = str(tmp_path / "chk")
    opts = fake_options(store, change_feed_mode=AVAD, batch_checkpoint_location=path)
    assert run_batch(opts)[0] == []  # starts at Now
    commit_batch_checkpoint(path)
    store.delete("p1", "d1")
    store.upsert({"id": "d2", "pk": "p2", "v": 20})
    rows, _ = run_batch(opts)
    assert sorted((r[0], r[2]) for r in rows) == [("d1", "delete"), ("d2", "replace")]


def test_checkpoint_validation(make_store, tmp_path) -> None:
    store = make_store()
    write(store, 0, 5)
    path = str(tmp_path / "chk")
    with pytest.raises(CosmosOffsetError, match="no planned read"):
        commit_batch_checkpoint(path)
    run_batch(fake_options(store, change_feed_mode="LatestVersion", batch_checkpoint_location=path))
    commit_batch_checkpoint(path)
    other = make_store()
    with pytest.raises(CosmosOffsetError, match="different container"):
        run_batch(fake_options(other, change_feed_mode="LatestVersion", batch_checkpoint_location=path))
    with pytest.raises(CosmosOffsetError, match="change_feed_mode"):
        run_batch(fake_options(store, change_feed_mode=AVAD, batch_checkpoint_location=path))
    with pytest.raises(CosmosConfigError, match="driver can open"):
        BatchCheckpoint("abfss://c@a.dfs.core.windows.net/chk")
    assert BatchCheckpoint("dbfs:/tmp/chk")._offsets == "/dbfs/tmp/chk/offsets"
    stream_ds = make_source(fake_options(store, change_feed_mode="LatestVersion", batch_checkpoint_location=path))
    with pytest.raises(CosmosConfigError, match="batch reads"):
        stream_ds.streamReader(stream_ds.schema())
    planned = json.loads((tmp_path / "chk" / "offsets" / "0").read_text())
    assert planned["v"] == 1 and planned["caught_up"] is True and planned["start"]["initial"] is True


@pytest.mark.parametrize(
    "extra",
    [
        {"partition_key": "p1", "end_at": "2026-01-01T00:00:00Z"},
        {"item_ids": '[["d1", "p1"]]', "batch_checkpoint_location": "/tmp/x"},
        {"infer_schema": "true", "batch_checkpoint_location": "/tmp/x"},
    ],
)
def test_batch_change_feed_options_do_not_mix_with_document_reads(make_store, extra: dict[str, str]) -> None:
    with pytest.raises(CosmosConfigError, match=r"end_at|batch_checkpoint_location"):
        make_source(fake_options(make_store(), **extra))
