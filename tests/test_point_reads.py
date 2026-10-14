"""Logical-partition reads (partition_key), item reads (item_ids) and read_many."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.point_reads import CosmosItemsPartition, CosmosLogicalPartition
from tests.conftest import fake_options
from tests.harness import make_source, run_batch, run_batch_with_filters
from tests.test_pushdown import EqualTo, In

SCHEMA = StructType([StructField("id", StringType()), StructField("pk", StringType()), StructField("v", LongType())])


def load(store, n: int = 60, keys: int = 6) -> None:
    for i in range(n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % keys}", "v": i})


def test_partition_key_reads_one_logical_partition(make_store) -> None:
    store = make_store(partitions=3)
    load(store)
    store.upsert({"id": "n1", "pk": 5, "v": 100})
    rows, parts = run_batch(fake_options(store, partition_key="p2"), SCHEMA)
    assert [type(p) for p in parts] == [CosmosLogicalPartition]
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(60) if i % 6 == 2)
    assert {e[0] for e in store.call_log} == {"partition_query_page"}
    assert parts[0].feed_range in {f"{p.lo}-{p.hi}" for p in store.partitions}
    assert [r[0] for r in run_batch(fake_options(store, partition_key="5"), SCHEMA)[0]] == ["n1"]  # JSON number
    assert run_batch(fake_options(store, partition_key='"5"'), SCHEMA)[0] == []  # the string "5"
    filtered, _ = run_batch(fake_options(store, partition_key="p2", query="SELECT * FROM c WHERE c.v > 30"), SCHEMA)
    assert sorted(r[2] for r in filtered) == [32, 38, 44, 50, 56]


def test_hierarchical_prefix(make_store) -> None:
    store = make_store(pk_paths=("/t", "/u"), kind="MultiHash", partitions=2)
    for i in range(30):
        store.upsert({"id": f"d{i}", "t": f"t{i % 3}", "u": f"u{i % 5}"})
    rows, parts = run_batch(fake_options(store, partition_key='["t1"]'))
    # A prefix can span physical partitions: its EPK range is read in each, like a sub-split read.
    assert parts and not any(isinstance(p, CosmosLogicalPartition) for p in parts)
    assert {p.feed_range for p in parts} <= {f"{p.lo}-{p.hi}" for p in store.partitions}
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(30) if i % 3 == 1)
    rows, _ = run_batch(fake_options(store, partition_key='["t1", "u1"]'))
    assert sorted(r[0] for r in rows) == ["d1", "d16"]


def test_item_ids_are_grouped_by_physical_partition(make_store) -> None:
    store = make_store(partitions=3)
    load(store)
    ids = [[f"d{i}", f"p{i % 6}"] for i in range(0, 60, 4)] + [
        ["d0", "p0"],
        ["missing", "p1"],
        {"id": "d1", "pk": "p1"},
    ]
    rows, parts = run_batch(fake_options(store, item_ids=json.dumps(ids)), SCHEMA)
    assert all(isinstance(p, CosmosItemsPartition) for p in parts) and 1 < len(parts) <= 3
    assert sum(len(p.items) for p in parts) == 17  # the repeated pair is read once
    assert sorted(r[0] for r in rows) == sorted({f"d{i}" for i in range(0, 60, 4)} | {"d1"})
    assert "items=" in repr(parts[0]) and "d0" not in repr(parts[0])


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ({"partition_key": "p1", "item_ids": '[["a", "b"]]'}, "cannot be combined"),
        ({"partition_key": "p1", "change_feed_mode": "LatestVersion"}, "batch reads"),
        ({"item_ids": '[["a", "b"]]', "query": "SELECT * FROM c"}, "remove 'query'"),
        ({"item_ids": "[]"}, "non-empty"),
        ({"item_ids": '[["a"]]'}, "item_ids\\[0\\]"),
        ({"item_ids": '[[1, "b"]]'}, "non-empty string"),
        ({"partition_key": '{"a": 1}'}, "partition key value"),
    ],
)
def test_options_are_validated(make_store, extra: dict[str, str], message: str) -> None:
    with pytest.raises(CosmosConfigError, match=message):
        make_source(fake_options(make_store(), **extra))


def test_item_ids_take_no_pushed_filters(make_store) -> None:
    store = make_store(partitions=2)
    load(store)
    filters = [EqualTo(("id",), "d1")]
    opts = fake_options(store, filter_pushdown="true", item_ids=json.dumps([["d1", "p1"], ["d2", "p2"]]))
    rows, _parts, remaining = run_batch_with_filters(opts, SCHEMA, filters)
    assert remaining == filters and len(rows) == 2  # Spark applies the filter to the point reads


def test_many_item_ids_are_planned_by_partition(make_store) -> None:
    store = make_store(partitions=4)
    load(store, n=120, keys=12)
    ids = [[f"d{i}", f"p{i % 12}"] for i in range(120)] * 2
    calls = []
    ds = make_source(fake_options(store, item_ids=json.dumps(ids)))
    reader = ds.reader(SCHEMA)
    from tests.fake_cosmos import FakeBackend

    original = FakeBackend.key_epk

    def counting(self, key):
        calls.append(key)
        return original(self, key)

    FakeBackend.key_epk = counting
    try:
        parts = reader.partitions()
    finally:
        FakeBackend.key_epk = original
    assert len(calls) == 12  # one EPK per distinct key
    assert sum(len(p.items) for p in parts) == 120


def test_pushed_pk_equality_reads_only_those_logical_partitions(make_store) -> None:
    store = make_store(partitions=3)
    load(store)
    opts = fake_options(store, filter_pushdown="true")
    rows, parts, remaining = run_batch_with_filters(opts, SCHEMA, [In(("pk",), ("p1", "p4", None))])
    assert remaining == [] and sorted(p.key for p in parts) == ["p1", "p4"]
    assert sorted(r[2] for r in rows) == sorted(i for i in range(60) if i % 6 in (1, 4))
    store.call_log.clear()
    both = [In(("pk",), ("p1", "p4")), EqualTo(("pk",), "p4")]
    rows, parts, _ = run_batch_with_filters(opts, SCHEMA, both)
    assert [p.key for p in parts] == ["p4"] and len(rows) == 10
    _rows, parts, remaining = run_batch_with_filters(opts, SCHEMA, [EqualTo(("pk",), "5")])
    assert len(remaining) == 1 and not any(isinstance(p, CosmosLogicalPartition) for p in parts)


def test_read_many_function_reads_identity_batches(make_store, monkeypatch: pytest.MonkeyPatch) -> None:
    from cosmosdb_datasource import readmany

    store = make_store(partitions=2)
    load(store)
    captured = {}

    class Frame:
        def __getitem__(self, name: str) -> str:
            return name

        def select(self, *columns: str) -> Frame:
            captured["columns"] = columns
            return self

        def mapInArrow(self, fn, schema):
            captured["fn"], captured["schema"] = fn, schema
            return "frame"

    assert (
        readmany.read_many(
            Frame(),
            id_column="doc",
            pk_column="key",
            schema=StructType([StructField("id", StringType()), StructField("v", LongType())]),
            **fake_options(store),
        )
        == "frame"
    )
    assert captured["columns"] == ("doc", "key")
    batch = pa.RecordBatch.from_pydict({"doc": ["d1", "d7", "nope", None], "key": ["p1", "p1", "p1", "p1"]})
    out = list(captured["fn"](iter([batch])))
    assert pa.Table.from_batches(out).to_pylist() == [{"id": "d1", "v": 1}, {"id": "d7", "v": 7}]
    with pytest.raises(CosmosConfigError, match="does not take"):
        readmany.read_many(Frame(), **fake_options(store, partition_key="p1"))
    with pytest.raises(CosmosConfigError, match="does not take"):
        readmany.read_many(Frame(), **fake_options(store), Query="SELECT * FROM c")
