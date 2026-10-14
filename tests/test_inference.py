"""Schema inference by sampling (infer_schema=true)."""

from __future__ import annotations

import logging

import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.inference import infer_schema, merge_types, value_type
from tests.conftest import fake_options
from tests.harness import make_source, run_batch

PK = PartitionKeyInfo(paths=("/pk",), kind="Hash", version=2)


def struct(*fields: tuple[str, object]) -> StructType:
    return StructType([StructField(n, t, True) for n, t in fields])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "dtype"),
    [
        (None, NullType()),
        (True, BooleanType()),
        (5, LongType()),
        (2**63, DoubleType()),
        (5.0, DoubleType()),
        ("x", StringType()),
        ([], ArrayType(NullType(), True)),
        ([None, 1, 2.5], ArrayType(DoubleType(), True)),
        ([1, "a"], ArrayType(StringType(), True)),
        ({"a": 1, "b": {"c": "x"}}, struct(("a", LongType()), ("b", struct(("c", StringType()))))),
    ],
)
def test_value_types(value: object, dtype: object) -> None:
    assert value_type(value) == dtype


@pytest.mark.parametrize(
    ("a", "b", "merged"),
    [
        (NullType(), LongType(), LongType()),
        (LongType(), DoubleType(), DoubleType()),
        (LongType(), StringType(), StringType()),
        (BooleanType(), LongType(), StringType()),
        (struct(("a", LongType())), StringType(), StringType()),
        (ArrayType(LongType(), True), ArrayType(StringType(), True), ArrayType(StringType(), True)),
        (ArrayType(NullType(), True), ArrayType(LongType(), True), ArrayType(LongType(), True)),
        (
            struct(("a", LongType()), ("b", NullType())),
            struct(("c", BooleanType()), ("a", DoubleType()), ("b", StringType())),
            struct(("a", DoubleType()), ("b", StringType()), ("c", BooleanType())),
        ),
    ],
)
def test_merge_rules(a: object, b: object, merged: object) -> None:
    assert merge_types(a, b) == merged  # type: ignore[arg-type]


def schema_of(docs: list, **kwargs: object) -> StructType:
    return infer_schema(docs, PK, raw_column="body", corrupt_column="_corrupt_record", **kwargs)  # type: ignore[arg-type]


def test_inferred_schema_layout(caplog: pytest.LogCaptureFixture) -> None:
    docs = [
        {"x": 1, "id": "a", "pk": "p", "_rid": "r", "_etag": "e", "_ts": 1, "_self": "s", "_attachments": "t"},
        {"id": "b", "pk": "p", "nothing": None, "x": 2.5, "tags": [], "body": "shadowed", "y": {"k": None}},
    ]
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        schema = schema_of(docs)
    assert schema == struct(
        ("id", StringType()),
        ("pk", StringType()),
        ("x", DoubleType()),
        ("nothing", StringType()),
        ("tags", ArrayType(StringType(), True)),
        ("y", struct(("k", StringType()))),
    )
    assert "'body' is not inferred" in caplog.text
    assert schema_of(docs, timestamp=True).fieldNames()[2:4] == ["x", "_ts"]
    system = ["x", "_rid", "_etag", "_ts", "_self", "_attachments"]
    assert schema_of(docs, system_properties=True).fieldNames()[2:8] == system
    assert schema_of([{"a": 1}]).fieldNames() == ["a"]  # a projection without id or the partition key
    with pytest.raises(CosmosConfigError, match="SELECT VALUE"):
        schema_of([1, 2])


def load(store, n: int = 90) -> None:
    for i in range(n):
        doc = {"id": f"d{i}", "pk": f"p{i % 7}", "n": i, "kind": ["a", "b"][i % 2], "nested": {"v": i * 1.5}}
        if i % 3 == 0:
            doc["tags"] = [f"t{i}"]
        if i == 89:
            doc["n"] = "late"  # outside a small sample: read as null under PERMISSIVE
        store.upsert(doc)


def test_batch_read_with_an_inferred_schema(make_store) -> None:
    store = make_store(partitions=3)
    load(store)
    ds = make_source(fake_options(store, infer_schema="true", sampling_size="30"))
    schema = ds.schema()
    assert schema == struct(
        ("id", StringType()),
        ("pk", StringType()),
        ("n", LongType()),
        ("kind", StringType()),
        ("nested", struct(("v", DoubleType()))),
        ("tags", ArrayType(StringType(), True)),
    )
    rows, _ = run_batch(fake_options(store, infer_schema="true", sampling_size="30"), schema)
    assert len(rows) == 90
    by_id = {r[0]: r for r in rows}
    assert by_id["d3"] == ("d3", "p3", 3, "b", (4.5,), ["t3"])
    assert by_id["d89"][2] is None


def test_sample_is_spread_over_partitions_and_bounded(make_store) -> None:
    store = make_store(partitions=3)
    load(store)
    make_source(fake_options(store, infer_schema="true", sampling_size="6")).schema()
    pages = [e for e in store.call_log if e[0] == "query_page"]
    assert len({(e[1], e[2]) for e in pages}) == 3 and sum(e[4] for e in pages) == 6


def test_sampling_query_and_user_query(make_store) -> None:
    store = make_store(partitions=2)
    load(store)
    sampled = make_source(
        fake_options(store, infer_schema="true", sampling_query="SELECT * FROM c WHERE c.id = 'd89'")
    ).schema()
    assert dict((f.name, f.dataType) for f in sampled.fields)["n"] == StringType()
    query = "SELECT * FROM c WHERE c.n >= 88 OR c.id = 'd89'"
    filtered = make_source(fake_options(store, infer_schema="true", query=query))
    assert dict((f.name, f.dataType) for f in filtered.schema().fields)["n"] == StringType()  # 88 and "late"
    with pytest.raises(CosmosConfigError, match="no documents"):
        make_source(fake_options(store, infer_schema="true", query="SELECT * FROM c WHERE c.n = -1")).schema()


def test_inference_options_are_validated(make_store) -> None:
    store = make_store()
    with pytest.raises(CosmosConfigError, match="only apply with infer_schema=true"):
        make_source(fake_options(store, sampling_size="10"))
    with pytest.raises(CosmosConfigError, match="batch reads"):
        make_source(fake_options(store, infer_schema="true", change_feed_mode="LatestVersion"))
    with pytest.raises(CosmosConfigError, match="batch reads"):
        make_source(fake_options(store, infer_schema="true", snapshot_stream="true"))
    with pytest.raises(CosmosConfigError, match="sampling_size"):
        make_source(fake_options(store, infer_schema="true", sampling_size="0"))
