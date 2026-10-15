"""Real local SparkSession tests (JVM required). Skipped when Java 17+ is not available.

The fake service is shared with Spark's Python worker processes through JSON files in
``FAKE_COSMOS_DIR``; the internal test hook is enabled through the environment, which local-mode
Python workers inherit.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from cosmosdb_datasource.options import TEST_HOOKS_ENV
from tests.conftest import fake_options, java_major
from tests.fake_cosmos import FakeCosmosStore

ROOT = Path(__file__).resolve().parents[1]
JAVA = java_major()
pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(JAVA is None or JAVA < 17, reason=f"local Spark needs Java 17+ (found: {JAVA or 'none'})"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory):
    from pyspark.sql import SparkSession

    from cosmosdb_datasource import CosmosDBDataSource

    fake_dir = tmp_path_factory.mktemp("fake_cosmos")
    os.environ["FAKE_COSMOS_DIR"] = str(fake_dir)
    os.environ[TEST_HOOKS_ENV] = "1"
    os.environ["PYTHONPATH"] = os.pathsep.join([str(ROOT), str(ROOT / "src"), os.environ.get("PYTHONPATH", "")])
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.master("local[4]")
        .appName("cosmosdb-datasource-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.dataSource.register(CosmosDBDataSource)
    yield session
    session.stop()
    os.environ.pop("FAKE_COSMOS_DIR", None)  # later tests use in-memory stores


def _store(name: str, partitions: int = 3, docs: int = 500) -> FakeCosmosStore:
    store = FakeCosmosStore(name, partitions=partitions).register()
    for i in range(docs):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 31}", "v": i, "nested": {"x": i}})
    return store


def test_batch_read(spark) -> None:
    store = _store("spark_batch")
    df = spark.read.format("cosmosdb").options(**fake_options(store, partitions_per_feed_range=2)).load()
    assert df.rdd.getNumPartitions() == 6
    assert df.count() == 500
    assert df.select("id").distinct().count() == 500
    typed = (
        spark.read.format("cosmosdb")
        .schema("id STRING, pk STRING, v BIGINT, nested STRUCT<x: INT>, body STRING")
        .options(**fake_options(store))
        .load()
    )
    assert typed.filter("nested.x = v").count() == 500


def test_stream_available_now(spark, tmp_path: Path) -> None:
    store = _store("spark_stream", partitions=2, docs=300)
    reader = spark.readStream.format("cosmosdb").options(**fake_options(store, change_feed_mode="LatestVersion"))
    query = (
        reader.load()
        .writeStream.format("parquet")
        .option("path", str(tmp_path / "out"))
        .option("checkpointLocation", str(tmp_path / "chk"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(300)
    assert spark.read.parquet(str(tmp_path / "out")).count() == 300
    for i in range(300, 340):
        store.upsert({"id": f"d{i}", "pk": "new", "v": i})
    query = (
        reader.load()
        .writeStream.format("parquet")
        .option("path", str(tmp_path / "out"))
        .option("checkpointLocation", str(tmp_path / "chk"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination(300)
    out = spark.read.parquet(str(tmp_path / "out"))
    assert out.count() == 340 and out.select("id").distinct().count() == 340


PUSHDOWN_EXPRESSIONS = [
    "id = 'd005'",
    "id >= 'd070' AND _ts > 0",
    "s = 'a'",
    "s <> 'a'",
    "s LIKE 'a%' OR s LIKE '%b'",
    "s IN ('a', 'b', '5') AND n > 0",
    "s IS NULL",
    "n BETWEEN 0 AND 2",
    "d < 1.6",
    "b",
    "NOT b",
    "st.k = 'k1'",
    "pk = '5'",
    "pk IN ('p1', 'true', 'null')",
    "NOT (pk = 'p2')",
]


def _filter_pushdown_supported() -> bool:
    from pyspark.sql.datasource import DataSourceReader

    return hasattr(DataSourceReader, "pushFilters")  # PySpark >= 4.1


def test_filter_pushdown_matches_spark_filtering(spark) -> None:
    import random

    from tests.test_pushdown import SCHEMA, random_document

    store = FakeCosmosStore("spark_pushdown", partitions=3).register()
    rng = random.Random(7)
    for i in range(120):
        store.upsert(random_document(rng, i))
    conf = "spark.sql.python.filterPushdown.enabled"

    def load(pushdown: str):
        # A fresh relation per query: Spark 4.1 to 4.3 reuse the read planned with pushed filters for
        # every later scan of the same loaded DataFrame (README, "Filter pushdown").
        options = fake_options(store, filter_pushdown=pushdown)
        return spark.read.format("cosmosdb").schema(SCHEMA).options(**options).load()

    supported = _filter_pushdown_supported()
    try:
        for expr in PUSHDOWN_EXPRESSIONS:
            spark.conf.set(conf, "false")
            expected = sorted(map(repr, load("auto").filter(expr).collect()))
            spark.conf.set(conf, "true")
            if supported:
                assert sorted(map(repr, load("true").filter(expr).collect())) == expected, expr
            assert sorted(map(repr, load("auto").filter(expr).collect())) == expected, expr
        if not supported:  # PySpark 4.0: Spark would never call pushFilters
            with pytest.raises(Exception, match="does not push filters to Python data sources"):
                load("true").collect()
        if supported:
            plan = load("auto").filter("id = 'd005' AND s = 'a'")._jdf.queryExecution().executedPlan().toString()
            (spark_filter,) = [line for line in plan.splitlines() if " Filter (" in line]
            assert "s#" in spark_filter and "id#" not in spark_filter, plan  # id is exact, s is re-checked
            spark.conf.set(conf, "false")
            with pytest.raises(Exception, match="DATA_SOURCE_PUSHDOWN_DISABLED"):
                load("true").filter("id = 'd005'").collect()
    finally:
        spark.conf.unset(conf)


def test_inferred_schema(spark) -> None:
    store = _store("spark_infer", docs=200)
    df = spark.read.format("cosmosdb").options(**fake_options(store, infer_schema="true", sampling_size="50")).load()
    assert df.schema.simpleString() == "struct<id:string,pk:string,v:bigint,nested:struct<x:bigint>>"
    assert df.filter("nested.x = v").count() == 200


def test_partition_key(spark) -> None:
    store = _store("spark_partition_key", docs=200)
    scoped = spark.read.format("cosmosdb").options(**fake_options(store, partition_key="p1")).load()
    assert scoped.rdd.getNumPartitions() == 1
    assert sorted(r.id for r in scoped.collect()) == sorted(f"d{i}" for i in range(200) if i % 31 == 1)


def test_change_feed_batch_reads_with_a_checkpoint(spark, tmp_path: Path) -> None:
    from cosmosdb_datasource import commit_batch_checkpoint

    store = _store("spark_cf_batch", partitions=2, docs=120)
    location = str(tmp_path / "cf")
    options = fake_options(
        store, change_feed_mode="LatestVersion", batch_checkpoint_location=location, max_items_per_trigger="50"
    )
    out = str(tmp_path / "out")
    reads = 0
    while reads < 10:
        spark.read.format("cosmosdb").options(**options).load().write.mode("append").parquet(out)
        reads += 1
        if commit_batch_checkpoint(location):
            break
    written = spark.read.parquet(out)
    assert reads >= 3 and written.count() == 120 and written.select("id").distinct().count() == 120


def test_change_feed_batch_reads_that_plan_nothing(spark, tmp_path: Path) -> None:
    from cosmosdb_datasource import commit_batch_checkpoint

    store = _store("spark_cf_empty", partitions=2, docs=30)

    def read(**extra: str) -> list:
        return spark.read.format("cosmosdb").options(**fake_options(store, **extra)).load().collect()

    assert read(change_feed_mode="LatestVersion", start_from="Now") == []
    assert read(change_feed_mode="AllVersionsAndDeletes") == []
    avad = str(tmp_path / "avad")
    assert read(change_feed_mode="AllVersionsAndDeletes", batch_checkpoint_location=avad) == []
    location = str(tmp_path / "latest")
    assert len(read(change_feed_mode="LatestVersion", batch_checkpoint_location=location)) == 30
    assert commit_batch_checkpoint(location)
    assert read(change_feed_mode="LatestVersion", batch_checkpoint_location=location) == []  # the idle read


def test_properties_that_differ_only_in_case(spark) -> None:
    store = FakeCosmosStore("spark_case", partitions=1).register()
    store.upsert({"id": "1", "pk": "a", "name": "x"})
    store.upsert({"id": "2", "pk": "b", "Name": "y"})
    store.upsert({"id": "3", "pk": "c", "Name": "other", "name": "exact"})
    inferred = spark.read.format("cosmosdb").options(**fake_options(store, infer_schema="true")).load()
    assert inferred.columns == ["id", "pk", "name"]
    assert sorted(tuple(r) for r in inferred.select("id", "Name").collect()) == [
        ("1", "x"),
        ("2", "y"),
        ("3", "exact"),
    ]
    declared = spark.read.format("cosmosdb").schema("id STRING, name STRING").options(**fake_options(store)).load()
    assert sorted(tuple(r) for r in declared.collect()) == [("1", "x"), ("2", "y"), ("3", "exact")]


def test_read_many(spark) -> None:
    from cosmosdb_datasource import read_many

    store = _store("spark_read_many", partitions=3, docs=60)
    wanted = spark.createDataFrame(
        [("d1", "p1"), ("d2", "p2"), ("d2", "p2"), ("missing", "p0"), (None, "p1")], "id string, key string"
    )
    options = {k: v for k, v in fake_options(store).items() if k != "container"}
    rows = read_many(wanted, pk_column="key", container=store.name, **options).collect()
    assert sorted(r.id for r in rows) == ["d1", "d2"]  # a repeated identity is read once
    assert {r.pk for r in rows} == {"p1", "p2"} and rows[0].body.startswith("{")
    typed = read_many(wanted, pk_column="key", schema="id STRING, v BIGINT", container=store.name, **options)
    assert typed.schema.simpleString() == "struct<id:string,v:bigint>"
    assert sorted(tuple(r) for r in typed.collect()) == [("d1", 1), ("d2", 2)]


def test_typed_reads_with_the_option_spark_connect_adds(spark) -> None:
    # Databricks (serverless and classic) adds isUserSpecifiedSchema to the options of a read with
    # .schema(...); OSS Spark, local Spark Connect included, does not, so it is passed here as Databricks does.
    store = _store("spark_engine_option", docs=40)
    typed = (
        spark.read.format("cosmosdb")
        .schema("id STRING, v BIGINT")
        .options(**fake_options(store))
        .option("query", "SELECT * FROM c WHERE c.v >= 10")
        .option("isUserSpecifiedSchema", "true")
        .load()
    )
    assert sorted(r.v for r in typed.collect()) == list(range(10, 40))


def test_read_many_with_the_same_column_for_id_and_partition_key(spark) -> None:
    from cosmosdb_datasource import read_many

    store = FakeCosmosStore("spark_read_many_by_id", pk_paths=("/id",), partitions=2).register()
    for i in range(10):
        store.upsert({"id": f"d{i}", "v": i})
    wanted = spark.createDataFrame([("d1",), ("d1",), ("d2",), ("missing",), (None,)], "id string")
    options = {k: v for k, v in fake_options(store).items() if k != "container"}
    rows = read_many(wanted, id_column="id", pk_column="id", container=store.name, **options).collect()
    assert sorted((r.id, r.pk) for r in rows) == [("d1", "d1"), ("d2", "d2")]
    typed = read_many(
        wanted, id_column="id", pk_column="id", schema="id STRING, v BIGINT", container=store.name, **options
    )
    assert sorted(tuple(r) for r in typed.collect()) == [("d1", 1), ("d2", 2)]
