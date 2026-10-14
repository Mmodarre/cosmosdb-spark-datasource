"""Real local SparkSession tests (JVM required). Skipped when Java 17+ is not available.

The fake service is shared with Spark's Python worker processes through JSON files in
``FAKE_COSMOS_DIR``; the internal test hook is enabled through the environment, which local-mode
Python workers inherit.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from cosmosdb_datasource.options import TEST_HOOKS_ENV
from tests.conftest import fake_options
from tests.fake_cosmos import FakeCosmosStore

ROOT = Path(__file__).resolve().parents[1]


def _java_major() -> int | None:
    java = shutil.which("java")
    if not java:
        return None
    try:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30, check=False)
    except Exception:
        return None
    match = re.search(r'version "(\d+)', out.stderr + out.stdout)
    return int(match.group(1)) if match else None


JAVA = _java_major()
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
