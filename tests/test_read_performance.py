"""Read-ahead of query pages and record-batch output."""

from __future__ import annotations

import sys
import threading
import time
import types
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches

from cosmosdb_datasource import arrow
from cosmosdb_datasource.backend import SdkBackend
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.prefetch import prefetched
from tests.conftest import fake_options
from tests.harness import make_source, run_batch


class Source:
    """An iterator that records how far it was advanced, optionally failing at one position."""

    def __init__(self, n: int, fail_at: int | None = None, delay: float = 0.0) -> None:
        self.n, self.fail_at, self.delay = n, fail_at, delay
        self.produced = 0
        self.closed = threading.Event()

    def __iter__(self) -> Source:
        return self

    def __next__(self) -> int:
        time.sleep(self.delay)
        if self.produced == self.fail_at:
            raise ConnectionError("boom")
        if self.produced >= self.n:
            raise StopIteration
        self.produced += 1
        return self.produced

    def close(self) -> None:
        self.closed.set()


def test_prefetch_keeps_order_and_bounds_the_read_ahead() -> None:
    source = Source(6)
    seen = []
    for item in prefetched(source, 1):
        time.sleep(0.05)  # the fetcher runs ahead while the consumer works
        seen.append(item)
        assert source.produced <= item + 1
    assert seen == [1, 2, 3, 4, 5, 6]
    assert source.closed.wait(2)


def test_prefetch_raises_in_order_and_stops_on_close() -> None:
    failing = Source(10, fail_at=3)
    got = []
    with pytest.raises(ConnectionError):
        for item in prefetched(failing, 2):
            got.append(item)
    assert got == [1, 2, 3]

    slow = Source(100, delay=0.01)
    pages = prefetched(slow, 1)
    assert next(pages) == 1
    pages.close()
    assert slow.closed.wait(2)
    assert slow.produced <= 3
    assert list(prefetched(Source(3), 0)) == [1, 2, 3]


class FakePager:
    """``ContainerProxy.query_items(...).by_page()`` as azure-cosmos returns it."""

    def __init__(self, pages: list[list[dict[str, Any]]], hook: Any, threads: list[str]) -> None:
        self._pages, self._hook, self._threads = pages, hook, threads
        self.continuation_token: str | None = None

    def by_page(self, continuation_token: str | None = None) -> FakePager:
        self._index = int(continuation_token) if continuation_token else 0
        return self

    def __iter__(self) -> FakePager:
        return self

    def __next__(self) -> Any:
        if self._index >= len(self._pages):
            raise StopIteration
        self._threads.append(threading.current_thread().name)
        self._hook({"x-ms-request-charge": "2", "x-ms-documentdb-partitionkeyrangeid": "0"}, None)
        page = self._pages[self._index]
        self._index += 1
        self.continuation_token = str(self._index) if self._index < len(self._pages) else None
        return iter(page)


class FakeContainer:
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages, self.threads = pages, []

    def query_items(self, *, response_hook: Any, **kwargs: Any) -> FakePager:
        return FakePager(self.pages, response_hook, self.threads)


@pytest.mark.parametrize("prefetch", [0, 1, 2])
def test_sdk_backend_pages_are_the_same_with_read_ahead(prefetch: int) -> None:
    pages = [[{"id": f"{p}-{i}"} for i in range(3)] for p in range(4)]
    container = FakeContainer(pages)
    backend = SdkBackend(container, prefetch_pages=prefetch)
    got = list(backend.query_pages("SELECT * FROM c", None, ("", "FF"), 3))
    assert [p.items for p in got] == pages
    assert [p.continuation for p in got] == ["1", "2", "3", None]
    assert all(p.request_charge == 2.0 and p.partitions == ("0",) for p in got)
    resumed = list(backend.query_pages("SELECT * FROM c", None, ("", "FF"), 3, continuation="2"))
    assert [p.items for p in resumed] == pages[2:]
    assert (set(container.threads) == {"cosmosdb-prefetch"}) == (prefetch > 0)


SCHEMA = StructType(
    [
        StructField("s", StringType()),
        StructField("i", IntegerType()),
        StructField("l", LongType()),
        StructField("d", DoubleType()),
        StructField("f", FloatType()),
        StructField("b", BooleanType()),
        StructField("dec", DecimalType(10, 2)),
        StructField("ts", TimestampType()),
        StructField("ntz", TimestampNTZType()),
        StructField("dt", DateType()),
        StructField("bin", BinaryType()),
        StructField("arr", ArrayType(StructType([StructField("x", LongType()), StructField("y", StringType())]))),
        StructField("m", MapType(StringType(), ArrayType(LongType()))),
        StructField("st", StructType([StructField("k", StringType()), StructField("n", DoubleType())])),
        StructField("null", NullType()),
    ]
)
ROWS = [
    (
        "é",
        1,
        2**40,
        1.5,
        0.25,
        True,
        Decimal("12.34"),
        datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc),
        datetime(2026, 1, 2, 3, 4, 5),
        date(2026, 1, 2),
        b"\x00\x01",
        [(1, "a"), None],
        {"k": [1, None]},
        ("v", 2.5),
        None,
    ),
    tuple([None] * 15),
]


class Named:
    @staticmethod
    def name() -> str:
        return "cosmosdb"


def test_record_batches_equal_pyspark_conversion() -> None:
    expected = pa.Table.from_batches(list(records_to_arrow_batches(iter(ROWS * 3), 4, SCHEMA, Named())))
    batches = list(arrow.record_batches(iter(ROWS * 3), SCHEMA, 4))
    assert [b.num_rows for b in batches] == [4, 2]
    assert pa.Table.from_batches(batches).equals(expected)
    assert list(arrow.record_batches(iter([]), SCHEMA, 4)) == []


def test_record_batches_fall_back_to_pyspark_conversion(monkeypatch: pytest.MonkeyPatch) -> None:
    real = pa.array
    calls = []

    def failing(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise pa.ArrowInvalid("rejected")
        return real(*args, **kwargs)

    monkeypatch.setattr(pa, "array", failing)
    batches = list(arrow.record_batches(iter(ROWS), SCHEMA, 10))
    expected = pa.Table.from_batches(list(records_to_arrow_batches(iter(ROWS), 10, SCHEMA, Named())))
    assert pa.Table.from_batches(batches).equals(expected)


def test_batch_reads_yield_record_batches_unless_disabled(make_store, monkeypatch: pytest.MonkeyPatch) -> None:
    store = make_store(partitions=2)
    for i in range(30):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 3}"})

    def output_types(**extra: str) -> set[type]:
        ds = make_source(fake_options(store, **extra))
        reader = ds.reader(ds.schema())
        return {type(item) for part in reader.partitions() for item in reader.read(part)}

    assert output_types() == {pa.RecordBatch}
    assert output_types(arrow_batches="false") == {tuple}
    monkeypatch.setitem(sys.modules, "_engine_pyspark", types.ModuleType("_engine_pyspark"))
    assert output_types() == {tuple}  # the Databricks engine: not verified there
    assert output_types(arrow_batches="true") == {pa.RecordBatch}
    rows, _ = run_batch(fake_options(store, page_size="7"))
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(30))


def test_options(make_store) -> None:
    store = make_store()
    opts = make_source(fake_options(store))._opts
    assert (opts.page_size, opts.prefetch_pages, opts.arrow_batches) == (5000, 1, "auto")
    with pytest.raises(CosmosConfigError, match="at most 4"):
        make_source(fake_options(store, prefetch_pages="5"))
    with pytest.raises(CosmosConfigError, match="arrow_batches"):
        make_source(fake_options(store, arrow_batches="sometimes"))
