"""Replays the PySpark Python-data-source call sequence without a JVM.

Mirrors (PySpark 4.0-4.2):
* ``worker/create_data_source.py``: DataSource(options=CaseInsensitiveDict), schema() unless a
  user schema is given, then the instance is pickled and shipped to other workers.
* ``worker/plan_data_source_read.py``: reader = ds.reader(schema) / streamReader(schema); the read
  function capturing the reader is pickled *before* ``partitions()``; every partition is pickled
  separately; an empty plan becomes ``[None]`` (4.0, 4.1) or ``[InputPartition(None)]`` (4.2); rows
  go through ``records_to_arrow_batches`` (type validation).
* ``streaming/python_streaming_source_runner.py``: a long-lived reader receives
  initialOffset / latestOffset / partitions / commit with offsets serialized as JSON.
* ``PythonMicroBatchStream.setLatestSeenOffset``: on restart ``partitions(offset, offset)``.
"""

from __future__ import annotations

import json
import pickle
from typing import Any

import pyarrow as pa
import pyspark
from pyspark import cloudpickle
from pyspark.sql.datasource import CaseInsensitiveDict, InputPartition
from pyspark.sql.types import ArrayType, MapType, StructType
from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches

from cosmosdb_datasource import CosmosDBDataSource


def _ship(obj: Any) -> Any:
    return cloudpickle.loads(cloudpickle.dumps(obj))


def make_source(options: dict[str, str]) -> CosmosDBDataSource:
    return _ship(CosmosDBDataSource(CaseInsensitiveDict(options)))


def validate_rows(rows: list[Any], schema: StructType, ds: CosmosDBDataSource) -> None:
    if rows:
        list(records_to_arrow_batches(iter(rows), 1000, schema, ds))


def _from_arrow(value: Any, dtype: Any) -> Any:
    if value is None:
        return None
    if isinstance(dtype, StructType):
        return tuple(_from_arrow(value[f.name], f.dataType) for f in dtype.fields)
    if isinstance(dtype, ArrayType):
        return [_from_arrow(v, dtype.elementType) for v in value]
    if isinstance(dtype, MapType):
        return {_from_arrow(k, dtype.keyType): _from_arrow(v, dtype.valueType) for k, v in value}
    return value


def as_rows(output: list[Any], schema: StructType) -> list[tuple]:
    """Return what ``read()`` produced as row tuples, also when it yielded ``pyarrow.RecordBatch``es."""
    rows: list[tuple] = []
    for item in output:
        if isinstance(item, pa.RecordBatch):
            columns = [
                [_from_arrow(v, f.dataType) for v in item.column(i).to_pylist()] for i, f in enumerate(schema.fields)
            ]
            rows.extend(zip(*columns, strict=True))
        else:
            rows.append(item)
    return rows


def spark_substitute_for_no_partitions() -> list[Any]:
    """What ``plan_data_source_read`` plans for a batch reader whose ``partitions()`` is empty."""
    major, minor = (int(p) for p in pyspark.__version__.split(".")[:2])
    return [InputPartition(None)] if (major, minor) >= (4, 2) else [None]


def run_batch(options: dict[str, str], schema: StructType | None = None) -> tuple[list[tuple], list[Any]]:
    rows, parts, _remaining = run_batch_with_filters(options, schema, None)
    return rows, parts


def run_batch_with_filters(
    options: dict[str, str], schema: StructType | None, filters: list[Any] | None
) -> tuple[list[tuple], list[Any], list[Any]]:
    """Like ``run_batch``; with ``filters``, call ``pushFilters`` first, as ``data_source_pushdown_filters`` does.

    Returns the rows, the partitions and the filters the reader left to Spark (checked to be input
    filters by reference, as Spark requires).
    """
    ds = make_source(options)
    schema = schema or ds.schema()
    reader = ds.reader(schema)
    remaining: list[Any] = []
    if filters is not None:
        remaining = list(reader.pushFilters(list(filters)))
        assert all(any(r is f for f in filters) for r in remaining), "pushFilters returned a filter it was not given"
    shipped = cloudpickle.dumps(reader)
    parts = reader.partitions()
    assert isinstance(parts, list)
    rows: list[tuple] = []
    for part in parts or spark_substitute_for_no_partitions():
        executor_reader = cloudpickle.loads(shipped)
        out = list(executor_reader.read(pickle.loads(pickle.dumps(part))))
        validate_rows(out, schema, ds)
        rows.extend(as_rows(out, schema))
    return rows, parts, remaining


class StreamHarness:
    """One streaming query: a planner-side reader plus fresh executor-side readers per task."""

    def __init__(self, options: dict[str, str], schema: StructType | None = None, *, admission_control: bool = False):
        self.options = options
        self.ds = make_source(options)
        self.schema = schema or self.ds.schema()
        self.admission_control = admission_control
        self.planner = self.ds.streamReader(self.schema)
        self._executor_blob = cloudpickle.dumps(self.ds.streamReader(self.schema))
        self.offset_log: list[tuple[str, str]] = []
        self.committed: str | None = None
        self.batches: list[list[tuple]] = []

    def start(self) -> str:
        """Start a fresh query.

        PySpark 4.0/4.1 call latestOffset() before initialOffset() here (MicroBatchExecution.constructNextBatch,
        then initialOffset when building the batch); 4.2 with admission control calls initialOffset() first.
        """
        if not self.admission_control:
            self.planner.latestOffset()
        self.committed = json.dumps(self.planner.initialOffset())
        return self.committed

    def restart(self) -> None:
        """Restart the query on the same checkpoint (fresh Python processes)."""
        self.planner.stop()
        self.planner = _ship(self.ds).streamReader(self.schema)
        self._executor_blob = cloudpickle.dumps(_ship(self.ds).streamReader(self.schema))
        assert self.committed is not None
        if not self.admission_control:
            self.planner.partitions(json.loads(self.committed), json.loads(self.committed))

    def latest(self, limit: Any = None) -> str:
        assert self.committed is not None
        if self.admission_control:
            return json.dumps(self.planner.latestOffset(json.loads(self.committed), limit))
        return json.dumps(self.planner.latestOffset())

    def read_range(self, start: str, end: str) -> list[tuple]:
        parts = self.planner.partitions(json.loads(start), json.loads(end))
        assert isinstance(parts, list)
        rows: list[tuple] = []
        for part in parts:
            executor_reader = cloudpickle.loads(self._executor_blob)
            out = list(executor_reader.read(pickle.loads(pickle.dumps(part))))
            validate_rows(out, self.schema, self.ds)
            rows.extend(out)
        return rows

    def run_batch(self, limit: Any = None) -> list[tuple] | None:
        """Run one trigger; return None when Spark would not run a batch (offset unchanged)."""
        assert self.committed is not None
        end = self.latest(limit)
        if end == self.committed:
            return None
        self.offset_log.append((self.committed, end))  # Spark logs the offsets before running the batch
        rows = self.read_range(self.committed, end)
        self.planner.commit(json.loads(end))
        self.committed = end
        self.batches.append(rows)
        return rows

    def run_until_idle(self, limit: Any = None, max_batches: int = 100) -> list[tuple]:
        rows: list[tuple] = []
        for _ in range(max_batches):
            batch = self.run_batch(limit)
            if batch is None:
                return rows
            rows.extend(batch)
        raise AssertionError("stream did not become idle")

    def column(self, rows: list[tuple], name: str) -> list[Any]:
        idx = self.schema.fieldNames().index(name)
        return [r[idx] for r in rows]
