"""Batch-read rows as ``pyarrow.RecordBatch``es.

PySpark converts tuples to Arrow in Python, value by value (``records_to_arrow_batches``), which cost
about 2.6 µs per document of a 16-column typed schema with nested structs in a local measurement; a
``RecordBatch`` yielded by ``read()`` is passed through as is. Building it from the same tuples with
``pyarrow.array`` per column costs about 0.6 µs. The values are the ones the row builder already produces for
PySpark's conversion, so both paths read the same rows; a chunk that pyarrow does not accept as given
goes through PySpark's own conversion instead.

The Arrow schema must be the one the worker derives from the output schema: PySpark 4.2 checks it
(``pa_schema.equals``), and 4.0 and 4.1 hand the batch to the JVM unchecked. It is built with the same
``to_arrow_schema`` call as the worker of the running version.
"""

from __future__ import annotations

from collections.abc import Iterator
from itertools import islice
from typing import Any

from pyspark.sql.types import StructType

from cosmosdb_datasource import _compat
from cosmosdb_datasource.errors import CosmosDataSourceError


class _Named:
    """The ``data_source`` argument of ``records_to_arrow_batches``, used only for its name in errors."""

    @staticmethod
    def name() -> str:
        return "cosmosdb"


def record_batches(rows: Iterator[tuple[Any, ...]], schema: StructType, chunk_rows: int) -> Iterator[Any]:
    """Yield ``rows`` as record batches of at most ``chunk_rows`` rows."""
    import pyarrow as pa  # type: ignore[import-untyped]

    arrow_schema = _compat.arrow_schema(schema)
    while True:
        chunk = list(islice(rows, chunk_rows))
        if not chunk:
            return
        try:
            columns = zip(*chunk, strict=True)
            arrays = [pa.array(column, type=field.type) for column, field in zip(columns, arrow_schema, strict=True)]
            yield pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)
        except (pa.ArrowException, TypeError, ValueError, OverflowError) as exc:
            try:
                from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches
            except ImportError:  # a runtime whose PySpark has no such helper
                raise CosmosDataSourceError(
                    f"cosmosdb could not convert a page to Arrow ({type(exc).__name__}); read with arrow_batches=false."
                ) from exc
            yield from records_to_arrow_batches(iter(chunk), len(chunk), schema, _Named())  # type: ignore[arg-type]
