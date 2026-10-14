"""State and helpers shared by the batch and stream readers.

Reader objects are pickled into the Spark task closure *before* ``partitions()`` runs
(``write_read_func_and_partitions`` in ``pyspark/sql/worker/plan_data_source_read.py``), so
executors must not depend on anything the reader learns while planning: everything discovered
then (partition key definition, tokens, LSN bounds) travels inside the ``InputPartition``
objects. The stream reader keeps mutable driver-side state (known/initial/target/reported
offsets); that is safe because executor copies come from a separate, never-planned instance.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from typing import Any, TypeVar

from pyspark.sql.types import StructType

from cosmosdb_datasource import client
from cosmosdb_datasource._compat import local_struct_type
from cosmosdb_datasource.auth import SecretToken, mint_driver_token
from cosmosdb_datasource.options import AUTO, CosmosOptions
from cosmosdb_datasource.retry import RetryPolicy, call_with_retry
from cosmosdb_datasource.schema import RowBuilder, validate_schema

logger = logging.getLogger("cosmosdb_datasource")

AUTO_TARGET_BYTES_PER_PARTITION = 1 << 30  # 1 GiB of stored documents per Spark task
AUTO_MAX_SPLITS_PER_FEED_RANGE = 64

_T = TypeVar("_T")


class CosmosReaderBase:
    def __init__(self, opts: CosmosOptions, schema: StructType, *, stream: bool) -> None:
        schema = local_struct_type(schema)  # the engine may pass its own StructType (see _compat)
        validate_schema(schema)
        self._opts = opts
        self._schema = schema
        self._stream = stream

    # -- driver side ----------------------------------------------------------------------
    def _driver_token(self) -> SecretToken | None:
        return mint_driver_token(self._opts.auth, self._opts.endpoint)

    def _driver_backend(self, token: SecretToken | None) -> Any:
        return client.get_backend(self._opts, token)

    def _driver_call(self, what: str, fn: Callable[[], _T]) -> _T:
        """A driver-side metadata request, retried on transient errors (throttling, timeouts, 5xx)."""
        return call_with_retry(fn, self._retry_policy(), what)

    def _splits_per_range(self, backend: Any, n_ranges: int) -> int:
        factor = self._opts.partitions_per_feed_range
        if factor != AUTO:
            return int(factor)
        usage = backend.quota()
        size_kb = None
        if usage:
            size_kb = usage.get("documentsSize") or usage.get("collectionSize")
        if not size_kb:
            logger.warning("partitions_per_feed_range=auto: container size unavailable; using 1.")
            return 1
        per_range_bytes = size_kb * 1024 / max(1, n_ranges)
        splits = math.ceil(per_range_bytes / AUTO_TARGET_BYTES_PER_PARTITION)
        splits = max(1, min(AUTO_MAX_SPLITS_PER_FEED_RANGE, splits))
        logger.info(
            "partitions_per_feed_range=auto: %.1f GiB across %d feed ranges -> %d split(s) per range",
            size_kb / (1 << 20),
            n_ranges,
            splits,
        )
        return splits

    # -- executor side --------------------------------------------------------------------
    def _backend(self, token: SecretToken | None) -> Any:
        return client.get_backend(self._opts, token)

    def _row_builder(self) -> RowBuilder:
        return RowBuilder(
            self._schema,
            stream=self._stream,
            raw_column=self._opts.raw_json_column,
            corrupt_column=self._opts.corrupt_record_column,
            parse_mode=self._opts.parse_mode,
        )

    def _retry_policy(self) -> RetryPolicy:
        return RetryPolicy(max_retries=self._opts.max_retries, max_backoff_seconds=self._opts.retry_max_backoff_seconds)

    def _log_metrics(self, what: str, started: float, **values: Any) -> None:
        level = logging.WARNING if self._opts.log_metrics else logging.DEBUG
        if not logger.isEnabledFor(level):
            return
        elapsed = time.monotonic() - started
        details = " ".join(f"{k}={v:.2f}" if isinstance(v, float) else f"{k}={v}" for k, v in values.items())
        logger.log(level, "cosmosdb %s finished in %.2fs %s", what, elapsed, details)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._opts!r})"
