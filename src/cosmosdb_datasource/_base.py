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
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from pyspark.sql.types import StructType

from cosmosdb_datasource import client
from cosmosdb_datasource._compat import local_struct_type
from cosmosdb_datasource.auth import SecretToken, mint_driver_token
from cosmosdb_datasource.options import AUTO, CosmosOptions
from cosmosdb_datasource.retry import RetryPolicy, call_with_retry
from cosmosdb_datasource.schema import RowBuilder, validate_schema
from cosmosdb_datasource.throughput import RuBudget, ThrottledBackend

logger = logging.getLogger("cosmosdb_datasource")

_warned_sub_split = False  # the RU warning for sub-splitting is logged once per process

_T = TypeVar("_T")
_P = TypeVar("_P")


class CosmosReaderBase:
    def __init__(self, opts: CosmosOptions, schema: StructType, *, stream: bool) -> None:
        schema = local_struct_type(schema)  # the engine may pass its own StructType (see _compat)
        validate_schema(schema)
        self._opts = opts
        self._schema = schema
        self._stream = stream

    def _driver_token(self) -> SecretToken | None:
        return mint_driver_token(self._opts.auth, self._opts.endpoint)

    def _driver_backend(self, token: SecretToken | None) -> Any:
        return client.get_backend(self._opts, token)

    def _driver_call(self, what: str, fn: Callable[[], _T]) -> _T:
        """Run a driver-side metadata request, retried on transient errors (throttling, timeouts, 5xx)."""
        return call_with_retry(fn, self._retry_policy(), what)

    def _splits_per_range(self) -> int:
        """Return the EPK sub-ranges per physical partition for query reads (batch, snapshots).

        The service answers an EPK-filtered query by scanning the whole physical partition, so every
        sub-range costs about the RU of a scan of the partition (measured: 2, 8 and 16 sub-ranges cost
        2.0, 6.3 and 12 times the RU of one, without a shorter read). ``auto`` therefore reads each
        physical partition as one task.
        """
        factor = self._opts.partitions_per_feed_range
        if factor == AUTO:
            return 1
        splits = int(factor)
        global _warned_sub_split
        if splits > 1 and not _warned_sub_split:
            _warned_sub_split = True
            logger.warning(
                "partitions_per_feed_range=%d: each EPK sub-range query scans its whole physical partition, "
                "so a full read costs about %d times the RU of partitions_per_feed_range=1.",
                splits,
                splits,
            )
        return splits

    def _backend(self, token: SecretToken | None, partition: Any = None) -> Any:
        backend = client.get_backend(self._opts, token)
        budget = getattr(partition, "ru_budget", None)
        return ThrottledBackend(backend, budget) if budget is not None else backend

    def _with_budget(self, parts: list[_P]) -> list[_P]:
        """Give every input partition an equal share of ``target_ru_per_second`` (one read, one budget)."""
        target = self._opts.target_ru_per_second
        if target and parts:
            budget = RuBudget(uuid.uuid4().hex, target / len(parts))
            for part in parts:
                part.ru_budget = budget  # type: ignore[attr-defined]
        return parts

    def _row_builder(self, *, snapshot: bool = False) -> RowBuilder:
        return RowBuilder(
            self._schema,
            stream=self._stream,
            raw_column=self._opts.raw_json_column,
            corrupt_column=self._opts.corrupt_record_column,
            parse_mode=self._opts.parse_mode,
            snapshot=snapshot,
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
