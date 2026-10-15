"""State and helpers shared by the batch and stream readers.

Reader objects are pickled into the Spark task closure *before* ``partitions()`` runs
(``write_read_func_and_partitions`` in ``pyspark/sql/worker/plan_data_source_read.py``), so
executors must not depend on anything the reader learns while planning: everything discovered
then (partition key definition, tokens, LSN bounds) travels inside the ``InputPartition``
objects. The stream reader keeps mutable driver-side state (known/initial/target/reported
offsets); that is safe because executor copies come from a separate, never-planned instance.
"""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any, TypeVar

from pyspark.sql.datasource import InputPartition
from pyspark.sql.types import StructType

from cosmosdb_datasource import client, throughput
from cosmosdb_datasource._compat import local_struct_type
from cosmosdb_datasource.auth import SecretToken, mint_driver_token
from cosmosdb_datasource.errors import CosmosDataSourceError
from cosmosdb_datasource.options import AUTO, CosmosOptions
from cosmosdb_datasource.retry import RetryPolicy, call_with_retry
from cosmosdb_datasource.schema import RowBuilder, validate_schema
from cosmosdb_datasource.throughput import RuBudget, ThrottledBackend

logger = logging.getLogger("cosmosdb_datasource")

_warned_sub_split = False  # the RU warning for sub-splitting is logged once per process

_T = TypeVar("_T")
_P = TypeVar("_P")

# With spark.sql.streaming.triggerAvailableNowWrapper.enabled=true, PySpark 4.0/4.1 wrap the source
# (AvailableNowDataStreamWrapper), which calls latestOffset() while the logical plan is built. On a restart
# that comes before setLatestSeenOffset, so the reader can only compute the run's end from a newly resolved
# start, and the wrapper returns that end for every batch of the run. A new query without the wrapper also
# calls latestOffset() first, so the wrapper shows only when setLatestSeenOffset follows such a call.
AVAILABLE_NOW_WRAPPER_UNSUPPORTED = (
    "cosmosdb: Spark asked for the latest offset before it passed the checkpoint's offset to the reader. "
    "spark.sql.streaming.triggerAvailableNowWrapper.enabled=true does that on PySpark 4.0 and 4.1 "
    "(Databricks Runtime 15.4 LTS to 17.3 LTS), and the cosmosdb source does not support it: the run "
    "would read from a newly resolved start instead of the checkpoint. Remove the conf; Trigger.AvailableNow "
    "then runs one micro-batch per run on these runtimes."
)


class CosmosEmptyPartition(InputPartition):
    """The single partition of a batch read that has nothing to read.

    Spark replaces an empty list from a batch reader's ``partitions()`` with a partition of its own:
    ``[None]`` in PySpark 4.0 and 4.1, ``[InputPartition(None)]`` from 4.2. Batch readers return this
    partition instead, so ``read()`` gets the same value on every Spark version.
    """

    def __init__(self) -> None:
        super().__init__(0)


def planned(parts: list[Any]) -> list[Any]:
    """Return ``parts``, or the empty partition if there are none (batch readers)."""
    return list(parts) if parts else [CosmosEmptyPartition()]


def is_empty_partition(partition: Any) -> bool:
    """Return True for a partition that reads nothing: the empty partition, or Spark's substitute for it.

    Spark's substitute is matched by class name: on Databricks serverless it is an instance of the
    engine's ``InputPartition`` (see :mod:`cosmosdb_datasource._compat`).
    """
    if partition is None or isinstance(partition, CosmosEmptyPartition):
        return True
    return type(partition).__name__ == "InputPartition" and getattr(partition, "value", None) is None


def unexpected_partition(partition: Any) -> CosmosDataSourceError:
    kind = f"{type(partition).__module__}.{type(partition).__qualname__}"
    return CosmosDataSourceError(f"The cosmosdb reader got a partition it did not plan: {kind}.")


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

    @contextlib.contextmanager
    def _task_backend(self, token: SecretToken | None, partition: Any) -> Iterator[Any]:
        """Yield the backend of one task's read, charged to the read's RU budget if it has one."""
        backend = client.get_backend(self._opts, token)
        budget = getattr(partition, "ru_budget", None)
        task = ThrottledBackend(backend, budget) if budget is not None else backend
        try:
            yield task
        finally:
            throughput.leave(task)

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
