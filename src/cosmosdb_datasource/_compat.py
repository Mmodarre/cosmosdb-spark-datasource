"""Bridge between the PySpark build that *runs* the data source and the one user code imports.

On Databricks serverless (Spark 4.2.0 server, environment version 4) the Python data source
workers run the engine's own PySpark, shipped as the package ``_engine_pyspark`` (``__version__``
4.4.0.dev0), while ``import pyspark`` in the *same* process resolves to the environment's client
PySpark (databricks-connect, 4.0.0). Classes therefore exist twice and ``isinstance`` across the
two copies is false. Two consequences are handled here:

* **Schemas.** The engine's batch planner converts the output schema to the client's classes
  before calling ``DataSource.reader()``, but its streaming source runner passes an *engine*
  ``StructType`` to ``DataSource.streamReader()``. :func:`local_struct_type` converts any foreign
  ``StructType`` through its JSON form so the type checks in :mod:`cosmosdb_datasource.schema`
  work.
* **Admission control.** The engine uses the PySpark 4.2+ protocol for every reader whose
  ``latestOffset`` takes parameters: it calls ``getDefaultReadLimit()`` (serialized with
  ``dataclasses.asdict`` plus the class *name*), ``latestOffset(start, limit)`` with its own
  ``ReadLimit`` classes, ``reportLatestOffset()``, and, for ``Trigger.AvailableNow``, checks
  ``isinstance(reader, SupportsTriggerAvailableNow)`` against its own mixin. Client PySpark 4.0
  has none of these. The classes are taken from the engine module when the engine is loaded in
  the process, then from ``pyspark``, then from local stand-ins with the same names and fields
  (the wire format only carries the name and the fields).

If no ``SupportsTriggerAvailableNow`` is found when :mod:`cosmosdb_datasource.stream` is imported,
the reader gets a placeholder base and Spark treats it as a source without native AvailableNow
support, which can silently run a single micro-batch per ``Trigger.AvailableNow`` run. That is
expected on PySpark 4.0/4.1, which never call the admission-control protocol. If the protocol *is*
called with the placeholder in place, :func:`warn_if_available_now_unsupported` logs a WARNING once
per process, naming the streaming modules that are loaded.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
import logging
import sys
from types import ModuleType
from typing import Any

from pyspark.sql.types import StructType

from cosmosdb_datasource.errors import CosmosConfigError

logger = logging.getLogger("cosmosdb_datasource")

ENGINE_PACKAGE = "_engine_pyspark"
ENGINE_STREAMING_MODULE = "_engine_pyspark.sql.streaming.datasource"
CLIENT_PACKAGE = "pyspark"
CLIENT_STREAMING_MODULE = "pyspark.sql.streaming.datasource"


@dataclasses.dataclass
class ReadAllAvailable:
    """Stand-in for ``pyspark.sql.streaming.datasource.ReadAllAvailable`` (same name and fields)."""


@dataclasses.dataclass
class ReadMaxRows:
    """Stand-in for ``pyspark.sql.streaming.datasource.ReadMaxRows`` (same name and fields)."""

    max_rows: int


def streaming_api_module() -> ModuleType | None:
    """Return the module defining the streaming admission-control API in the PySpark that runs the reader.

    The engine module is preferred only when the engine package is already loaded (inside Databricks
    data source workers); importing it anywhere else is avoided.
    """
    if ENGINE_PACKAGE in sys.modules:
        try:
            return importlib.import_module(ENGINE_STREAMING_MODULE)
        except ImportError:
            pass
    try:
        return importlib.import_module(CLIENT_STREAMING_MODULE)
    except ImportError:  # PySpark 4.0 / 4.1
        return None


class NoTriggerAvailableNow:
    """Placeholder base: without the mixin Spark handles Trigger.AvailableNow itself."""


def available_now_base() -> type:
    """Return the running engine's ``SupportsTriggerAvailableNow`` or the :class:`NoTriggerAvailableNow` placeholder."""
    module = streaming_api_module()
    base = getattr(module, "SupportsTriggerAvailableNow", None) if module is not None else None
    if isinstance(base, type):
        return base
    return NoTriggerAvailableNow


def describe_streaming_modules() -> str:
    """Describe which PySpark builds and streaming API modules are loaded (for diagnostics; no imports)."""
    parts = []
    for name in (ENGINE_PACKAGE, ENGINE_STREAMING_MODULE, CLIENT_PACKAGE, CLIENT_STREAMING_MODULE):
        module = sys.modules.get(name)
        if module is None:
            parts.append(f"{name}: not loaded")
            continue
        details = []
        version = getattr(module, "__version__", None)
        if version:
            details.append(f"version {version}")
        if name.endswith(".datasource"):
            mixin = isinstance(getattr(module, "SupportsTriggerAvailableNow", None), type)
            details.append("has SupportsTriggerAvailableNow" if mixin else "no SupportsTriggerAvailableNow")
        parts.append(f"{name}: loaded" + (f" ({', '.join(details)})" if details else ""))
    return "; ".join(parts)


_warned_available_now = False


def warn_if_available_now_unsupported(base: type, caller: str) -> bool:
    """Log a WARNING once per process if Spark speaks admission control to a placeholder-based reader.

    Return True if this call logged it.
    """
    global _warned_available_now
    if _warned_available_now or base is not NoTriggerAvailableNow:
        return False
    _warned_available_now = True
    logger.warning(
        "cosmosdb: Spark called %s (the PySpark >= 4.2 admission-control protocol), but no "
        "SupportsTriggerAvailableNow class was found when cosmosdb_datasource.stream was imported, so "
        "the stream reader does not implement native Trigger.AvailableNow. Spark may then run only one "
        "micro-batch per AvailableNow trigger (later triggers continue where it stopped). Modules: %s.",
        caller,
        describe_streaming_modules(),
    )
    return True


def read_limit(max_rows: int | None) -> Any:
    """Return ``ReadMaxRows(max_rows)`` or ``ReadAllAvailable()`` using the running engine's classes."""
    module = streaming_api_module()
    if max_rows:
        cls = getattr(module, "ReadMaxRows", None) if module is not None else None
        return (cls or ReadMaxRows)(max_rows)
    cls = getattr(module, "ReadAllAvailable", None) if module is not None else None
    return (cls or ReadAllAvailable)()


# Spark's planner for reads with filter pushdown (``UserDefinedPythonDataSourceFilterPushdownRunner``),
# under ``pyspark`` or the Databricks engine package. Spark only runs it while
# spark.sql.python.filterPushdown.enabled is true; otherwise it plans with plan_data_source_read.
PUSHDOWN_PLANNER_MODULE_SUFFIX = ".sql.worker.data_source_pushdown_filters"


def filter_pushdown_planner() -> bool:
    """Return True if the call comes from Spark's filter-pushdown planner (inspects the call stack)."""
    frame = sys._getframe(1)
    while frame is not None:
        module = frame.f_globals
        names = (module.get("__name__"), getattr(module.get("__spec__"), "name", None))
        if any(isinstance(n, str) and n.endswith(PUSHDOWN_PLANNER_MODULE_SUFFIX) for n in names):
            return True
        frame = frame.f_back  # type: ignore[assignment]
    return False


def engine_loaded() -> bool:
    """Return True inside Databricks data source workers, where the engine's PySpark drives the reader."""
    return ENGINE_PACKAGE in sys.modules


def arrow_schema(schema: StructType) -> Any:
    """Return the Arrow schema PySpark's read worker derives from ``schema`` (``records_to_arrow_batches``)."""
    from pyspark.sql.pandas.types import to_arrow_schema

    # 4.2 calls to_arrow_schema(return_type, timezone="UTC"); 4.0 and 4.1 have no timezone parameter
    # and call to_arrow_schema(return_type), which gives timestamps in UTC as well.
    if "timezone" in inspect.signature(to_arrow_schema).parameters:
        return to_arrow_schema(schema, timezone="UTC")
    return to_arrow_schema(schema)


def local_struct_type(schema: Any) -> StructType:
    """Return ``schema`` as a ``pyspark.sql.types.StructType`` of the PySpark imported here."""
    if isinstance(schema, StructType):
        return schema
    to_json = getattr(schema, "jsonValue", None)
    if not callable(to_json) or type(schema).__name__ != "StructType":
        raise CosmosConfigError(
            f"Expected a StructType schema, got {type(schema).__module__}.{type(schema).__qualname__}."
        )
    try:
        converted = StructType.fromJson(to_json())
    except Exception as exc:
        raise CosmosConfigError(
            f"Could not convert the schema passed by Spark ({type(schema).__module__}) to this PySpark's types: {exc}"
        ) from exc
    return converted
