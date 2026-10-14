"""Bridge between the PySpark build that *runs* the data source and the one user code imports.

On Databricks serverless (verified 2026-10-14, Spark 4.2.0 server, environment version 4) the
Python data source workers run the engine's own PySpark, shipped as the package
``_engine_pyspark`` (``__version__`` 4.4.0.dev0), while ``import pyspark`` in the *same* process
resolves to the environment's client PySpark (databricks-connect, 4.0.0). Classes therefore exist
twice and ``isinstance`` across the two copies is false. Two consequences are handled here:

1. **Schemas.** The engine's batch planner converts the output schema to the client's classes
   before calling ``DataSource.reader()``, but its streaming source runner passes an *engine*
   ``StructType`` to ``DataSource.streamReader()``. :func:`local_struct_type` converts any foreign
   ``StructType`` through its JSON form so the type checks in :mod:`cosmosdb_datasource.schema`
   work.
2. **Admission control.** The engine uses the PySpark 4.2+ protocol for every reader whose
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
    """The module defining the streaming admission-control API in the PySpark that runs us.

    The engine module is preferred, but only when the engine package is already loaded: that is
    the case inside Databricks data source workers, and importing it anywhere else is avoided.
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
    """``SupportsTriggerAvailableNow`` of the running engine, or the :class:`NoTriggerAvailableNow` placeholder."""
    module = streaming_api_module()
    base = getattr(module, "SupportsTriggerAvailableNow", None) if module is not None else None
    if isinstance(base, type):
        return base
    return NoTriggerAvailableNow


def describe_streaming_modules() -> str:
    """Which PySpark builds and streaming API modules are loaded (for diagnostics; no imports)."""
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
    """Log a WARNING (once per process) if Spark speaks admission control to a placeholder-based reader.

    Returns True if the warning was logged by this call.
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
    """``ReadMaxRows(max_rows)`` or ``ReadAllAvailable()`` using the running engine's classes."""
    module = streaming_api_module()
    if max_rows:
        cls = getattr(module, "ReadMaxRows", None) if module is not None else None
        return (cls or ReadMaxRows)(max_rows)
    cls = getattr(module, "ReadAllAvailable", None) if module is not None else None
    return (cls or ReadAllAvailable)()


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
