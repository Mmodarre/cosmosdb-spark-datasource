"""Regression tests for Databricks serverless, where the engine PySpark differs from ``pyspark``.

Observed on serverless: data source workers run ``_engine_pyspark`` 4.4.0.dev0 while
``import pyspark`` resolves to the environment's client PySpark 4.0.0. The streaming runner then

* passes an engine ``StructType`` to ``DataSource.streamReader()`` (left unconverted, it fails with
  ``Spark type string is not supported by the cosmosdb source``), and
* speaks the 4.2+ admission-control protocol: ``getDefaultReadLimit()`` serialized as
  ``dataclasses.asdict(limit) | {"_type": name}``, ``latestOffset(start, engine_limit)``,
  ``reportLatestOffset()`` (``AttributeError`` unless the reader defines it) and
  ``isinstance(reader, engine SupportsTriggerAvailableNow)``.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import json
import logging
import os
import subprocess
import sys
import textwrap
import types
from pathlib import Path

import pyspark.sql.types as client_types
import pytest

from cosmosdb_datasource import _compat
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import TEST_HOOKS_ENV
from cosmosdb_datasource.schema import batch_schema, change_feed_schema
from tests.conftest import fake_options
from tests.harness import StreamHarness, make_source, validate_rows

ROOT = Path(__file__).resolve().parents[1]


def foreign_types_module() -> types.ModuleType:
    """Load a second, independent copy of ``pyspark.sql.types`` (like ``_engine_pyspark.sql.types``)."""
    spec = importlib.util.spec_from_file_location("_foreign_engine_types", client_types.__file__)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load(store, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 13}", "v": i})


def test_foreign_struct_type_is_really_foreign() -> None:
    foreign = foreign_types_module()
    schema = foreign._parse_datatype_json_string(change_feed_schema().json())
    assert not isinstance(schema, client_types.StructType)
    assert not isinstance(schema.fields[0].dataType, client_types.StringType)


def test_stream_reader_accepts_engine_struct_type(make_store) -> None:
    store = make_store(partitions=2)
    load(store, 40)
    foreign = foreign_types_module()
    engine_schema = foreign._parse_datatype_json_string(change_feed_schema().json())
    ds = make_source(fake_options(store, change_feed_mode="LatestVersion"))
    reader = ds.streamReader(engine_schema)
    assert isinstance(reader._schema, client_types.StructType)
    assert reader._schema == change_feed_schema()

    h = StreamHarness(fake_options(store, change_feed_mode="LatestVersion"), schema=engine_schema)
    h.schema = change_feed_schema()  # rows are validated against the client's types
    h.start()
    rows = h.run_until_idle()
    assert sorted(h.column(rows, "id")) == sorted(f"d{i}" for i in range(40))


def test_batch_reader_accepts_engine_struct_type(make_store) -> None:
    store = make_store(partitions=1)
    load(store, 10)
    foreign = foreign_types_module()
    engine_schema = foreign._parse_datatype_json_string(batch_schema().json())
    ds = make_source(fake_options(store))
    reader = ds.reader(engine_schema)
    assert reader._schema == batch_schema()
    rows = [row for part in reader.partitions() for row in reader.read(part)]
    validate_rows(rows, batch_schema(), ds)  # the engine validates with its own (equivalent) types
    assert sorted(r[0] for r in rows) == sorted(f"d{i}" for i in range(10))


def test_engine_batch_schema_on_stream_still_rejected(make_store) -> None:
    store = make_store(partitions=1)
    foreign = foreign_types_module()
    engine_schema = foreign._parse_datatype_json_string(batch_schema().json())
    ds = make_source(fake_options(store))
    with pytest.raises(CosmosConfigError, match="needs the change-feed schema"):
        ds.streamReader(engine_schema)


def test_local_struct_type_rejects_non_struct() -> None:
    with pytest.raises(CosmosConfigError, match="Expected a StructType"):
        _compat.local_struct_type(client_types.StringType())
    with pytest.raises(CosmosConfigError, match="Expected a StructType"):
        _compat.local_struct_type("id STRING")


def test_read_limits_without_any_streaming_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """Client PySpark 4.0 and no engine: stand-ins with the wire format the engine expects."""
    monkeypatch.setattr(_compat, "streaming_api_module", lambda: None)
    limit = _compat.read_limit(500)
    assert type(limit).__name__ == "ReadMaxRows" and dataclasses.asdict(limit) == {"max_rows": 500}
    assert type(_compat.read_limit(None)).__name__ == "ReadAllAvailable"
    assert dataclasses.asdict(_compat.read_limit(None)) == {}
    base = _compat.available_now_base()
    assert not hasattr(base, "prepareForTriggerAvailableNow")


def test_placeholder_base_warns_once_when_admission_control_is_used(make_store, monkeypatch, caplog) -> None:
    """The engine speaks admission control, but no SupportsTriggerAvailableNow was found at import."""
    from cosmosdb_datasource import stream as stream_module

    monkeypatch.setattr(_compat, "_warned_available_now", False)
    monkeypatch.setattr(stream_module, "_AvailableNowBase", _compat.NoTriggerAvailableNow)
    store = make_store(partitions=1)
    load(store, 5)
    h = StreamHarness(
        fake_options(store, change_feed_mode="LatestVersion", max_items_per_trigger=5), admission_control=True
    )
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        limit = h.planner.getDefaultReadLimit()
        h.start()
        h.run_batch(limit)
        h.planner.getDefaultReadLimit()
    warnings = [r for r in caplog.records if "SupportsTriggerAvailableNow" in r.getMessage()]
    assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING
    message = warnings[0].getMessage()
    assert "getDefaultReadLimit()" in message and "one micro-batch per AvailableNow trigger" in message
    for name in ("_engine_pyspark", _compat.ENGINE_STREAMING_MODULE, "pyspark", _compat.CLIENT_STREAMING_MODULE):
        assert f"{name}: " in message
    assert "_engine_pyspark: not loaded" in message


def test_no_available_now_warning_with_a_real_mixin(make_store, monkeypatch, caplog) -> None:
    monkeypatch.setattr(_compat, "_warned_available_now", False)
    with caplog.at_level(logging.WARNING, logger="cosmosdb_datasource"):
        assert _compat.warn_if_available_now_unsupported(type("Mixin", (), {}), "getDefaultReadLimit()") is False
        assert _compat.warn_if_available_now_unsupported(_compat.NoTriggerAvailableNow, "x") is True
        assert _compat.warn_if_available_now_unsupported(_compat.NoTriggerAvailableNow, "x") is False
    assert len(caplog.records) == 1


def test_describe_streaming_modules_reports_a_loaded_engine(monkeypatch) -> None:
    fakes = _fake_engine_modules()
    for name, module in fakes.items():
        monkeypatch.setitem(sys.modules, name, module)
    text = _compat.describe_streaming_modules()
    assert f"{_compat.ENGINE_STREAMING_MODULE}: loaded (has SupportsTriggerAvailableNow)" in text
    monkeypatch.delattr(fakes[_compat.ENGINE_STREAMING_MODULE], "SupportsTriggerAvailableNow")
    assert f"{_compat.ENGINE_STREAMING_MODULE}: loaded (no SupportsTriggerAvailableNow)" in (
        _compat.describe_streaming_modules()
    )


def _fake_engine_modules() -> dict[str, types.ModuleType]:
    pkg = types.ModuleType("_engine_pyspark")
    pkg.__path__ = []  # type: ignore[attr-defined]
    sql = types.ModuleType("_engine_pyspark.sql")
    sql.__path__ = []  # type: ignore[attr-defined]
    streaming = types.ModuleType("_engine_pyspark.sql.streaming")
    streaming.__path__ = []  # type: ignore[attr-defined]
    api = types.ModuleType(_compat.ENGINE_STREAMING_MODULE)

    @dataclasses.dataclass
    class ReadMaxRows:
        max_rows: int

    class SupportsTriggerAvailableNow:
        pass

    api.ReadMaxRows = ReadMaxRows  # type: ignore[attr-defined]
    api.SupportsTriggerAvailableNow = SupportsTriggerAvailableNow  # type: ignore[attr-defined]
    return {
        "_engine_pyspark": pkg,
        "_engine_pyspark.sql": sql,
        "_engine_pyspark.sql.streaming": streaming,
        _compat.ENGINE_STREAMING_MODULE: api,
    }


def test_streaming_api_prefers_loaded_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    fakes = _fake_engine_modules()
    for name, module in fakes.items():
        monkeypatch.setitem(sys.modules, name, module)
    api = fakes[_compat.ENGINE_STREAMING_MODULE]
    assert _compat.streaming_api_module() is api
    assert type(_compat.read_limit(7)) is api.ReadMaxRows
    assert _compat.available_now_base() is api.SupportsTriggerAvailableNow
    # ReadAllAvailable missing from the engine module -> stand-in with the same name
    assert type(_compat.read_limit(None)).__name__ == "ReadAllAvailable"


def test_streaming_api_ignores_engine_when_not_loaded(monkeypatch: pytest.MonkeyPatch) -> None:
    # The engine's streaming module is importable (in sys.modules) but the engine package itself
    # is not loaded: only the ENGINE_PACKAGE guard keeps the engine module from being chosen.
    fakes = _fake_engine_modules()
    for name, module in fakes.items():
        if name != "_engine_pyspark":
            monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.delitem(sys.modules, "_engine_pyspark", raising=False)
    module = _compat.streaming_api_module()
    assert module is not fakes[_compat.ENGINE_STREAMING_MODULE]
    assert module is None or module.__name__ == _compat.CLIENT_STREAMING_MODULE


def test_report_latest_offset(make_store) -> None:
    store = make_store(partitions=3)
    load(store, 30)
    h = StreamHarness(fake_options(store, change_feed_mode="LatestVersion", max_items_per_trigger=5))
    assert h.planner.reportLatestOffset() is None
    h.start()
    end = json.loads(h.latest())
    reported = h.planner.reportLatestOffset()
    json.dumps(reported)
    # reports the probed head (not the capped end) and never less than the end offset
    head = reported["ranges"]
    capped = end["ranges"]
    assert set(head) == set(capped)
    assert all(head[k] >= capped[k] for k in head)
    assert sum(head.values()) > sum(capped.values())


_ENGINE_STREAMING_SOURCE = """
from abc import ABC, abstractmethod
from dataclasses import dataclass

class ReadLimit: ...

@dataclass
class ReadAllAvailable(ReadLimit): ...

@dataclass
class ReadMaxRows(ReadLimit):
    max_rows: int

class SupportsTriggerAvailableNow(ABC):
    @abstractmethod
    def prepareForTriggerAvailableNow(self) -> None: ...
"""

_SIMULATION = r"""
import dataclasses, importlib.util, inspect, json, os, pickle, sys

# 1. Client PySpark looks like 4.0: no streaming admission-control module, no 4.2 base methods.
from pyspark.sql import datasource as client_ds   # (4.2's module imports the streaming API itself)
sys.modules["pyspark.sql.streaming.datasource"] = None
for name in ("getDefaultReadLimit", "reportLatestOffset"):
    if name in vars(client_ds.DataSourceStreamReader):
        delattr(client_ds.DataSourceStreamReader, name)

# 2. The engine package is loaded first, as in a Databricks data source worker.
import _engine_pyspark.sql.streaming.datasource as engine
import pyspark.sql.types as client_types
spec = importlib.util.spec_from_file_location("_engine_types", client_types.__file__)
engine_types = importlib.util.module_from_spec(spec); spec.loader.exec_module(engine_types)

from pyspark import cloudpickle
from pyspark.sql.datasource import CaseInsensitiveDict
from cosmosdb_datasource import CosmosDBDataSource
from cosmosdb_datasource.schema import change_feed_schema
from tests.fake_cosmos import FakeCosmosStore
from tests.conftest import fake_options

store = FakeCosmosStore("engine_sim", partitions=3).register()
for i in range(200):
    store.upsert({"id": f"d{i}", "pk": f"p{i % 17}", "v": i})

extra = json.loads(os.environ.get("SIM_OPTIONS", "{}"))
ds = cloudpickle.loads(cloudpickle.dumps(CosmosDBDataSource(CaseInsensitiveDict(
    fake_options(store, change_feed_mode="LatestVersion", max_items_per_trigger=40, **extra)))))
engine_schema = engine_types._parse_datatype_json_string(change_feed_schema().json())
reader = ds.streamReader(schema=engine_schema)          # runner: _streamReader(data_source, schema)
executor_blob = cloudpickle.dumps(ds.streamReader(schema=engine_schema))

# check_support_func
admission_control = len(inspect.signature(reader.latestOffset).parameters) > 0
available_now = isinstance(reader, engine.SupportsTriggerAvailableNow)
registry = {c.__name__: c for c in (engine.ReadAllAvailable, engine.ReadMaxRows)}

def default_limit():                                    # get_default_read_limit_func
    limit = reader.getDefaultReadLimit()
    wire = json.loads(json.dumps(dataclasses.asdict(limit) | {"_type": limit.__class__.__name__}))
    cls = registry[wire.pop("_type")]
    return cls(**wire)

reader.prepareForTriggerAvailableNow()                  # AvailableNow target captured here
for i in range(200, 230):                               # arrives after the target
    store.upsert({"id": f"d{i}", "pk": f"p{i % 17}", "v": i})

start = json.dumps(reader.initialOffset())
batches, ids, operations = 0, [], []
while True:
    limit = default_limit()
    end = json.dumps(reader.latestOffset(json.loads(start), limit))   # latest_offset_admission_control_func
    report = reader.reportLatestOffset()                               # report_latest_offset_func
    json.dumps(report)
    if end == start:
        break
    parts = reader.partitions(json.loads(start), json.loads(end))
    for p in parts:
        executor = cloudpickle.loads(executor_blob)
        for row in executor.read(pickle.loads(pickle.dumps(p))):
            ids.append(row[0])
            operations.append(row[2])
    reader.commit(json.loads(end))
    start = end
    batches += 1
    assert batches < 100
reader.stop()
print(json.dumps({"admission_control": admission_control, "available_now": available_now,
                  "limit": type(limit).__module__ + "." + type(limit).__name__, "max_rows": limit.max_rows,
                  "batches": batches, "ids": len(ids), "unique": len(set(ids)),
                  "snapshot_rows": operations.count("snapshot"),
                  "change_ids": sorted(i for i, op in zip(ids, operations) if op != "snapshot"),
                  "expected": sorted(ids) == sorted(f"d{i}" for i in range(200))}))
"""


def _simulate_engine(tmp_path: Path, options: dict[str, str] | None = None) -> tuple[dict, str]:
    pkg = tmp_path / "_engine_pyspark"
    (pkg / "sql" / "streaming").mkdir(parents=True)
    for init in (pkg, pkg / "sql", pkg / "sql" / "streaming"):
        (init / "__init__.py").write_text("")
    (pkg / "sql" / "streaming" / "datasource.py").write_text(textwrap.dedent(_ENGINE_STREAMING_SOURCE))
    env = dict(
        os.environ,
        PYTHONPATH=os.pathsep.join([str(tmp_path), str(ROOT)]),
        SIM_OPTIONS=json.dumps(options or {}),
        **{TEST_HOOKS_ENV: "1"},
    )
    proc = subprocess.run(
        [sys.executable, "-c", _SIMULATION], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    return json.loads(proc.stdout.strip().splitlines()[-1]), proc.stderr


def test_engine_admission_control_protocol_with_client_pyspark_40(tmp_path: Path) -> None:
    """Replays the Databricks serverless streaming runner in a fresh interpreter (see module doc)."""
    result, stderr = _simulate_engine(tmp_path)
    assert "SupportsTriggerAvailableNow class was found" not in stderr  # the engine mixin was used
    assert result["admission_control"] is True
    assert result["available_now"] is True
    assert result["limit"] == "_engine_pyspark.sql.streaming.datasource.ReadMaxRows"
    assert result["max_rows"] == 40
    assert result["batches"] > 1
    assert result["ids"] == result["unique"] == 200
    assert result["expected"] is True
    assert result["snapshot_rows"] == 0


@pytest.mark.parametrize("start_from", ["Beginning", "Now"])
def test_engine_protocol_available_now_with_initial_snapshot(tmp_path: Path, start_from: str) -> None:
    """AvailableNow under the engine protocol: uncapped snapshot in batch 0, target reached, run terminates.

    The simulation captures the target after 200 documents, before ``initialOffset()`` (as Spark does), and
    writes 30 more documents in between, so the snapshot holds all 230 documents. After the snapshot the
    target moves once to the current position: from Beginning the change feed then delivers d0..d229 in
    capped batches; from Now (P0 after the 30 writes) nothing follows.
    """
    options = {"initial_snapshot": "true", "initial_snapshot_delay_seconds": "0", "start_from": start_from}
    result, _stderr = _simulate_engine(tmp_path, options)
    assert result["available_now"] is True
    assert result["snapshot_rows"] == result["unique"] == 230
    if start_from == "Now":
        assert result["batches"] == 1 and result["change_ids"] == []
    else:
        assert result["change_ids"] == sorted(f"d{i}" for i in range(230))
        assert result["batches"] >= 1 + 230 // 40
