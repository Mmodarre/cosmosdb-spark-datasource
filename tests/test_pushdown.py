"""Filter pushdown: query parsing, translation, and equivalence with Spark's own filtering."""

from __future__ import annotations

import json
import random
import types
from dataclasses import dataclass
from typing import Any

import pytest
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from cosmosdb_datasource.batch import CosmosBatchReader
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.pushdown import CosmosPushdownBatchReader, Translator, parse_simple_query
from tests.conftest import fake_options
from tests.fake_query import compile_query
from tests.harness import make_source, run_batch, run_batch_with_filters

# Stand-ins with the names and fields of pyspark.sql.datasource's filters (PySpark >= 4.1), which the
# translator dispatches on by name, as for the Databricks engine's own classes. And and Or are not sent
# by Spark 4.1 or 4.2.


@dataclass(frozen=True)
class EqualTo:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class EqualNullSafe:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class GreaterThan:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class GreaterThanOrEqual:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class LessThan:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class LessThanOrEqual:
    attribute: tuple[str, ...]
    value: Any


@dataclass(frozen=True)
class In:
    attribute: tuple[str, ...]
    value: tuple[Any, ...]


@dataclass(frozen=True)
class IsNull:
    attribute: tuple[str, ...]


@dataclass(frozen=True)
class IsNotNull:
    attribute: tuple[str, ...]


@dataclass(frozen=True)
class StringStartsWith:
    attribute: tuple[str, ...]
    value: str


@dataclass(frozen=True)
class StringEndsWith:
    attribute: tuple[str, ...]
    value: str


@dataclass(frozen=True)
class StringContains:
    attribute: tuple[str, ...]
    value: str


@dataclass(frozen=True)
class Not:
    child: Any


@dataclass(frozen=True)
class And:
    left: Any
    right: Any


@dataclass(frozen=True)
class Or:
    left: Any
    right: Any


SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("pk", StringType()),
        StructField("_ts", LongType()),
        StructField("s", StringType()),
        StructField("n", LongType()),
        StructField("d", DoubleType()),
        StructField("b", BooleanType()),
        StructField("st", StructType([StructField("k", StringType()), StructField("m", LongType())])),
        StructField("body", StringType()),
    ]
)


def pushdown_options(store, **extra: str) -> dict[str, str]:
    return fake_options(store, filter_pushdown="true", **extra)


@pytest.mark.parametrize(
    ("query", "parts"),
    [
        ("SELECT * FROM c", ("SELECT * FROM c", "c", None)),
        ("select * from root r", ("select * from root r", "r", None)),
        ("SELECT * FROM c AS x WHERE x.a = 1", ("SELECT * FROM c AS x", "x", "x.a = 1")),
        ("SELECT * FROM c WHERE (c.a = 1) OR c.b", ("SELECT * FROM c", "c", "(c.a = 1) OR c.b")),
        (
            "SELECT * FROM c WHERE c.n = 'ORDER BY' AND c.m > 1",
            ("SELECT * FROM c", "c", "c.n = 'ORDER BY' AND c.m > 1"),
        ),
        (
            "SELECT * FROM c WHERE EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = 'x')",
            ("SELECT * FROM c", "c", "EXISTS(SELECT VALUE t FROM t IN c.tags WHERE t = 'x')"),
        ),
    ],
)
def test_simple_queries_are_parsed(query: str, parts: tuple[str, str, str | None]) -> None:
    parsed = parse_simple_query(query)
    assert parsed is not None
    assert (parsed.head, parsed.alias, parsed.where) == parts


@pytest.mark.parametrize(
    "query",
    [
        "SELECT c.id FROM c",
        "SELECT VALUE c FROM c",
        "SELECT TOP 10 * FROM c",
        "SELECT DISTINCT * FROM c",
        "SELECT * FROM c ORDER BY c._ts",
        "SELECT * FROM c WHERE c.a = 1 ORDER BY c._ts",
        "SELECT * FROM c JOIN t IN c.tags",
        "SELECT * FROM c WHERE c.a = 1 OFFSET 0 LIMIT 5",
        "SELECT * FROM c WHERE c.a = 1 -- comment",
        "SELECT * FROM c WHERE",
        "SELECT * FROM c GROUP BY c.a",
    ],
)
def test_other_queries_are_not_combined(query: str) -> None:
    assert parse_simple_query(query) is None


def translator(pk: PartitionKeyInfo | None = None, reserved: tuple[str, ...] = ()) -> Translator:
    return Translator(
        SCHEMA,
        "c",
        raw_column="body",
        corrupt_column="_corrupt_record",
        partition_key=lambda: pk or PartitionKeyInfo(paths=("/pk",), kind="Hash", version=2),
        reserved_names=reserved,
    )


@pytest.mark.parametrize(
    ("flt", "sql", "exact"),
    [
        (EqualTo(("id",), "d1"), 'c["id"] = @__f0', True),
        (Not(EqualTo(("id",), "d1")), 'NOT (c["id"] = @__f0)', True),
        (GreaterThanOrEqual(("_ts",), 5), 'c["_ts"] >= @__f0', True),
        (In(("id",), ("a", "b")), 'ARRAY_CONTAINS(@__f0, c["id"])', True),
        (StringStartsWith(("id",), "d"), 'STARTSWITH(c["id"], @__f0)', True),
        (IsNotNull(("id",)), None, True),
        (IsNull(("id",)), "false", True),
        (EqualTo(("s",), "x"), 'c["s"] = @__f0 OR NOT IS_STRING(c["s"])', False),
        (Not(EqualTo(("s",), "x")), 'NOT (IS_STRING(c["s"]) AND c["s"] = @__f0)', False),
        (LessThan(("n",), 3), 'c["n"] < @__f0 OR NOT IS_NUMBER(c["n"])', False),
        (EqualTo(("b",), True), 'c["b"] = @__f0 OR NOT IS_BOOL(c["b"])', False),
        (EqualTo(("st", "k"), "v"), 'c["st"]["k"] = @__f0 OR NOT IS_STRING(c["st"]["k"])', False),
        (StringContains(("s",), "x"), 'CONTAINS(c["s"], @__f0) OR NOT IS_STRING(c["s"])', False),
        (IsNull(("s",)), 'NOT IS_DEFINED(c["s"]) OR IS_NULL(c["s"])', False),
        (EqualTo(("pk",), "p1"), 'ARRAY_CONTAINS(@__f0, c["pk"])', True),
        (EqualTo(("pk",), "5"), 'ARRAY_CONTAINS(@__f0, c["pk"])', False),
        (GreaterThan(("pk",), "p"), 'c["pk"] > @__f0 OR NOT IS_STRING(c["pk"])', False),
    ],
)
def test_translation(flt: Any, sql: str | None, exact: bool) -> None:
    pred = translator().bounds(flt).true
    assert (pred.sql, pred.exact) == (sql, exact)


@pytest.mark.parametrize(
    "flt",
    [
        IsNotNull(("s",)),  # removes only explicit nulls: not worth a predicate
        IsNull(("n",)),  # a failed conversion is NULL too
        IsNull(("st", "k")),  # a sibling that fails conversion makes the struct NULL
        EqualTo(("n",), 2**53),  # not exactly comparable as a double
        GreaterThan(("s",), "\ue000"),  # code-unit and code-point order may differ
        GreaterThan(("b",), False),
        EqualTo(("body",), "{}"),
        EqualTo(("missing",), 1),
        EqualTo(("st",), {"k": "v"}),
        In(("s",), (None,)),
        EqualTo(("s",), None),
    ],
)
def test_untranslatable_filters(flt: Any) -> None:
    assert translator().bounds(flt).true.sql is None


def test_parameter_names_avoid_user_parameters() -> None:
    t = translator(reserved=("@__f0",))
    assert t.bounds(EqualTo(("id",), "x")).true.sql == 'c["id"] = @__f_0'
    assert t.parameters == [{"name": "@__f_0", "value": "x"}]


def test_pk_candidates_and_hierarchical_keys() -> None:
    t = translator()
    t.bounds(In(("pk",), ("a", "5", "true", "null")))
    assert t.parameters[0]["value"] == ["a", "5", 5, "true", True, "null", None]
    for text in ("NaN", "Infinity", "-Infinity"):  # not JSON values: only a string key renders like that
        t = translator()
        pred = t.bounds(EqualTo(("pk",), text)).true
        assert pred.exact and t.parameters[0]["value"] == [text]
        json.dumps(t.parameters, allow_nan=False)
    t = translator()
    pred = t.bounds(EqualTo(("pk",), '{"o":1}')).true  # an object key renders as NULL, not as its JSON
    assert pred.exact and t.parameters[0]["value"] == ['{"o":1}']
    t = translator()
    pred = t.bounds(EqualTo(("pk",), "[1,2]")).true  # an array key renders as its JSON: Spark re-checks
    assert not pred.exact and t.parameters[0]["value"] == ["[1,2]", [1, 2]]
    assert translator().bounds(IsNull(("pk",))).true.sql == 'NOT IS_DEFINED(c["pk"]) OR IS_OBJECT(c["pk"])'
    hierarchical = translator(PartitionKeyInfo(paths=("/t", "/u"), kind="MultiHash", version=2))
    assert hierarchical.bounds(EqualTo(("pk",), '["a","b"]')).true.sql is None


def test_and_or_not_combine_bounds() -> None:
    t = translator()
    both = t.bounds(And(EqualTo(("id",), "a"), EqualTo(("s",), "x"))).true
    assert both.sql == '(c["id"] = @__f0) AND (c["s"] = @__f1 OR NOT IS_STRING(c["s"]))' and not both.exact
    either = t.bounds(Or(EqualTo(("id",), "a"), EqualTo(("missing",), 1))).true
    assert either.sql is None and not either.exact  # one side unknown: no condition
    negated = t.bounds(Not(Or(EqualTo(("id",), "a"), EqualTo(("id",), "b")))).true
    assert negated.sql == '(NOT (c["id"] = @__f3)) AND (NOT (c["id"] = @__f4))' and negated.exact


def test_fake_where_semantics() -> None:
    def returns(where: str, doc: dict, params: dict | None = None) -> bool:
        p = [{"name": k, "value": v} for k, v in (params or {}).items()]
        return compile_query(f"SELECT * FROM c WHERE {where}", p)(doc)

    assert returns("c.a = 1", {"a": 1.0})
    assert not returns("c.a = '1'", {"a": 1})
    assert not returns("NOT (c.a = '1')", {"a": 1})  # undefined, not true
    assert returns("c.a = '1' OR NOT IS_STRING(c.a)", {"a": 1})
    assert not returns("c.missing = 1 OR false", {})
    assert returns("NOT (IS_STRING(c.a) AND c.a = 'x')", {})
    assert returns('ARRAY_CONTAINS(@p, c["a"]["b"])', {"a": {"b": 2}}, {"@p": [1, 2]})
    assert returns("c.a IN ('x', 'y') AND STARTSWITH(c.a, 'y')", {"a": "yz"}) is False
    assert returns("c.a > 'B'", {"a": "a"})


def test_reader_selection(make_store) -> None:
    store = make_store()
    default = make_source(fake_options(store))
    auto = make_source(fake_options(store, filter_pushdown="auto"))
    schema = default.schema()
    assert type(default.reader(schema)) is CosmosBatchReader
    assert type(auto.reader(schema)) is CosmosBatchReader
    assert isinstance(make_source(pushdown_options(store)).reader(schema), CosmosPushdownBatchReader)

    # Spark's pushdown planner calls reader() from pyspark.sql.worker.data_source_pushdown_filters (or
    # the Databricks engine's copy); only then does "auto" return the reader with pushFilters.
    planner = types.ModuleType("_engine_pyspark.sql.worker.data_source_pushdown_filters")
    exec("def plan(ds, schema):\n    return ds.reader(schema)\n", planner.__dict__)
    assert isinstance(planner.plan(auto, schema), CosmosPushdownBatchReader)
    assert type(planner.plan(default, schema)) is CosmosBatchReader
    run_as_main = types.ModuleType("__main__")
    run_as_main.__spec__ = types.SimpleNamespace(name="pyspark.sql.worker.data_source_pushdown_filters")
    exec("def plan(ds, schema):\n    return ds.reader(schema)\n", run_as_main.__dict__)
    assert isinstance(run_as_main.plan(auto, schema), CosmosPushdownBatchReader)
    with pytest.raises(CosmosConfigError, match="filter_pushdown"):
        make_source(fake_options(store, filter_pushdown="maybe"))


def load_typed(store) -> None:
    for i in range(60):
        store.upsert(
            {
                "id": f"d{i:02d}",
                "pk": f"p{i % 7}",
                "s": ["a", "b", "c"][i % 3],
                "n": i,
                "st": {"k": f"k{i % 4}", "m": i},
            }
        )


def test_pushed_filters_reduce_the_documents_read(make_store) -> None:
    store = make_store(partitions=3)
    load_typed(store)
    filters = [IsNotNull(("s",)), EqualTo(("s",), "b"), GreaterThanOrEqual(("_ts",), store.clock - 30)]
    rows, _parts, remaining = run_batch_with_filters(pushdown_options(store), SCHEMA, filters)
    served = sum(e[4] for e in store.call_log if e[0] == "query_page")
    assert remaining == filters[:2]  # _ts is exact; the property filters are re-checked by Spark
    assert served == len(rows) == 10 and {r[3] for r in rows} == {"b"}


def test_user_query_and_parameters_are_combined(make_store) -> None:
    store = make_store(partitions=2)
    load_typed(store)
    opts = pushdown_options(
        store, query="SELECT * FROM root r WHERE r.n >= @low", query_parameters='[{"name": "@low", "value": 30}]'
    )
    rows, _parts, remaining = run_batch_with_filters(opts, SCHEMA, [EqualTo(("pk",), "p3"), LessThan(("n",), 50)])
    assert remaining == [LessThan(("n",), 50)]
    assert sorted(r[0] for r in rows) == ["d31", "d38", "d45"]
    # pk = 'p3' is exact: one logical partition, read routed by key
    assert [e[0] for e in store.call_log] == ["partition_query_page"] and store.call_log[0][2] == 3


def test_parameters_of_dropped_sub_filters_are_not_sent(make_store) -> None:
    store = make_store()
    load_typed(store)
    reader = make_source(pushdown_options(store)).reader(SCHEMA)
    filters = [Or(EqualTo(("id",), "d01"), EqualTo(("st",), {"k": "v"})), EqualTo(("id",), "d02")]
    assert list(reader.pushFilters(filters)) == filters[:1]
    query, parameters = reader._query()
    assert query == 'SELECT * FROM c WHERE (c["id"] = @__f1)'
    assert parameters == [{"name": "@__f1", "value": "d02"}]


def test_queries_that_cannot_be_combined_push_nothing(make_store) -> None:
    store = make_store()
    load_typed(store)
    filters = [EqualTo(("id",), "d01")]
    rows, _parts, remaining = run_batch_with_filters(
        pushdown_options(store, query="SELECT * FROM c ORDER BY c._ts"), SCHEMA, filters
    )
    assert remaining == filters and len(rows) == 60


def test_partition_key_is_read_only_for_pk_filters(make_store, no_sleep) -> None:
    store = make_store()
    load_typed(store)
    reader = make_source(pushdown_options(store)).reader(SCHEMA)
    store.throttle_calls["properties"] = 100  # any properties() call would be throttled
    assert list(reader.pushFilters([EqualTo(("id",), "d01")])) == []
    store.throttle_calls["properties"] = 0
    assert list(reader.pushFilters([EqualTo(("pk",), "p1")])) == []


# ----------------------------------------------------------------------------- equivalence with Spark

STRINGS = ["a", "ab", "b", "B", "", "5", "true", "null", "é", "\ue000x", "x\U0001f600"]
NUMBERS = [-1, 0, 2, 3, 2.0, 2.5, 1e20, 2**60]
DOUBLES = [-0.0, 0.0, 1.5, 2.0, 2, 1e20]
PKS = ["p1", "p2", "5", "5.0", " 5", "true", "null", "1e2"]


def _value(rng: random.Random, pool: list[Any]) -> Any:
    roll = rng.random()
    if roll < 0.55:
        return rng.choice(pool)
    return rng.choice(
        [None, True, False, 7, 2.0, "2", "x", {"k": "a"}, [1], "MISSING", rng.choice(STRINGS), rng.choice(NUMBERS)]
    )


def random_document(rng: random.Random, i: int) -> dict[str, Any]:
    doc: dict[str, Any] = {"id": f"d{i:03d}"}
    pk = rng.choice([*PKS, 5, 5.0, True, None, "MISSING"])
    if pk != "MISSING":
        doc["pk"] = pk
    for name, pool in (("s", STRINGS), ("n", NUMBERS), ("d", DOUBLES)):
        value = _value(rng, pool)
        if value == "MISSING":
            continue
        key = name.upper() if rng.random() < 0.1 else name  # read through the case-insensitive fallback
        doc[key] = value
    b = rng.choice([True, False, "true", "FALSE", 0, 1, None, "x", "MISSING"])
    if b != "MISSING":
        doc["b"] = b
    st = rng.choice(["obj", "obj", "obj", "string", None, "MISSING"])
    if st == "obj":
        inner: dict[str, Any] = {}
        k = _value(rng, ["k0", "k1", "K1"])
        if k != "MISSING":
            inner["K" if rng.random() < 0.1 else "k"] = k
        m = _value(rng, [1, 2, 3])
        if m != "MISSING":
            inner["m"] = m
        doc["st"] = inner
    elif st == "string":
        doc["st"] = "not an object"
    elif st is None:
        doc["st"] = None
    return doc


COLUMNS = {
    ("id",): [f"d{i:03d}" for i in range(0, 80, 7)] + ["d0"],
    ("pk",): PKS,
    ("_ts",): None,  # filled from the store clock
    ("s",): STRINGS,
    ("n",): [-1, 0, 2, 3, 2**60],
    ("d",): [-0.0, 1.5, 2.0, 1e20],
    ("b",): [True, False],
    ("st", "k"): ["k0", "k1", "K1", "a"],
    ("st", "m"): [1, 2, 3],
}
STRING_COLUMNS = [("id",), ("pk",), ("s",), ("st", "k")]


def random_leaf(rng: random.Random, ts_values: list[int]) -> Any:
    column = rng.choice(list(COLUMNS))
    pool = COLUMNS[column] or ts_values
    kind = rng.choice(["eq", "eq", "nullsafe", "range", "range", "in", "isnull", "isnotnull", "string"])
    if kind == "string" and column in STRING_COLUMNS:
        cls = rng.choice([StringStartsWith, StringEndsWith, StringContains])
        return cls(column, rng.choice(["a", "b", "5", "", "d0", "p", "k1", "tr"]))
    if kind == "isnull":
        return IsNull(column)
    if kind == "isnotnull":
        return IsNotNull(column)
    if kind == "in":
        values = tuple(rng.sample(pool, k=min(len(pool), rng.randint(1, 3))))
        return In(column, values + ((None,) if rng.random() < 0.2 else ()))
    value = rng.choice(pool)
    if column == ("b",) and kind == "range":
        kind = "eq"
    if kind == "range":
        return rng.choice([GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual])(column, value)
    return (EqualNullSafe if kind == "nullsafe" else EqualTo)(column, value)


def random_filter(rng: random.Random, ts_values: list[int], depth: int = 0) -> Any:
    roll = rng.random()
    if depth < 2 and roll < 0.15:
        return rng.choice([And, Or])(random_filter(rng, ts_values, depth + 1), random_filter(rng, ts_values, depth + 1))
    leaf = random_leaf(rng, ts_values)
    return Not(leaf) if roll > 0.7 else leaf


def _get(row: tuple, schema: StructType, path: tuple[str, ...]) -> Any:
    value: Any = row
    struct = schema
    for name in path:
        if value is None:
            return None
        index = struct.fieldNames().index(name)
        value = value[index]
        struct = struct.fields[index].dataType  # type: ignore[assignment]
    return value


def spark_eval(flt: Any, row: tuple) -> bool | None:
    """Spark SQL's three-valued semantics of a pushed filter on a converted row."""
    kind = type(flt).__name__
    if kind == "Not":
        inner = spark_eval(flt.child, row)
        return None if inner is None else not inner
    if kind in ("And", "Or"):
        a, b = spark_eval(flt.left, row), spark_eval(flt.right, row)
        if kind == "And":
            return False if False in (a, b) else (None if None in (a, b) else True)
        return True if True in (a, b) else (None if None in (a, b) else False)
    value = _get(row, SCHEMA, flt.attribute)
    if kind == "IsNull":
        return value is None
    if kind == "IsNotNull":
        return value is not None
    if kind == "EqualNullSafe":
        return (value is None and flt.value is None) or (value is not None and value == flt.value)
    if value is None:
        return None
    if kind == "In":
        if value in [v for v in flt.value if v is not None]:
            return True
        return None if None in flt.value else False
    ops = {
        "EqualTo": lambda a, b: a == b,
        "GreaterThan": lambda a, b: a > b,
        "GreaterThanOrEqual": lambda a, b: a >= b,
        "LessThan": lambda a, b: a < b,
        "LessThanOrEqual": lambda a, b: a <= b,
        "StringStartsWith": lambda a, b: a.startswith(b),
        "StringEndsWith": lambda a, b: a.endswith(b),
        "StringContains": lambda a, b: b in a,
    }
    return ops[kind](value, flt.value)


def test_pk_filters_on_keys_that_are_not_strings(make_store) -> None:
    store = make_store(partitions=2)
    keys = [{"o": 1}, [1, 2], "[1,2]", '{"o":1}', "NaN", 5, "5", None, ...]
    for i, key in enumerate(keys):
        store.upsert({"id": f"d{i}"} if key is ... else {"id": f"d{i}", "pk": key})
    all_rows, _ = run_batch(fake_options(store), SCHEMA)
    assert {r[0]: r[1] for r in all_rows}["d0"] is None  # an object key is the undefined key
    for flt in (
        EqualTo(("pk",), '{"o":1}'),
        EqualTo(("pk",), "[1,2]"),
        EqualTo(("pk",), "NaN"),
        In(("pk",), ("5", "[1,2]")),
        IsNull(("pk",)),
        IsNotNull(("pk",)),
        Not(EqualTo(("pk",), '{"o":1}')),
    ):
        expected = sorted(r[0] for r in all_rows if spark_eval(flt, r) is True)
        rows, _parts, remaining = run_batch_with_filters(pushdown_options(store), SCHEMA, [flt])
        assert sorted(r[0] for r in rows if all(spark_eval(f, r) is True for f in remaining)) == expected, flt


@pytest.mark.parametrize("seed", range(6))
@pytest.mark.parametrize("mode", ["PERMISSIVE", "DROPMALFORMED"])
def test_pushdown_returns_what_spark_filtering_returns(make_store, seed: int, mode: str) -> None:
    rng = random.Random(seed)
    store = make_store(partitions=3)
    store.clock_step = 0 if seed % 2 else 1
    for i in range(80):
        store.upsert(random_document(rng, i))
    ts_values = sorted({json.loads(r[-1])["_ts"] for r in run_batch(fake_options(store))[0]})
    all_rows, _ = run_batch(fake_options(store, mode=mode), SCHEMA)
    assert len(all_rows) > (40 if mode == "PERMISSIVE" else 15)
    pushed_any = 0
    for _ in range(25):
        filters = [random_filter(rng, ts_values) for _ in range(rng.randint(1, 3))]
        expected = sorted(repr(r) for r in all_rows if all(spark_eval(f, r) is True for f in filters))
        store.call_log.clear()
        rows, _parts, remaining = run_batch_with_filters(pushdown_options(store, mode=mode), SCHEMA, filters)
        got = sorted(repr(r) for r in rows if all(spark_eval(f, r) is True for f in remaining))
        assert got == expected, filters
        pages = [e for e in store.call_log if e[0] in ("query_page", "partition_query_page")]
        pushed_any += sum(e[-1] for e in pages) < len(all_rows)
    assert pushed_any > 5  # the filters did reduce what the service returned
