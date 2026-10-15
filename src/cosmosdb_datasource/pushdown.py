"""Filter pushdown for batch reads (``pushFilters``, PySpark >= 4.1).

Spark evaluates a filter on the *converted* column value, the service evaluates a WHERE clause on the raw
document. The two differ where conversion is not the identity: a STRING column renders a number as text,
a BIGINT column accepts integer strings, and a property is matched case-insensitively when the exact name
is missing. So for every filter the translator builds a predicate that every document whose row passes the
filter satisfies (a necessary condition), and pushes it into the query; the filter stays with Spark unless
the predicate is also sufficient. Necessary conditions guard each comparison with the JSON type it is exact
for, e.g. ``c["severity"] = @p OR NOT IS_STRING(c["severity"])``: documents with another type, or without
the exact property, are passed on to Spark. Against an account these guards cost next to nothing for a
property every document has (79 RU without, 81 RU with, for a 1% equality filter over 200,000 documents).

Exact (left to the service only) are comparisons on the system properties ``id``, ``_rid``, ``_self``,
``_etag`` (STRING columns) and ``_ts`` (a BIGINT column), which every document has with that type, and
equality on the ``pk`` column with a string that is not also JSON text of another value.

Spark 4.1 and 4.2 push a conjunction of leaf filters, each optionally negated: the JVM serializes only
EqualTo, EqualNullSafe, the four range comparisons, In, IsNull, IsNotNull and the three string matches
(``UserDefinedPythonDataSourceFilterPushdownRunner``), and never And or Or. Filters are dispatched by class
*name* and read by attribute, because on Databricks serverless they are instances of the engine's
classes (see :mod:`cosmosdb_datasource._compat`). And and Or with ``left``/``right`` children are
translated the same way.

The Python Data Source API passes the full output schema to ``reader()`` and has no column-pruning hook
(``PythonScanBuilder`` implements only ``SupportsPushDownFilters``), so projections are not pushed.
"""

from __future__ import annotations

import json
import logging
import math
import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from cosmosdb_datasource.batch import CosmosBatchReader
from cosmosdb_datasource.documents import PartitionKeyInfo, parse_path
from cosmosdb_datasource.schema import FEED_RANGE_COLUMN, PK_COLUMN

logger = logging.getLogger("cosmosdb_datasource")

# Integers beyond this cannot be compared exactly as the IEEE doubles the service compares.
_MAX_SAFE_INTEGER = 2**53
_SYSTEM_STRING_PROPERTIES = frozenset({"id", "_rid", "_self", "_etag"})
_INTEGRAL_TYPES = (ByteType, ShortType, IntegerType, LongType)
_COMPARISONS = {
    "EqualTo": "=",
    "EqualNullSafe": "=",
    "GreaterThan": ">",
    "GreaterThanOrEqual": ">=",
    "LessThan": "<",
    "LessThanOrEqual": "<=",
}
_STRING_FUNCTIONS = {"StringStartsWith": "STARTSWITH", "StringEndsWith": "ENDSWITH", "StringContains": "CONTAINS"}


@dataclass(frozen=True)
class Pred:
    """A WHERE predicate (``None``: no condition) and whether it is equivalent to, not only implied by, a filter."""

    sql: str | None
    exact: bool


UNKNOWN = Pred(None, False)
ALWAYS = Pred(None, True)
NEVER = Pred("false", True)


@dataclass(frozen=True)
class Bounds:
    """``true`` holds on every document whose row makes the filter true, ``false`` where it makes it false."""

    true: Pred
    false: Pred

    def negated(self) -> Bounds:
        return Bounds(self.false, self.true)


def _and(a: Pred, b: Pred) -> Pred:
    parts = [p.sql for p in (a, b) if p.sql is not None]
    sql = " AND ".join(f"({s})" for s in parts) if len(parts) > 1 else (parts[0] if parts else None)
    return Pred(sql, a.exact and b.exact)


def _or(a: Pred, b: Pred) -> Pred:
    if a.sql is None or b.sql is None:  # no condition on one side: none on the disjunction
        return Pred(None, (a.sql is None and a.exact) or (b.sql is None and b.exact))
    return Pred(f"({a.sql}) OR ({b.sql})", a.exact and b.exact)


_TOKEN = re.compile(r"""\s+|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|--|[A-Za-z_][A-Za-z0-9_]*|.""", re.DOTALL)
_REJECTED_KEYWORDS = frozenset({"JOIN", "ORDER", "GROUP", "OFFSET", "LIMIT", "SELECT"})


@dataclass(frozen=True)
class SimpleQuery:
    """``SELECT * FROM <container> [[AS] alias] [WHERE <condition>]``: the only form filters are added to."""

    head: str  # the query up to (excluding) WHERE
    alias: str
    where: str | None


def parse_simple_query(query: str) -> SimpleQuery | None:
    """Return the parts of ``query`` if it selects whole documents with at most a WHERE clause, else None."""
    tokens: list[tuple[int, str]] = []
    depth = 0
    for m in _TOKEN.finditer(query):
        text = m.group(0)
        if text.isspace():
            continue
        if text == "--":
            return None  # a comment could hide the rest of the query from this scan
        if depth == 0:
            tokens.append((m.start(), text))
        if text in ("(", "["):
            depth += 1
        elif text in (")", "]"):
            depth -= 1
    words = [t.upper() for _, t in tokens]
    if len(words) < 4 or words[0] != "SELECT" or words[1] != "*" or words[2] != "FROM":
        return None
    if not _is_identifier(tokens[3][1]):
        return None
    alias = tokens[3][1]
    i = 4
    if i < len(words) and words[i] == "AS":
        i += 1
    if i < len(words) and words[i] != "WHERE":
        if not _is_identifier(tokens[i][1]):
            return None
        alias = tokens[i][1]
        i += 1
    if i == len(words):
        return SimpleQuery(query.rstrip(), alias, None)
    if words[i] != "WHERE" or i + 1 == len(words):
        return None
    if any(w in _REJECTED_KEYWORDS for w in words[i + 1 :]):
        return None
    start = tokens[i][0]
    return SimpleQuery(query[:start].rstrip(), alias, query[start + len("WHERE") :].strip())


def _is_identifier(text: str) -> bool:
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text) is not None and text.upper() not in {
        "WHERE",
        "AS",
        "JOIN",
        "ORDER",
        "GROUP",
        "OFFSET",
        "LIMIT",
    }


@dataclass(frozen=True)
class _Target:
    expr: str  # SQL expression of the raw value, e.g. c["device"]["kind"]
    dtype: DataType
    kind: str  # "system", "pk" or "property"
    top_level: bool


def _field(struct: StructType, name: str) -> StructField | None:
    for f in struct.fields:
        if f.name == name:
            return f
    matches = [f for f in struct.fields if f.name.lower() == name.lower()]
    return matches[0] if len(matches) == 1 else None


def _path_expr(alias: str, segments: Sequence[str]) -> str:
    return alias + "".join(f"[{json.dumps(s, ensure_ascii=False)}]" for s in segments)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not a JSON value")


def _ordinal_safe(text: str) -> bool:
    # Below U+D800 UTF-16 code units (one possible service order) and code points (Spark's UTF-8 binary
    # order) agree, so a range comparison against such a literal orders every string the same way.
    return all(ord(ch) < 0xD800 for ch in text)


class Translator:
    """Translate Spark filters into WHERE predicates on documents read with ``alias``."""

    def __init__(
        self,
        schema: StructType,
        alias: str,
        *,
        raw_column: str,
        corrupt_column: str,
        partition_key: Callable[[], PartitionKeyInfo | None],
        reserved_names: Iterable[str] = (),
    ) -> None:
        self._schema = schema
        self._alias = alias
        self._raw_column = raw_column
        self._corrupt_column = corrupt_column
        self._partition_key = partition_key
        reserved = {n.lower() for n in reserved_names}
        prefix = "@__f"
        while any(n.startswith(prefix.lower()) for n in reserved):
            prefix += "_"
        self._prefix = prefix
        self.parameters: list[dict[str, Any]] = []

    def _param(self, value: Any) -> str:
        name = f"{self._prefix}{len(self.parameters)}"
        self.parameters.append({"name": name, "value": value})
        return name

    def bounds(self, flt: Any) -> Bounds:
        kind = type(flt).__name__
        if kind == "Not":
            return self.bounds(flt.child).negated()
        if kind in ("And", "Or"):
            left, right = self.bounds(flt.left), self.bounds(flt.right)
            if kind == "And":
                return Bounds(_and(left.true, right.true), _or(left.false, right.false))
            return Bounds(_or(left.true, right.true), _and(left.false, right.false))
        attribute = getattr(flt, "attribute", None)
        if not isinstance(attribute, (tuple, list)) or not all(isinstance(s, str) for s in attribute):
            return Bounds(UNKNOWN, UNKNOWN)
        target = self._resolve(tuple(attribute))
        if target is None:
            return Bounds(UNKNOWN, UNKNOWN)
        value = getattr(flt, "value", None)
        if kind in _COMPARISONS:
            if value is None:  # EqualNullSafe(col, None) is IS NULL; EqualTo(col, None) is never true
                return self._is_null(target) if kind == "EqualNullSafe" else Bounds(UNKNOWN, UNKNOWN)
            # Every "false" bound below also holds where the value is NULL, so EqualNullSafe needs no case
            # of its own: it is false there, where EqualTo is NULL.
            return self._compare(target, _COMPARISONS[kind], value)
        if kind == "In":
            return self._in(target, value)
        if kind == "IsNull":
            return self._is_null(target)
        if kind == "IsNotNull":
            return self._is_null(target).negated()
        if kind in _STRING_FUNCTIONS:
            return self._string_function(target, _STRING_FUNCTIONS[kind], value)
        return Bounds(UNKNOWN, UNKNOWN)

    def _resolve(self, path: tuple[str, ...]) -> _Target | None:
        if not path:
            return None
        top = _field(self._schema, path[0])
        if top is None or top.name in (self._raw_column, self._corrupt_column, FEED_RANGE_COLUMN):
            return None
        if top.name == PK_COLUMN:
            if len(path) != 1 or not isinstance(top.dataType, StringType):
                return None
            pk = self._partition_key()
            if pk is None or len(pk.paths) != 1:
                return None  # hierarchical keys render as JSON arrays
            return _Target(_path_expr(self._alias, parse_path(pk.paths[0])), top.dataType, "pk", True)
        segments = [top.name]
        dtype = top.dataType
        for name in path[1:]:
            if not isinstance(dtype, StructType):
                return None
            sub = _field(dtype, name)
            if sub is None:
                return None
            segments.append(sub.name)
            dtype = sub.dataType
        system = len(segments) == 1 and (
            (top.name in _SYSTEM_STRING_PROPERTIES and isinstance(dtype, StringType))
            or (top.name == "_ts" and isinstance(dtype, LongType))
        )
        return _Target(_path_expr(self._alias, segments), dtype, "system" if system else "property", len(segments) == 1)

    @staticmethod
    def _type_check(dtype: DataType, value: Any) -> str | None:
        """Return the type-check function for which comparing with ``value`` is exact, or None if unsupported."""
        if isinstance(dtype, StringType):
            return "IS_STRING" if isinstance(value, str) else None
        if isinstance(value, bool):
            return "IS_BOOL" if isinstance(dtype, BooleanType) else None
        if isinstance(dtype, _INTEGRAL_TYPES):
            return "IS_NUMBER" if isinstance(value, int) and abs(value) < _MAX_SAFE_INTEGER else None
        if isinstance(dtype, DoubleType):
            if isinstance(value, int):
                return "IS_NUMBER" if abs(value) < _MAX_SAFE_INTEGER else None
            return "IS_NUMBER" if isinstance(value, float) and math.isfinite(value) else None
        return None

    def _guarded(self, target: _Target, check: str, condition: str) -> Bounds:
        if target.kind == "system":
            return Bounds(Pred(condition, True), Pred(f"NOT ({condition})", True))
        guard = f"{check}({target.expr})"
        return Bounds(Pred(f"{condition} OR NOT {guard}", False), Pred(f"NOT ({guard} AND {condition})", False))

    def _compare(self, target: _Target, op: str, value: Any) -> Bounds:
        check = self._type_check(target.dtype, value)
        if check is None:
            return Bounds(UNKNOWN, UNKNOWN)
        if op != "=" and (check == "IS_BOOL" or (isinstance(value, str) and not _ordinal_safe(value))):
            return Bounds(UNKNOWN, UNKNOWN)
        if target.kind == "pk":
            if op == "=":
                return self._pk_equal(target, [value])
            target = _Target(target.expr, target.dtype, "property", True)  # numbers render as text
        return self._guarded(target, check, f"{target.expr} {op} {self._param(value)}")

    def _in(self, target: _Target, values: Any) -> Bounds:
        if not isinstance(values, (list, tuple)):
            return Bounds(UNKNOWN, UNKNOWN)
        present = [v for v in values if v is not None]
        if not present:
            return Bounds(UNKNOWN, UNKNOWN)
        checks = {self._type_check(target.dtype, v) for v in present}
        if len(checks) != 1 or None in checks:
            return Bounds(UNKNOWN, UNKNOWN)
        (check,) = checks
        assert check is not None
        if target.kind == "pk":
            bounds = self._pk_equal(target, present)
        else:
            bounds = self._guarded(target, check, f"ARRAY_CONTAINS({self._param(present)}, {target.expr})")
        if len(present) < len(values):
            return Bounds(bounds.true, NEVER)  # x IN (..., NULL) is true for a listed value, else NULL
        return bounds

    def _pk_equal(self, target: _Target, values: list[Any]) -> Bounds:
        # A string that is not JSON text can only be the rendering of a string key; any other string may
        # also render a number, boolean, null or array key, so those candidates are added and Spark
        # re-checks. Objects render as undefined (NULL), and NaN and Infinity are not JSON values.
        candidates: list[Any] = []
        exact = True
        for value in values:
            if not isinstance(value, str):
                return Bounds(UNKNOWN, UNKNOWN)
            candidates.append(value)
            try:
                other = json.loads(value, parse_constant=_reject_constant)
            except ValueError:
                continue
            if isinstance(other, (bool, int, float, list)) or other is None:
                candidates.append(other)
                exact = False
        condition = f"ARRAY_CONTAINS({self._param(candidates)}, {target.expr})"
        false = Pred(f"NOT (IS_STRING({target.expr}) AND {condition})", False) if exact else UNKNOWN
        return Bounds(Pred(condition, exact), false)

    def _is_null(self, target: _Target) -> Bounds:
        if target.kind == "system":
            return Bounds(NEVER, ALWAYS)
        if target.kind == "pk":  # NULL: undefined, or an object (see documents.render_partition_key)
            missing = f"NOT IS_DEFINED({target.expr}) OR IS_OBJECT({target.expr})"
            return Bounds(Pred(missing, False), Pred(f"IS_DEFINED({target.expr})", False))
        # IS NOT NULL is not pushed for properties: it would only exclude explicit JSON nulls.
        if target.top_level and isinstance(target.dtype, StringType):
            # Conversion to STRING never fails, so only a missing or null property gives NULL. Not for
            # nested fields: a sibling that fails conversion makes the whole struct NULL.
            return Bounds(Pred(f"NOT IS_DEFINED({target.expr}) OR IS_NULL({target.expr})", False), UNKNOWN)
        return Bounds(UNKNOWN, UNKNOWN)

    def _string_function(self, target: _Target, function: str, value: Any) -> Bounds:
        if not isinstance(value, str) or not isinstance(target.dtype, StringType):
            return Bounds(UNKNOWN, UNKNOWN)
        condition = f"{function}({target.expr}, {self._param(value)})"
        if target.kind == "pk":
            target = _Target(target.expr, target.dtype, "property", True)
        return self._guarded(target, "IS_STRING", condition)


class CosmosPushdownBatchReader(CosmosBatchReader):
    """Batch reader with ``pushFilters``, returned by the data source only when Spark pushes filters."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._pushed: tuple[str, tuple[tuple[str, Any], ...]] | None = None  # query, generated parameters
        self._pushed_keys: tuple[str, ...] | None = None  # partition keys (JSON) an exact pk filter allows

    def pushFilters(self, filters: list[Any]) -> Iterable[Any]:
        if not filters or self._opts.item_ids:  # point reads run no query to add the filters to
            return filters
        simple = parse_simple_query(self._opts.query)
        if simple is None:
            logger.info(
                "cosmosdb filter pushdown: the query is not of the form SELECT * FROM <alias> [WHERE ...]; "
                "%d filter(s) are applied by Spark only.",
                len(filters),
            )
            return filters
        pk_cache: list[PartitionKeyInfo | None] = []

        def partition_key() -> PartitionKeyInfo | None:
            if not pk_cache:
                pk_cache.append(self._partition_key_info())
            return pk_cache[0]

        translator = Translator(
            self._schema,
            simple.alias,
            raw_column=self._opts.raw_json_column,
            corrupt_column=self._opts.corrupt_record_column,
            partition_key=partition_key,
            reserved_names=[name for name, _ in self._opts.query_parameters],
        )
        conditions: list[str] = []
        remaining: list[Any] = []
        for flt in filters:
            try:
                pred = translator.bounds(flt).true
            except Exception as exc:  # noqa: BLE001 - an untranslatable filter stays with Spark
                logger.warning("cosmosdb filter pushdown: %s is left to Spark (%s)", type(flt).__name__, exc)
                pred = UNKNOWN
            if pred.sql is not None:
                conditions.append(pred.sql)
            if not pred.exact:
                remaining.append(flt)
            else:
                self._narrow_keys(flt)
        if conditions:
            where = " AND ".join(f"({c})" for c in ([simple.where] if simple.where else []) + conditions)
            query = f"{simple.head} WHERE {where}"
            # A parameter of a sub-filter that an OR dropped is not in the query.
            params = tuple(
                (p["name"], p["value"])
                for p in translator.parameters
                if re.search(re.escape(p["name"]) + r"(?![A-Za-z0-9_])", query)
            )
            self._pushed = (query, params)
            logger.info(
                "cosmosdb filter pushdown: %d of %d filter(s) pushed, %d also applied by Spark; query: %s",
                len(conditions),
                len(filters),
                len(remaining),
                query,
            )
        return remaining

    def _narrow_keys(self, flt: Any) -> None:
        # An exact pk = 'v' or pk IN (...) limits the read to those logical partitions, each read routed
        # by key instead of scanning every physical partition's index.
        if getattr(flt, "attribute", None) not in (("pk",), ["pk"]):
            return
        kind = type(flt).__name__
        if kind == "EqualTo":
            values = [flt.value]
        elif kind == "In":
            values = [v for v in flt.value if v is not None]
        else:
            return
        keys = {json.dumps(v, ensure_ascii=False, separators=(",", ":")) for v in values}
        self._pushed_keys = tuple(sorted(keys if self._pushed_keys is None else keys & set(self._pushed_keys)))

    def _partition_keys(self) -> list[str] | None:
        if self._opts.partition_key is not None or self._pushed_keys is None:
            return super()._partition_keys()
        return list(self._pushed_keys)  # empty if the pushed key filters exclude each other

    def _partition_key_info(self) -> PartitionKeyInfo:
        backend = self._driver_backend(self._driver_token())
        props = self._driver_call("reading container properties", lambda: backend.properties())
        return PartitionKeyInfo.from_container_properties(props)

    def _query(self) -> tuple[str, list[dict[str, Any]] | None]:
        if self._pushed is None:
            return super()._query()
        query, generated = self._pushed
        params = [*self._opts.query_parameters, *generated]
        return query, [{"name": name, "value": value} for name, value in params]
