"""Output schemas and document -> Spark row conversion.

Rows are emitted as tuples in schema order; ``pyspark.sql.worker.plan_data_source_read``
(``records_to_arrow_batches``) converts them to Arrow and rejects values whose Python type does
not match the declared Spark type, so every converter here returns exactly the Python type
PySpark expects (``str``, ``int``, ``float``, ``bool``, ``Decimal``, aware ``datetime`` for
TIMESTAMP, naive ``datetime`` for TIMESTAMP_NTZ, ``date``, ``bytes``, ``list``, ``dict``,
tuples for structs) or raises ``_ConversionFailure``.
"""

from __future__ import annotations

import base64
import binascii
import collections
import json
import re
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from cosmosdb_datasource.errors import CosmosConfigError, CosmosConversionError
from cosmosdb_datasource.options import PARSE_DROPMALFORMED, PARSE_FAILFAST

PK_COLUMN = "pk"
FEED_RANGE_COLUMN = "feed_range"
SNAPSHOT_TS_COLUMN = "snapshot_ts"
STREAM_METADATA_COLUMNS = ("id", "_ts", "operation", "lsn", "lsn_seq", "crts", "ttl_expired")


def batch_schema(raw_column: str = "body") -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField(PK_COLUMN, StringType(), True),
            StructField("_ts", LongType(), True),
            StructField("_etag", StringType(), True),
            StructField(raw_column, StringType(), True),
        ]
    )


def snapshot_stream_schema(raw_column: str = "body") -> StructType:
    """Batch schema plus ``snapshot_ts`` (the same value on every row of one snapshot)."""
    return StructType([*batch_schema(raw_column).fields, StructField(SNAPSHOT_TS_COLUMN, TimestampType(), True)])


def change_feed_schema(raw_column: str = "body") -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField(PK_COLUMN, StringType(), True),
            StructField("operation", StringType(), True),
            StructField("_ts", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("lsn_seq", IntegerType(), True),
            StructField("crts", LongType(), True),
            StructField("ttl_expired", BooleanType(), True),
            StructField(raw_column, StringType(), True),
            StructField(FEED_RANGE_COLUMN, StringType(), True),
        ]
    )


class _ConversionFailure(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# One encoder for every call: json.dumps with non-default arguments builds a new JSONEncoder each time,
# about 15% of the cost of encoding a 0.5 KB document. The output is identical.
_ENCODER = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))


def dumps(value: Any) -> str:
    return _ENCODER.encode(value)


_INT_RANGES = {
    ByteType: (-(2**7), 2**7 - 1),
    ShortType: (-(2**15), 2**15 - 1),
    IntegerType: (-(2**31), 2**31 - 1),
    LongType: (-(2**63), 2**63 - 1),
}

_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})"
    r"(?:[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d+))?)?)?"
    r"\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?$",
    re.ASCII,
)
# Numeric text: ASCII digits with an optional sign, fraction and exponent. Python's int(), float() and
# Decimal() also accept other Unicode digits and "1_000", which Spark's CAST does not.
_NUMBER_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?", re.ASCII)
_INTEGER_RE = re.compile(r"[+-]?\d+", re.ASCII)
# The non-finite values Spark's CAST to DOUBLE accepts, in any case.
_NON_FINITE = frozenset({"nan", "+nan", "-nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"})
_ASCII_SPACE = " \t\n\r\x0b\x0c"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _type_name(value: Any) -> str:
    return type(value).__name__


def _to_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return dumps(value)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to boolean")


def _int_converter(lo: int, hi: int) -> Callable[[Any], int]:
    def convert(value: Any) -> int:
        if isinstance(value, bool):
            raise _ConversionFailure("cannot convert bool to an integer type")
        if isinstance(value, int):
            out = value
        elif isinstance(value, float):
            if not value.is_integer():
                raise _ConversionFailure("non-integral number for an integer type")
            out = int(value)
        elif isinstance(value, str):
            text = value.strip(_ASCII_SPACE)
            if _INTEGER_RE.fullmatch(text):
                out = int(text)
            elif _NUMBER_RE.fullmatch(text):
                as_decimal = Decimal(text)
                if as_decimal != as_decimal.to_integral_value():
                    raise _ConversionFailure("non-integral number for an integer type")
                if not lo <= as_decimal <= hi:  # before int(): "1e999999999" has a billion digits
                    raise _ConversionFailure("integer out of range")
                out = int(as_decimal)
            else:
                raise _ConversionFailure("string is not an integer")
        else:
            raise _ConversionFailure(f"cannot convert {_type_name(value)} to an integer type")
        if not lo <= out <= hi:
            raise _ConversionFailure("integer out of range")
        return out

    return convert


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        raise _ConversionFailure("cannot convert bool to a floating point type")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip(_ASCII_SPACE)
        if _NUMBER_RE.fullmatch(text) or text.lower() in _NON_FINITE:
            return float(text)
        raise _ConversionFailure("string is not a number")
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to a floating point type")


def _decimal_converter(dt: DecimalType) -> Callable[[Any], Decimal]:
    quantum = Decimal(1).scaleb(-dt.scale)
    max_int_digits = dt.precision - dt.scale
    # The default decimal context has 28 digits, too few for DECIMAL(38, s).
    context = Context(prec=dt.precision + 2, rounding=ROUND_HALF_UP)

    def convert(value: Any) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
            raise _ConversionFailure(f"cannot convert {_type_name(value)} to decimal")
        text = value.strip(_ASCII_SPACE) if isinstance(value, str) else str(value)
        if isinstance(value, str) and not _NUMBER_RE.fullmatch(text):
            raise _ConversionFailure("value is not a decimal number")
        try:
            parsed = Decimal(text)
        except (InvalidOperation, ValueError):
            raise _ConversionFailure("value is not a decimal number") from None
        if not parsed.is_finite():
            raise _ConversionFailure("decimal is not finite")
        try:
            dec = parsed.quantize(quantum, context=context)
        except InvalidOperation:
            raise _ConversionFailure(f"decimal does not fit DECIMAL({dt.precision},{dt.scale})") from None
        digits = len(dec.as_tuple().digits) - dt.scale
        if dec != 0 and max(digits, 0) > max_int_digits:
            raise _ConversionFailure(f"decimal does not fit DECIMAL({dt.precision},{dt.scale})")
        return dec

    return convert


def _parse_iso(value: str) -> tuple[datetime, bool]:
    """Parse ISO-8601 (incl. 7-digit .NET fractions) into ``(datetime, had_explicit_zone)``."""
    m = _ISO_RE.match(value.strip())
    if not m:
        raise _ConversionFailure("string is not an ISO-8601 timestamp")
    year, month, day, hour, minute, second, frac, zone = m.groups()
    micros = int((frac or "0")[:6].ljust(6, "0"))
    try:
        naive = datetime(int(year), int(month), int(day), int(hour or 0), int(minute or 0), int(second or 0), micros)
    except ValueError:
        raise _ConversionFailure("invalid calendar date/time") from None
    if zone is None:
        return naive, False
    if zone in ("Z", "z"):
        return naive.replace(tzinfo=timezone.utc), True
    sign = 1 if zone[0] == "+" else -1
    digits = zone[1:].replace(":", "")
    try:
        offset = timedelta(hours=int(digits[:2]), minutes=int(digits[2:4] or 0))
        return naive.replace(tzinfo=timezone(sign * offset)), True
    except ValueError:  # |offset| >= 24h
        raise _ConversionFailure("invalid UTC offset") from None


def _epoch_to_datetime(value: int | float) -> datetime:
    """Convert an epoch number to an aware UTC datetime; magnitude selects seconds, milliseconds or microseconds."""
    magnitude = abs(value)
    if magnitude < 1e11:
        seconds = float(value)
    elif magnitude < 1e14:
        seconds = value / 1e3
    else:
        seconds = value / 1e6
    try:
        return _EPOCH + timedelta(seconds=seconds)
    except OverflowError:
        raise _ConversionFailure("epoch value out of range") from None


def _to_timestamp(value: Any) -> datetime:
    if isinstance(value, bool):
        raise _ConversionFailure("cannot convert bool to timestamp")
    if isinstance(value, (int, float)):
        return _epoch_to_datetime(value)
    if isinstance(value, str):
        parsed, zoned = _parse_iso(value)
        try:
            return parsed.astimezone(timezone.utc) if zoned else parsed.replace(tzinfo=timezone.utc)
        except (OverflowError, ValueError):  # e.g. year 1 with a positive offset
            raise _ConversionFailure("timestamp out of range") from None
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to timestamp")


def _to_timestamp_ntz(value: Any) -> datetime:
    if isinstance(value, bool):
        raise _ConversionFailure("cannot convert bool to timestamp_ntz")
    if isinstance(value, (int, float)):
        return _epoch_to_datetime(value).replace(tzinfo=None)
    if isinstance(value, str):
        parsed, _ = _parse_iso(value)
        return parsed.replace(tzinfo=None)  # keep wall-clock time, like CAST(... AS TIMESTAMP_NTZ)
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to timestamp_ntz")


def _to_date(value: Any) -> date:
    if isinstance(value, bool):
        raise _ConversionFailure("cannot convert bool to date")
    if isinstance(value, (int, float)):
        return _epoch_to_datetime(value).date()
    if isinstance(value, str):
        parsed, _ = _parse_iso(value)
        return parsed.date()
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to date")


def _to_binary(value: Any) -> bytes:
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            raise _ConversionFailure("string is not valid base64") from None
    raise _ConversionFailure(f"cannot convert {_type_name(value)} to binary")


def _lookup(doc: Mapping[str, Any], name: str, fallback: bool = True) -> Any:
    if name in doc:
        return doc[name]
    return _lookup_case_insensitive(doc, name) if fallback else None


def _lookup_case_insensitive(doc: Mapping[str, Any], name: str) -> Any:
    """Return the value of the first property, in document order, whose name equals ``name`` in any case."""
    lowered = name.lower()
    for key, value in doc.items():
        if isinstance(key, str) and key.lower() == lowered:
            return value
    return None


def _unique_ignoring_case(names: list[str]) -> list[bool]:
    """For each name, whether no other name equals it in another case (only those fall back to any case)."""
    counts = collections.Counter(n.lower() for n in names)
    return [counts[n.lower()] == 1 for n in names]


def _null_checked(inner: Callable[[Any], Any], nullable: bool) -> Callable[[Any], Any]:
    def convert(value: Any) -> Any:
        if value is None:
            if not nullable:
                raise _ConversionFailure("null value for a non-nullable field")
            return None
        return inner(value)

    return convert


def build_converter(dt: DataType, nullable: bool = True) -> Callable[[Any], Any]:
    """Return ``f(value) -> spark_value`` raising ``_ConversionFailure`` on unsafe input."""
    inner = _build_non_null_converter(dt)
    native = _native_check(dt)
    if native is None or not nullable:
        return _null_checked(inner, nullable)

    # A value of the column's own JSON type is returned as is, in one call: most values are.
    def convert(value: Any) -> Any:
        if native(value):
            return value
        if value is None:
            return None
        return inner(value)

    return convert


def _native_check(dt: DataType) -> Callable[[Any], bool] | None:
    """Return a test for values the converter returns unchanged, or None."""
    if isinstance(dt, StringType):
        return lambda v: v.__class__ is str
    if isinstance(dt, BooleanType):
        return lambda v: v.__class__ is bool
    if isinstance(dt, DoubleType):
        return lambda v: v.__class__ is float
    for cls, (lo, hi) in _INT_RANGES.items():
        if isinstance(dt, cls):
            return lambda v: v.__class__ is int and lo <= v <= hi  # bool is excluded by the class test
    return None


def _build_non_null_converter(dt: DataType) -> Callable[[Any], Any]:
    if isinstance(dt, StringType):  # includes CHAR/VARCHAR on PySpark versions that subclass it
        return _to_string
    if isinstance(dt, BooleanType):
        return _to_bool
    for cls, (lo, hi) in _INT_RANGES.items():
        if isinstance(dt, cls):
            return _int_converter(lo, hi)
    if isinstance(dt, (FloatType, DoubleType)):
        return _to_float
    if isinstance(dt, DecimalType):
        return _decimal_converter(dt)
    if isinstance(dt, TimestampNTZType):
        return _to_timestamp_ntz
    if isinstance(dt, TimestampType):
        return _to_timestamp
    if isinstance(dt, DateType):
        return _to_date
    if isinstance(dt, BinaryType):
        return _to_binary
    if isinstance(dt, NullType):
        return lambda value: None
    if isinstance(dt, ArrayType):
        element = build_converter(dt.elementType, dt.containsNull)

        def convert_array(value: Any) -> list[Any]:
            if not isinstance(value, list):
                raise _ConversionFailure(f"cannot convert {_type_name(value)} to array")
            return [element(v) for v in value]

        return convert_array
    if isinstance(dt, MapType):
        key_conv = build_converter(dt.keyType, False)
        value_conv = build_converter(dt.valueType, dt.valueContainsNull)

        def convert_map(value: Any) -> dict[Any, Any]:
            if not isinstance(value, Mapping):
                raise _ConversionFailure(f"cannot convert {_type_name(value)} to map")
            return {key_conv(k): value_conv(v) for k, v in value.items()}

        return convert_map
    if isinstance(dt, StructType):
        unique = _unique_ignoring_case(dt.fieldNames())
        fields = [(f.name, build_converter(f.dataType, f.nullable), u) for f, u in zip(dt.fields, unique, strict=True)]

        def convert_struct(value: Any) -> tuple[Any, ...]:
            if value.__class__ is not dict and not isinstance(value, Mapping):
                raise _ConversionFailure(f"cannot convert {_type_name(value)} to struct")
            return tuple([conv(_lookup(value, name, fallback)) for name, conv, fallback in fields])

        return convert_struct
    raise CosmosConfigError(
        f"Spark type {dt.simpleString()} is not supported by the cosmosdb source. Read the column as "
        "STRING (raw JSON) and convert it in Spark, e.g. with parse_json() or from_json()."
    )


class RecordView:
    """Values available to the row builder for one document / change event."""

    __slots__ = (
        "crts",
        "doc",
        "feed_range",
        "id",
        "lsn",
        "lsn_seq",
        "operation",
        "pk",
        "snapshot_ts",
        "ts",
        "ttl_expired",
    )

    def __init__(
        self,
        doc: Mapping[str, Any] | None,
        *,
        id: Any = None,
        pk: Any = None,
        operation: str | None = None,
        ts: int | None = None,
        lsn: int | None = None,
        lsn_seq: int | None = None,
        crts: int | None = None,
        ttl_expired: bool | None = None,
        feed_range: str | None = None,
        snapshot_ts: str | None = None,
    ) -> None:
        self.doc = doc
        self.id = id
        self.pk = pk
        self.operation = operation
        self.ts = ts
        self.lsn = lsn
        self.lsn_seq = lsn_seq
        self.crts = crts
        self.ttl_expired = ttl_expired
        self.feed_range = feed_range
        self.snapshot_ts = snapshot_ts


_Getter = Callable[[RecordView], Any]


def _doc_getter(name: str, fallback: bool) -> _Getter:
    def get(view: RecordView) -> Any:
        doc = view.doc
        if doc.__class__ is dict:  # skips the slow Mapping ABC check for what the SDK returns
            return doc[name] if name in doc else (_lookup_case_insensitive(doc, name) if fallback else None)
        return _lookup(doc, name, fallback) if isinstance(doc, Mapping) else None

    return get


class RowBuilder:
    """Compiled mapping from a ``RecordView`` to a Spark row tuple for a given schema."""

    def __init__(
        self,
        schema: StructType,
        *,
        stream: bool,
        raw_column: str,
        corrupt_column: str,
        parse_mode: str,
        snapshot: bool = False,
    ) -> None:
        self.parse_mode = parse_mode
        self.corrupt_index: int | None = None
        self._fields: list[tuple[str, _Getter, Callable[[Any], Any]] | None] = []
        # A column whose name another column has in another case reads only the property of its exact name.
        unique = _unique_ignoring_case(schema.fieldNames())
        for index, f in enumerate(schema.fields):
            name = f.name
            if name == corrupt_column:
                if not isinstance(f.dataType, StringType):
                    raise CosmosConfigError(f"Column '{corrupt_column}' (corrupt records) must be STRING.")
                self.corrupt_index = index
                self._fields.append(None)
                continue
            getter: _Getter
            if name == raw_column:
                getter = _raw_doc
            elif name == PK_COLUMN:
                getter = _pk
            elif name == FEED_RANGE_COLUMN:
                getter = _feed_range
            elif snapshot and name == SNAPSHOT_TS_COLUMN:
                getter = _snapshot_ts  # ISO-8601 UTC text: converted exactly to TIMESTAMP / TIMESTAMP_NTZ / STRING
            elif stream and name in STREAM_METADATA_COLUMNS:
                getter = _STREAM_GETTERS[name]
            else:
                getter = _doc_getter(name, unique[index])
            if name == raw_column and isinstance(f.dataType, StringType):
                conv: Callable[[Any], Any] = _null_checked(dumps, f.nullable)
            else:
                conv = build_converter(f.dataType, f.nullable)
            self._fields.append((name, getter, conv))
        self._width = len(self._fields)
        self._fast = [(spec[1], spec[2]) if spec is not None else (_no_value, _identity) for spec in self._fields]

    def build(self, view: RecordView) -> tuple[Any, ...] | None:
        """Return the row tuple, or None when the record must be dropped (DROPMALFORMED)."""
        try:
            return tuple([conv(get(view)) for get, conv in self._fast])
        except _ConversionFailure:
            return self._build_failed(view)

    def _build_failed(self, view: RecordView) -> tuple[Any, ...] | None:
        out: list[Any] = [None] * self._width
        failed: list[tuple[str, str]] = []
        for index, spec in enumerate(self._fields):
            if spec is None:
                continue
            name, getter, conv = spec
            try:
                out[index] = conv(getter(view))
            except _ConversionFailure as exc:
                failed.append((name, exc.reason))
        if failed:
            if self.parse_mode == PARSE_FAILFAST:
                details = "; ".join(f"field '{n}': {r}" for n, r in failed)
                raise CosmosConversionError(
                    f"Malformed document id={view.id!r} ({details}). Use mode=PERMISSIVE to keep such "
                    "rows with nulls (optionally capturing the raw JSON in a corrupt-record column)."
                )
            if self.parse_mode == PARSE_DROPMALFORMED:
                return None
            if self.corrupt_index is not None:
                out[self.corrupt_index] = dumps(view.doc) if view.doc is not None else None
        return tuple(out)


def _no_value(view: RecordView) -> Any:
    return None


def _identity(value: Any) -> Any:
    return value


def _raw_doc(view: RecordView) -> Any:
    return view.doc


def _pk(view: RecordView) -> Any:
    return view.pk


def _feed_range(view: RecordView) -> Any:
    return view.feed_range


def _snapshot_ts(view: RecordView) -> Any:
    return view.snapshot_ts


_STREAM_GETTERS: dict[str, _Getter] = {
    "id": lambda v: v.id,
    "_ts": lambda v: v.ts,
    "operation": lambda v: v.operation,
    "lsn": lambda v: v.lsn,
    "lsn_seq": lambda v: v.lsn_seq,
    "crts": lambda v: v.crts,
    "ttl_expired": lambda v: v.ttl_expired,
}


def validate_schema(schema: StructType) -> None:
    """Fail fast on the driver for types the converter cannot produce."""
    for f in schema.fields:
        build_converter(f.dataType, f.nullable)
