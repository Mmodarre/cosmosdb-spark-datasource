"""Type conversion, row building modes, partition-key rendering and change-feed item mapping."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.worker.plan_data_source_read import records_to_arrow_batches

from cosmosdb_datasource.documents import PartitionKeyInfo, parse_path, to_change_record
from cosmosdb_datasource.errors import CosmosChangeFeedError, CosmosConfigError, CosmosConversionError
from cosmosdb_datasource.schema import (
    RecordView,
    RowBuilder,
    _ConversionFailure,
    batch_schema,
    build_converter,
    change_feed_schema,
)


class _DS:
    @classmethod
    def name(cls) -> str:
        return "cosmosdb"


def arrow_ok(rows, schema) -> list[dict]:
    batches = list(records_to_arrow_batches(iter(rows), 100, schema, _DS()))
    return [r for b in batches for r in b.to_pylist()]


def conv(dt, value, nullable=True):
    return build_converter(dt, nullable)(value)


@pytest.mark.parametrize(
    ("dt", "value", "expected"),
    [
        (StringType(), "x", "x"),
        (StringType(), 12, "12"),
        (StringType(), 1.5, "1.5"),
        (StringType(), True, "true"),
        (StringType(), {"a": [1, "é"]}, '{"a":[1,"é"]}'),
        (BooleanType(), "TRUE", True),
        (BooleanType(), 0, False),
        (LongType(), "42", 42),
        (LongType(), 42.0, 42),
        (LongType(), "1e3", 1000),
        (IntegerType(), -(2**31), -(2**31)),
        (DoubleType(), "2.5", 2.5),
        (DoubleType(), 3, 3.0),
        (DecimalType(10, 2), 1.005, Decimal("1.01")),
        (DecimalType(10, 2), "12345678.9", Decimal("12345678.90")),
        (BinaryType(), "aGVsbG8=", b"hello"),
        (DateType(), "2026-03-04T23:59:00-05:00", date(2026, 3, 4)),
        (DateType(), 0, date(1970, 1, 1)),
    ],
)
def test_scalar_conversions(dt, value, expected) -> None:
    assert conv(dt, value) == expected


@pytest.mark.parametrize(
    ("dt", "value"),
    [
        (LongType(), "abc"),
        (LongType(), 1.5),
        (LongType(), True),
        (ByteType(), 300),
        (IntegerType(), 2**31),
        (BooleanType(), "yes"),
        (DoubleType(), "nan-ish"),
        (DoubleType(), [1]),
        (DecimalType(4, 2), 123.45),
        (DecimalType(10, 2), "1,5"),
        (BinaryType(), "not base64!"),
        (TimestampType(), "31/01/2026"),
        (TimestampType(), "2026-02-30T00:00:00Z"),
        (ArrayType(LongType()), "1,2"),
        (MapType(StringType(), LongType()), [1]),
        (StructType([StructField("a", LongType())]), "{}"),
    ],
)
def test_unsafe_conversions_fail(dt, value) -> None:
    with pytest.raises(_ConversionFailure):
        conv(dt, value)


def test_timestamps() -> None:
    utc = timezone.utc
    assert conv(TimestampType(), "2026-01-31T10:00:00.1234567Z") == datetime(2026, 1, 31, 10, 0, 0, 123456, tzinfo=utc)
    assert conv(TimestampType(), "2026-01-31T10:00:00+10:00") == datetime(2026, 1, 31, 0, 0, tzinfo=utc)
    assert conv(TimestampType(), "2026-01-31 10:00") == datetime(2026, 1, 31, 10, 0, tzinfo=utc)  # naive = UTC
    assert conv(TimestampType(), 1_769_853_600) == datetime(2026, 1, 31, 10, 0, tzinfo=utc)  # seconds
    assert conv(TimestampType(), 1_769_853_600_000) == datetime(2026, 1, 31, 10, 0, tzinfo=utc)  # millis
    assert conv(TimestampType(), 1_769_853_600_000_000) == datetime(2026, 1, 31, 10, 0, tzinfo=utc)  # micros
    assert conv(TimestampNTZType(), "2026-01-31T10:00:00+10:00") == datetime(2026, 1, 31, 10, 0)  # wall clock
    assert conv(TimestampNTZType(), 0) == datetime(1970, 1, 1)


def test_nested_types_and_nullability() -> None:
    dt = StructType(
        [
            StructField("name", StringType()),
            StructField("tags", ArrayType(StringType())),
            StructField("scores", MapType(StringType(), DoubleType())),
            StructField("inner", StructType([StructField("n", LongType(), False)])),
        ]
    )
    value = {"Name": "x", "tags": ["a", None], "scores": {"m": 1}, "inner": {"n": "5"}}
    assert conv(dt, value) == ("x", ["a", None], {"m": 1.0}, (5,))
    with pytest.raises(_ConversionFailure, match="non-nullable"):
        conv(dt, {"inner": {}})
    with pytest.raises(_ConversionFailure, match="non-nullable"):
        conv(ArrayType(LongType(), containsNull=False), [1, None])
    with pytest.raises(_ConversionFailure, match="non-nullable"):
        conv(LongType(), None, nullable=False)


def test_unsupported_type_is_config_error() -> None:
    with pytest.raises(CosmosConfigError, match="parse_json"):
        build_converter(DayTimeIntervalType())


USER_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("pk", StringType()),
        StructField("age", IntegerType()),
        StructField("when", TimestampType()),
        StructField("address", StructType([StructField("city", StringType()), StructField("zip", IntegerType())])),
        StructField("missing", StringType()),
        StructField("body", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)


def builder(schema=USER_SCHEMA, mode="PERMISSIVE", stream=False, raw="body") -> RowBuilder:
    return RowBuilder(schema, stream=stream, raw_column=raw, corrupt_column="_corrupt_record", parse_mode=mode)


GOOD = {
    "id": "1",
    "patientId": "p1",
    "age": "41",
    "when": "2026-01-01T00:00:00Z",
    "address": {"city": "Syd", "zip": 2000},
}
BAD = {"id": "2", "patientId": "p2", "age": "forty", "when": "soon", "address": {"city": "Mel"}}


def test_permissive_keeps_row_with_nulls_and_corrupt_json() -> None:
    b = builder()
    good = b.build(RecordView(GOOD, id="1", pk="p1"))
    bad = b.build(RecordView(BAD, id="2", pk="p2"))
    assert good[:6] == ("1", "p1", 41, datetime(2026, 1, 1, tzinfo=timezone.utc), ("Syd", 2000), None)
    assert json.loads(good[6]) == GOOD and good[7] is None
    assert bad[2] is None and bad[3] is None and bad[4] == ("Mel", None)
    assert json.loads(bad[7]) == BAD
    assert len(arrow_ok([good, bad], USER_SCHEMA)) == 2


def test_dropmalformed_and_failfast() -> None:
    assert builder(mode="DROPMALFORMED").build(RecordView(BAD, id="2")) is None
    with pytest.raises(CosmosConversionError, match=r"id='2'.*field 'age'") as info:
        builder(mode="FAILFAST").build(RecordView(BAD, id="2"))
    assert "forty" not in str(info.value)  # values are not echoed into error messages


def test_raw_column_can_be_struct_or_renamed() -> None:
    schema = StructType(
        [StructField("body", StructType([StructField("id", StringType()), StructField("age", LongType())]))]
    )
    assert builder(schema).build(RecordView(GOOD)) == (("1", 41),)
    renamed = StructType([StructField("_raw", StringType()), StructField("body", StringType())])
    row = builder(renamed, raw="_raw").build(RecordView({"body": "hello", "id": "x"}))
    assert json.loads(row[0]) == {"body": "hello", "id": "x"} and row[1] == "hello"


def test_corrupt_column_must_be_string() -> None:
    with pytest.raises(CosmosConfigError, match="must be STRING"):
        builder(StructType([StructField("_corrupt_record", LongType())]))


def test_default_schemas_produce_arrow_compatible_rows() -> None:
    b = builder(batch_schema(), stream=False)
    row = b.build(RecordView({"id": "a", "_ts": 5, "_etag": '"e"'}, id="a", pk=["t1", 2]))
    assert row == ("a", '["t1",2]', 5, '"e"', '{"id":"a","_ts":5,"_etag":"\\"e\\""}')
    arrow_ok([row], batch_schema())
    cf = builder(change_feed_schema(), stream=True)
    row = cf.build(
        RecordView(None, id="a", pk="p", operation="delete", ts=9, lsn=10, crts=9, ttl_expired=True, feed_range="-FF")
    )
    assert row == ("a", "p", "delete", 9, 10, 9, True, None, "-FF")
    arrow_ok([row], change_feed_schema())


@pytest.mark.parametrize(
    ("paths", "doc", "expected", "rendered"),
    [
        (("/patientId",), {"patientId": "p1"}, "p1", "p1"),
        (("/n",), {"n": 7}, 7, "7"),
        (("/b",), {"b": False}, False, "false"),
        (("/x",), {"y": 1}, None, None),
        (("/a/b",), {"a": {"b": "deep"}}, "deep", "deep"),
        (('/"odd key"',), {"odd key": "v"}, "v", "v"),
        (("/t", "/u", "/s"), {"t": "T", "u": 1, "s": "S"}, ["T", 1, "S"], '["T",1,"S"]'),
        (("/t", "/u"), {"t": "T"}, ["T", None], '["T",null]'),
    ],
)
def test_partition_key_extraction(paths, doc, expected, rendered) -> None:
    info = PartitionKeyInfo(paths=paths, kind="MultiHash" if len(paths) > 1 else "Hash", version=2)
    assert info.extract(doc) == expected
    assert build_converter(StringType())(info.extract(doc)) == rendered


def test_parse_path_and_container_properties() -> None:
    assert parse_path("/a/b/c") == ("a", "b", "c")
    with pytest.raises(ValueError):
        parse_path("a/b")
    info = PartitionKeyInfo.from_container_properties(
        {"partitionKey": {"paths": ["/tenantId", "/userId"], "kind": "MultiHash", "version": 2}}
    )
    assert info.hierarchical and info.kind == "MultiHash"
    legacy = PartitionKeyInfo.from_container_properties({"partitionKey": {"paths": ["/pk"]}})
    assert legacy.version == 1


PK = PartitionKeyInfo(paths=("/deviceId",), kind="Hash", version=2)


def test_latest_version_item() -> None:
    rec = to_change_record({"id": "d1", "deviceId": "dev", "_ts": 10, "_lsn": 77}, "LatestVersion", PK)
    assert (rec.id, rec.pk, rec.operation, rec.ts, rec.lsn, rec.crts, rec.ttl_expired) == (
        "d1",
        "dev",
        "upsert",
        10,
        77,
        None,
        False,
    )
    assert rec.doc["_lsn"] == 77
    with pytest.raises(CosmosChangeFeedError, match="_lsn"):
        to_change_record({"id": "x"}, "LatestVersion", PK)


def test_avad_items() -> None:
    create = {
        "current": {"id": "d1", "deviceId": "dev", "_ts": 11},
        "metadata": {"operationType": "create", "lsn": 5, "crts": 11},
    }
    rec = to_change_record(create, "AllVersionsAndDeletes", PK)
    assert (rec.operation, rec.lsn, rec.ts, rec.crts, rec.pk, rec.doc["id"]) == ("create", 5, 11, 11, "dev", "d1")
    delete = {
        "metadata": {
            "operationType": "delete",
            "lsn": 9,
            "crts": 20,
            "id": "d1",
            "partitionKey": {"deviceId": "dev"},
            "timeToLiveExpired": True,
        }
    }
    rec = to_change_record(delete, "AllVersionsAndDeletes", PK)
    assert (rec.operation, rec.id, rec.pk, rec.ts, rec.doc, rec.ttl_expired) == ("delete", "d1", "dev", 20, None, True)
    # delete identified via a previous image only
    prev = {"previous": {"id": "d2", "deviceId": "dev2", "_ts": 3}, "metadata": {"operationType": "delete", "lsn": 10}}
    rec = to_change_record(prev, "AllVersionsAndDeletes", PK)
    assert (rec.id, rec.pk, rec.ts) == ("d2", "dev2", 3)


def test_avad_hierarchical_delete_partition_key() -> None:
    info = PartitionKeyInfo(paths=("/tenantId", "/userId"), kind="MultiHash", version=2)
    for meta_pk in ({"tenantId": "t", "userId": "u"}, {"/tenantId": "t", "/userId": "u"}, ["t", "u"]):
        item = {"metadata": {"operationType": "delete", "lsn": 1, "crts": 1, "id": "x", "partitionKey": meta_pk}}
        assert to_change_record(item, "AllVersionsAndDeletes", info).pk == ["t", "u"]


def test_wide_decimals_and_timestamp_edge_cases() -> None:
    assert conv(DecimalType(38, 18), "12345678901.5") == Decimal("12345678901.500000000000000000")
    assert conv(DecimalType(38, 0), 10**37) == Decimal(10**37)
    with pytest.raises(_ConversionFailure, match="does not fit"):
        conv(DecimalType(38, 18), 10**25)
    for bad in ("2026-01-01T00:00:00+24:00", "0001-01-01T00:00:00+05:00"):
        with pytest.raises(_ConversionFailure):
            conv(TimestampType(), bad)
    row = builder(StructType([StructField("when", TimestampType()), StructField("_corrupt_record", StringType())]))
    assert row.build(RecordView({"when": "0001-01-01T00:00:00+05:00"}))[0] is None
