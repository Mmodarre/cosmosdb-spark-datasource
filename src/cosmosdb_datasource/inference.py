"""Schema inference from a sample of documents (``infer_schema=true``).

``DataSource.schema()`` runs on the driver, once per ``load()``, and only when no schema was given, so
the sample is read there: up to ``sampling_size`` results of ``sampling_query`` (default: the read's
``query``), spread evenly over the physical partitions, which are read one after the other. A query read
returns documents in ``_rid`` (insertion) order per partition, so the sample favours the oldest
documents of every partition.

Types merge like the connector's ``CosmosTableSchemaInferrer`` (``TypeCoercion.findTightestCommonType``,
then struct and array merging, else STRING), with three differences: every integer is BIGINT (JSON as
parsed by Python does not tell 32 from 64 bits, and a later document may not fit an INT), integers beyond
the BIGINT range are DOUBLE, and a property that is null in every sampled document is STRING rather than
NULL (an array that is empty in every sample is ARRAY<STRING>). Fields keep the order in which they first
appear in the sample.

Spark resolves column names case-insensitively by default, so property names that differ only in case
(``name`` and ``Name``) are one field, named as first seen, with the types of both merged. The row builder
reads such a column by exact name first, then case-insensitively (:mod:`cosmosdb_datasource.schema`).
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterator, Mapping, Sequence
from itertools import islice
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from cosmosdb_datasource import client, retry
from cosmosdb_datasource.auth import mint_driver_token
from cosmosdb_datasource.batch import QueryStats, query_documents
from cosmosdb_datasource.documents import PartitionKeyInfo, parse_path
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import CosmosOptions
from cosmosdb_datasource.schema import FEED_RANGE_COLUMN, PK_COLUMN

logger = logging.getLogger("cosmosdb_datasource")

SYSTEM_PROPERTIES = ("_rid", "_self", "_etag", "_attachments")
TIMESTAMP_PROPERTY = "_ts"
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1


def value_type(value: Any) -> DataType:
    """Return the Spark type of one JSON value (NullType for null and for empty or all-null arrays)."""
    if value is None:
        return NullType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return LongType() if _LONG_MIN <= value <= _LONG_MAX else DoubleType()
    if isinstance(value, float):
        return DoubleType()
    if isinstance(value, str):
        return StringType()
    if isinstance(value, list):
        element: DataType = NullType()
        for item in value:
            element = merge_types(element, value_type(item))
        return ArrayType(element, True)
    if isinstance(value, Mapping):
        return object_type(value)
    return StringType()


def _add_field(fields: dict[str, tuple[str, DataType]], name: str, dtype: DataType) -> None:
    """Add a field to ``fields`` (lower-case name -> (name, type)): one field per name in any case."""
    key = name.lower()
    if key in fields:
        first, current = fields[key]
        fields[key] = (first, merge_types(current, dtype))
    else:
        fields[key] = (name, dtype)


def _struct(fields: dict[str, tuple[str, DataType]]) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields.values()])


def object_type(doc: Mapping[str, Any], *, skip: Sequence[str] = ()) -> StructType:
    fields: dict[str, tuple[str, DataType]] = {}
    for key, value in doc.items():
        if isinstance(key, str) and key not in skip:
            _add_field(fields, key, value_type(value))
    return _struct(fields)


def merge_types(a: DataType, b: DataType) -> DataType:
    """Return the tightest type both values convert to; STRING when there is none."""
    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType) or a == b:
        return a
    numeric = (LongType, DoubleType)
    if isinstance(a, numeric) and isinstance(b, numeric):
        return DoubleType()
    if isinstance(a, StructType) and isinstance(b, StructType):
        return merge_structs(a, b)
    if isinstance(a, ArrayType) and isinstance(b, ArrayType):
        return ArrayType(merge_types(a.elementType, b.elementType), True)
    return StringType()


def merge_structs(a: StructType, b: StructType) -> StructType:
    fields: dict[str, tuple[str, DataType]] = {}
    for f in (*a.fields, *b.fields):
        _add_field(fields, f.name, f.dataType)
    return _struct(fields)


def _resolve_nulls(dtype: DataType) -> DataType:
    if isinstance(dtype, NullType):
        return StringType()
    if isinstance(dtype, ArrayType):
        return ArrayType(_resolve_nulls(dtype.elementType), True)
    if isinstance(dtype, StructType):
        return StructType([StructField(f.name, _resolve_nulls(f.dataType), True) for f in dtype.fields])
    return dtype


def infer_schema(
    documents: Sequence[Any],
    pk: PartitionKeyInfo,
    *,
    raw_column: str,
    corrupt_column: str,
    system_properties: bool = False,
    timestamp: bool = False,
) -> StructType:
    """Return the schema of ``documents``: ``id`` and ``pk`` first if present, then every property."""
    skip = [] if system_properties else list(SYSTEM_PROPERTIES)
    if not timestamp and not system_properties:
        skip.append(TIMESTAMP_PROPERTY)
    # Names the row builder fills from somewhere else than the property of the same name, in any case.
    shadowed = {n.lower() for n in (PK_COLUMN, raw_column, corrupt_column, FEED_RANGE_COLUMN)}
    key_path = parse_path(pk.paths[0]) if len(pk.paths) == 1 else ()
    merged = StructType([])
    has_pk = False
    for doc in documents:
        if not isinstance(doc, Mapping):
            raise CosmosConfigError(
                "infer_schema needs query results that are JSON objects; the sampling query returned "
                f"a {type(doc).__name__}. Use SELECT * or a projection, not SELECT VALUE."
            )
        merged = merge_structs(merged, object_type(doc, skip=skip))
        has_pk = has_pk or pk.render(doc) is not None
    out = []
    properties = []
    for f in merged.fields:
        dtype = _resolve_nulls(f.dataType)
        if f.name.lower() == "id":
            out.append(StructField("id", dtype, True))
        elif f.name.lower() in shadowed:
            if key_path != (f.name,):  # the partition key property itself is the pk column
                logger.warning(
                    "cosmosdb infer_schema: property %r is not inferred; the column %r has another use.",
                    f.name,
                    f.name.lower(),
                )
        else:
            properties.append(StructField(f.name, dtype, True))
    if has_pk:
        out.append(StructField(PK_COLUMN, StringType(), True))
    return StructType(out + properties)


def sample_documents(opts: CosmosOptions) -> tuple[list[Any], PartitionKeyInfo]:
    """Read up to ``sampling_size`` results of the sampling query, spread over the physical partitions."""
    token = mint_driver_token(opts.auth, opts.endpoint)
    backend = client.get_backend(opts, token)
    policy = retry.RetryPolicy(max_retries=opts.max_retries, max_backoff_seconds=opts.retry_max_backoff_seconds)
    props = retry.call_with_retry(lambda: backend.properties(refresh=True), policy, "reading container properties")
    pk = PartitionKeyInfo.from_container_properties(props)
    ranges = retry.call_with_retry(lambda: backend.feed_ranges(force_refresh=True), policy, "reading feed ranges")
    per_range = math.ceil(opts.sampling_size / max(1, len(ranges)))
    query = opts.sampling_query or opts.query
    parameters = opts.parameters_for_sdk()

    def sample(rng: tuple[str, str]) -> list[Any]:
        docs: Iterator[Any] = query_documents(
            backend,
            query,
            parameters,
            rng,
            min(per_range, opts.page_size),
            policy=policy,
            stats=QueryStats(),
            describe=f"the schema sample of range {rng[0]}-{rng[1]}",
        )
        return list(islice(docs, per_range))

    # One range after the other: requests on a client are serialized anyway (SdkBackend).
    samples = [sample(rng) for rng in ranges]
    return [doc for docs in samples for doc in docs][: opts.sampling_size], pk


def infer_schema_from_container(opts: CosmosOptions) -> StructType:
    documents, pk = sample_documents(opts)
    schema = infer_schema(
        documents,
        pk,
        raw_column=opts.raw_json_column,
        corrupt_column=opts.corrupt_record_column,
        system_properties=opts.infer_system_properties,
        timestamp=opts.infer_timestamp,
    )
    if not schema.fields:
        raise CosmosConfigError(
            "infer_schema found no properties: the sampling query returned no documents. Supply a schema "
            "with .schema(...), or read without infer_schema."
        )
    logger.info("cosmosdb infer_schema: %d column(s) from %d sampled document(s)", len(schema.fields), len(documents))
    return schema
