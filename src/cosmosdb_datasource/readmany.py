"""``read_many``: read the documents identified by a DataFrame of ``(id, partition key)`` rows."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType, StructType

from cosmosdb_datasource import _compat, client, retry
from cosmosdb_datasource.arrow import record_batches
from cosmosdb_datasource.auth import mint_driver_token
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import READ_OPTIONS, parse_options
from cosmosdb_datasource.point_reads import ITEMS_PER_REQUEST
from cosmosdb_datasource.schema import RecordView, RowBuilder, batch_schema, validate_schema


def read_many(
    identities: DataFrame,
    *,
    id_column: str = "id",
    pk_column: str = "pk",
    schema: StructType | str | None = None,
    **options: str,
) -> DataFrame:
    """Return the documents whose ``(id, partition key)`` are rows of ``identities``, read by point reads.

    ``options`` are the data source's connection and conversion options; options that choose what to read
    (such as ``query`` or ``page_size``) are rejected. The partition key column holds
    the key in its JSON type (a string, number or boolean; an array for hierarchical keys). Rows of
    documents that do not exist are left out. The result has the batch-read schema, or ``schema``.
    """
    given = {str(k).strip().lower() for k, v in options.items() if v is not None}
    stray = sorted(given & set(READ_OPTIONS))
    if stray:
        raise CosmosConfigError(
            f"read_many does not take option(s) {', '.join(stray)}: it takes the connection and conversion options."
        )
    opts = parse_options(options)
    if schema is None:
        out_schema = batch_schema(opts.raw_json_column)
    elif isinstance(schema, str):
        parsed: DataType = DataType.fromDDL(schema)
        if not isinstance(parsed, StructType):
            raise CosmosConfigError(f"read_many schema must be a struct, got {parsed.simpleString()}.")
        out_schema = parsed
    else:
        out_schema = _compat.local_struct_type(schema)
    validate_schema(out_schema)
    token = mint_driver_token(opts.auth, opts.endpoint)  # executors refresh it themselves near expiry
    policy = retry.RetryPolicy(max_retries=opts.max_retries, max_backoff_seconds=opts.retry_max_backoff_seconds)

    def read(batches: Iterator[Any]) -> Iterator[Any]:
        backend = client.get_backend(opts, token)
        props = retry.call_with_retry(lambda: backend.properties(), policy, "reading container properties")
        pk = PartitionKeyInfo.from_container_properties(props)
        builder = RowBuilder(
            out_schema,
            stream=False,
            raw_column=opts.raw_json_column,
            corrupt_column=opts.corrupt_record_column,
            parse_mode=opts.parse_mode,
        )
        for batch in batches:
            ids = batch.column(0).to_pylist()
            keys = batch.column(1).to_pylist()
            items = list(dict.fromkeys((i, _hashable(k)) for i, k in zip(ids, keys, strict=True) if i is not None))
            rows = []
            for start in range(0, len(items), ITEMS_PER_REQUEST):
                chunk = [(i, _key(k)) for i, k in items[start : start + ITEMS_PER_REQUEST]]

                def fetch(chunk: list[tuple[str, Any]] = chunk) -> tuple[list[dict[str, Any]], float]:
                    return backend.read_items(chunk)

                docs, _charge = retry.call_with_retry(fetch, policy, "reading items")
                for doc in docs:
                    row = builder.build(RecordView(doc, id=doc.get("id"), pk=pk.render(doc)))
                    if row is not None:
                        rows.append(row)
            if rows:
                yield from record_batches(iter(rows), out_schema, len(rows))

    return identities.select(identities[id_column], identities[pk_column]).mapInArrow(read, out_schema)


def _hashable(key: Any) -> Any:
    return tuple(key) if isinstance(key, list) else key


def _key(key: Any) -> Any:
    return list(key) if isinstance(key, tuple) else key
