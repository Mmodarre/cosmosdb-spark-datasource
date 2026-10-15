# cosmosdb-spark-datasource

[![CI](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/workflows/ci.yml)
[![Integration](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/workflows/integration.yml/badge.svg?branch=main)](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/workflows/integration.yml)
[![Coverage](https://codecov.io/gh/Mmodarre/cosmosdb-spark-datasource/graph/badge.svg?branch=main)](https://codecov.io/gh/Mmodarre/cosmosdb-spark-datasource)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A PySpark data source for **Azure Cosmos DB for NoSQL**, written in Python on the PySpark Python
Data Source API (`pyspark.sql.datasource`). It reads containers in batch and streams their change
feed, with `spark.read.format("cosmosdb")` and `spark.readStream.format("cosmosdb")`.

## Why

The Azure Cosmos DB Spark connector (`azure-cosmos-spark`) is a JVM library. It cannot run on
Databricks serverless compute or in Lakeflow Spark Declarative Pipelines (SDP), where JVM
libraries cannot be installed. This data source needs only Python packages (`azure-cosmos`, and
optionally `azure-identity`), which serverless environments and pipelines can install.

## Features

- **Batch reads**: one Spark task per physical partition (feed range), optionally split further by
  effective partition key (EPK); a server-side `query` with parameters; opt-in
  [filter pushdown](#filter-pushdown); [schema inference](#schema-inference) by sampling.
- **Logical partitions and point reads**: `partition_key`, `item_ids` and `read_many` for a
  DataFrame of `(id, pk)` ([details](#logical-partitions-and-point-reads)).
- **Change-feed streams** in `LatestVersion` or `AllVersionsAndDeletes` mode. Micro-batches are
  bounded by LSN per range, so a retried or replayed batch reads the same changes.
- **Initial snapshot** (`initial_snapshot=true`): the first micro-batch reads the whole container,
  later micro-batches read the change feed from the position the stream started at. One stream
  yields the current state plus every later change, including deletes with
  `AllVersionsAndDeletes`.
- **Snapshot stream** (`snapshot_stream=true`): every micro-batch is one complete snapshot of the
  container, stamped with `snapshot_ts`, so an append flow keeps a history of container states.
- **Change feed as a batch read**, optionally continued from a batch checkpoint
  ([details](#change-feed-as-a-batch-read)).
- **Authentication**: account key, Entra ID service principal (secret or certificate), pre-minted
  access token (for example from a Unity Catalog service credential), or `DefaultAzureCredential`.
  Tokens are minted on the driver and shipped to the tasks.
- **Throughput control**: `priority`, throughput buckets and a per-read RU budget
  ([details](#throughput-control)).
- **Typed reads**: a user-specified schema is filled from the documents with safe conversions, in
  `PERMISSIVE`, `DROPMALFORMED` or `FAILFAST` mode.
- Rows are streamed page by page with the next page read ahead and handed to Spark as Arrow
  batches, one client is reused per Python worker, retries back off with jitter and honour
  `x-ms-retry-after-ms`, and per-partition metrics (rows, RU, duration) and slow requests can be
  logged.

## Compatibility

| Component | Versions | Tested |
|---|---|---|
| PySpark | 4.0, 4.1, 4.2 | CI: unit tests and local Spark (JVM) tests on each version |
| Python | 3.10 to 3.13 | CI |
| `azure-cosmos` | `>=4.14.0,<5` | CI: unit tests with 4.14.0 and with the latest 4.x; integration tests with 4.17.1 |
| `azure-identity` (optional) | `>=1.19,<2` | unit tests only |
| Databricks serverless | environment version 2 or later | manually: environments 2 to 5, jobs and notebooks, with 0.2.0; environment 4 with 0.3.1 (not in CI) |
| Databricks classic compute | DBR 15.4 LTS or later | manually with 0.2.0: DBR 15.4, 16.4, 17.3 (dedicated and standard access mode), 18.3 and 19.x (not in CI) |
| Lakeflow SDP | DBR 18.3, serverless and classic | manually, with 0.2.0 and 0.3.1 (not in CI) |
| OSS Spark 3.x | | not supported: the Python Data Source API starts with Spark 4.0 |

Databricks runtimes built on Spark 3.5 from DBR 15.4 LTS (and serverless environments 2 and 3)
include the streaming Python data source API; DBR 14.3 LTS and serverless environment 1 include
only the batch API, and importing the package there fails with an `ImportError` that names the
supported runtimes. Python 3.10 is therefore not usable on Databricks: only those two runtimes
have it.

The integration tests run against two Cosmos DB emulators in CI (see
[CONTRIBUTING.md](CONTRIBUTING.md#integration-tests)). Neither emulator has `AllVersionsAndDeletes`
or Entra ID: those are covered by unit tests against a service fake and were run manually against
Azure. Partition splits are covered by the fake in CI and were run manually against Azure (see
[Guarantees](#guarantees)).

## Installation

```bash
pip install cosmosdb-spark-datasource             # account key or access-token auth
pip install "cosmosdb-spark-datasource[identity]" # adds azure-identity (service_principal, default)
```

PySpark is not a dependency: the Spark runtime provides it.

- **Databricks serverless notebooks and jobs**: add the package to the notebook or job
  environment, or run `%pip install cosmosdb-spark-datasource`. The environment must be version 2
  or later. A notebook without environment metadata (for example one imported in `SOURCE`
  format) runs on version 1, where the import fails.
- **Databricks classic compute**: install it as a cluster library or with `%pip`.
- **Lakeflow Spark Declarative Pipelines, serverless**: add it to the pipeline's
  `environment.dependencies`. Write version ranges with `~=` (e.g.
  `cosmosdb-spark-datasource~=0.3.1`): on DBR 18.3 the dependency string `azure-identity>=1.19,<2`
  failed to install (`ENVIRONMENT_PIP_INSTALL_ERROR`) while `azure-identity~=1.19` installed.
- **Lakeflow Spark Declarative Pipelines, classic compute**: put
  `%pip install cosmosdb-spark-datasource azure-identity~=1.19` in the **first** cell of a notebook
  source. With any cell before it, even a comment, the update fails with
  `PIP_INSTALL_NOT_AT_TOP_OF_NOTEBOOK` (DBR 18.3).

A wheel built from source (`uv build`) can be installed the same way from a Unity Catalog volume
path. In standard access mode a wheel from a volume installs as a cluster library without an
artifact allowlist entry (DBR 17.3).

## Quick start

```python
from cosmosdb_datasource import CosmosDBDataSource

spark.dataSource.register(CosmosDBDataSource)  # once per Spark session

conn = {
    "endpoint": "https://<account>.documents.azure.com:443/",
    "database": "<database>",
    "auth_type": "key",
    "account_key": dbutils.secrets.get("<scope>", "<key-name>"),
}
```

**Batch read.** The whole container, one task per physical partition, or a server-side filter:

```python
customers = spark.read.format("cosmosdb").options(**conn, container="customers").load()

recent = (
    spark.read.format("cosmosdb")
    .options(
        **conn,
        container="orders",
        query="SELECT * FROM c WHERE c._ts > @since",
        query_parameters='[{"name": "@since", "value": 1790000000}]',
    )
    .load()
)
```

**Change feed.** `LatestVersion` from the beginning of the container into a Delta table:

```python
changes = (
    spark.readStream.format("cosmosdb").options(**conn, container="orders", change_feed_mode="LatestVersion").load()
)
(
    changes.writeStream.option("checkpointLocation", "/Volumes/<catalog>/<schema>/<volume>/orders")
    .trigger(availableNow=True)
    .toTable("<catalog>.<schema>.orders_changes")
)
```

**Initial snapshot.** `AllVersionsAndDeletes` can only start at "now"; with `initial_snapshot` the
first micro-batch reads every current document, and later micro-batches every create, replace and
delete:

```python
orders = (
    spark.readStream.format("cosmosdb")
    .options(
        **conn,
        container="orders",
        change_feed_mode="AllVersionsAndDeletes",
        initial_snapshot="true",
    )
    .load()
)
```

To merge snapshot and change rows into the current state, order the rows of each key `(pk, id)` by
`struct(operation <> 'snapshot', lsn, lsn_seq)` and keep the last one; a `delete` means the document
is gone. In a Lakeflow pipeline that is one `create_auto_cdc_flow`:

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct


@dp.table(name="orders_bronze")
def orders_bronze():
    return (
        spark.readStream.format("cosmosdb")
        .options(**conn, container="orders", change_feed_mode="AllVersionsAndDeletes", initial_snapshot="true")
        .load()
    )


dp.create_streaming_table("orders_current")
dp.create_auto_cdc_flow(
    target="orders_current",
    source="orders_bronze",
    keys=["pk", "id"],
    sequence_by=struct((col("operation") != "snapshot").cast("int").alias("rank"), col("lsn"), col("lsn_seq")),
    apply_as_deletes=expr("operation = 'delete'"),
    except_column_list=["operation", "lsn_seq", "crts", "ttl_expired", "feed_range"],
    stored_as_scd_type="1",
)
```

Register the data source at the top of the pipeline source file: pipelines run the file in their
own session.

## Options

Option names are case-insensitive. Unknown options are logged as a warning and ignored.

### Connection and authentication

| Option | Default | Description |
|---|---|---|
| `endpoint` | required | Account URI, `https://<account>.documents.azure.com:443/`. `http://` only for an emulator: `localhost`, a loopback address or a host name without a domain (a container on a local network). |
| `database` | required | Database name |
| `container` | required | Container name |
| `auth_type` | inferred | `key`, `service_principal`, `access_token` or `default`. Inferred when exactly one credential option is set; `default` must be explicit. `service_credential` is rejected, see [Authentication](#authentication). |
| `account_key` | | Account key (`auth_type=key`) |
| `tenant_id`, `client_id`, `client_secret` | | Entra ID service principal (`auth_type=service_principal`) |
| `client_certificate`, `client_certificate_password` | | Service principal with a certificate instead of `client_secret`: the base64 of a PEM (private key and certificate) or PKCS#12 file, and its password if it has one |
| `access_token`, `access_token_expires_on` | | Pre-minted bearer token. Expiry in epoch seconds or ISO-8601; read from the JWT `exp` claim if omitted (`auth_type=access_token`). |
| `preferred_regions` | account default | Comma-separated read regions, e.g. `West Europe,North Europe` |
| `consistency_level` | account default | `Strong`, `BoundedStaleness`, `Session`, `ConsistentPrefix` or `Eventual` (can only weaken the account level) |
| `priority` | | `Low` or `High` ([priority-based execution](https://learn.microsoft.com/azure/cosmos-db/priority-based-execution)). `Low` lets OLTP traffic win under contention. |
| `throughput_bucket` | | Throughput bucket (1 to 5) for every request, to cap this workload at the share of the container's RU/s that the bucket is configured for ([throughput buckets](https://learn.microsoft.com/azure/cosmos-db/throughput-buckets)) |
| `target_ru_per_second` | | RU budget of one read (or one micro-batch): every task waits after a page as long as needed to keep the read at or below it. See [Throughput control](#throughput-control). |
| `connection_timeout_seconds` | SDK default | HTTP request timeout |
| `read_timeout_seconds` | SDK default | Socket read timeout |
| `tls_ca_bundle` | system CAs | Path of a PEM file with the CA certificates to verify the endpoint with, e.g. behind a TLS-inspecting proxy or for an emulator certificate; the file must exist on every Spark node |
| `tls_verify` | `true` | `false` turns certificate verification off (a WARNING is logged). azure-cosmos does not verify `localhost` and `127.0.0.1` anyway. |
| `throttle_max_retries` | `9` | SDK retries on HTTP 429; `0` disables them, so 429s go straight to the retry layer of `max_retries` |
| `throttle_max_wait_seconds` | `60` | Cumulative SDK wait on 429 per request; `0` disables the SDK's 429 retries as well |
| `max_retries` | `8` | Retries after the SDK gives up (408, 410, 429, 449, 5xx, 404 with sub-status 1002, network errors). Reads resume from the last completed page. The driver's metadata requests use the same policy. |
| `retry_max_backoff_seconds` | `60` | Cap of the exponential backoff (jittered; `x-ms-retry-after-ms` is honoured) |

### Batch reads

| Option | Default | Description |
|---|---|---|
| `query` | `SELECT * FROM c` | Query run **per input partition**, for server-side filters and projections. `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP`, `OFFSET` and aggregates apply per partition, not globally; do them in Spark. |
| `query_parameters` | | JSON list of `{"name": "@p", "value": ...}` |
| `page_size` | `5000` | Maximum items per request (the service also caps a page at about 4 MB). Pages of 5,000 cost about 7% fewer RU per document than pages of 1,000. |
| `prefetch_pages` | `1` | Query pages fetched ahead, on one thread per task, while the current page is converted; `0` fetches on demand |
| `arrow_batches` | `auto` | Hand rows to Spark as Arrow record batches, built per page (`auto`: not where the Databricks engine runs the reader, on serverless compute, where the batch layout is not verified) |
| `partitions_per_feed_range` | `1` | Split every physical partition into N EPK sub-ranges, for N times more tasks. Each sub-range query scans its whole physical partition, so a full read costs about N times the RU of `1` (see [Sizing and performance](#sizing-and-performance)). `auto`: one task per physical partition. |
| `mode` | `PERMISSIVE` | Conversion errors: `PERMISSIVE` (null, plus the corrupt-record column), `DROPMALFORMED` or `FAILFAST` |
| `raw_json_column` | `body` | Name of the column holding the whole document |
| `corrupt_record_column` | `_corrupt_record` | Column that receives the JSON of malformed documents (only if it is in the schema) |
| `log_metrics` | `false` | Log per-partition metrics (rows, pages, RU, retries, duration) at WARNING, so they appear in executor logs |
| `slow_request_ms`, `slow_request_log_per_minute` | off, `10` | Log a WARNING for every page fetch slower than this, with activity ids, partitions, request charge, service-side duration and throttling retries; at most `slow_request_log_per_minute` lines per Python worker per minute (the number left out is reported). Batch, snapshot and change-feed reads. |
| `max_integrated_cache_staleness_ms` | | Maximum staleness of results served by the [integrated cache](https://learn.microsoft.com/azure/cosmos-db/integrated-cache) of a dedicated gateway (queries; needs a dedicated-gateway endpoint and Session or Eventual consistency) |
| `infer_schema` | `false` | Infer the schema from a sample of documents instead of returning raw JSON; batch reads only. See [Schema inference](#schema-inference). |
| `sampling_size`, `sampling_query` | `1000`, `query` | Documents sampled for `infer_schema`, and the query that samples them (with `query_parameters`) |
| `infer_system_properties`, `infer_timestamp` | `false` | Include `_rid`, `_self`, `_etag`, `_attachments` and `_ts` (`infer_timestamp`: `_ts` only) in the inferred schema |
| `partition_key` | | Read one logical partition: the key as JSON (`"dev-1"`, `42`, `["tenant1","user7"]` or a prefix such as `["tenant1"]` of a hierarchical key); text that is not JSON is a string key. See [Logical partitions and point reads](#logical-partitions-and-point-reads). |
| `item_ids` | | Read given documents: a JSON list of `["id", pk]` pairs or `{"id": ..., "pk": ...}` objects |
| `filter_pushdown` | `false` | `true`: push Spark filters into the query (PySpark 4.1+; needs `spark.sql.python.filterPushdown.enabled=true`, else planning fails with `DATA_SOURCE_PUSHDOWN_DISABLED`). `auto`: push only while that conf is true. See [Filter pushdown](#filter-pushdown). |

### Filter pushdown

With `filter_pushdown=true` and the Spark conf `spark.sql.python.filterPushdown.enabled=true`, the
filters of a batch read become part of the query, so the service returns only matching documents:

```python
spark.conf.set("spark.sql.python.filterPushdown.enabled", "true")
critical = (
    spark.read.format("cosmosdb")
    .options(**conn, container="events", filter_pushdown="true")
    .load()
    .filter("severity = 'critical'")
)
```

- **Pushed**: `=`, `<=>`, `<`, `<=`, `>`, `>=`, `IN`, `IS NULL`, `LIKE 'a%'`, `LIKE '%a'` and
  `LIKE '%a%'`, each optionally negated, on STRING, integer, DOUBLE and BOOLEAN columns that are
  document properties (struct fields included), and on `pk`, `id`, `_ts` and `_etag`. Spark pushes
  a conjunction of such filters; it does not push `OR`.
- **Semantics are Spark's.** Spark compares converted values, the service raw JSON, so each filter
  is pushed as a condition every matching document meets, with values as query parameters. A
  comparison on a property also lets through documents where the property has another JSON type
  or is missing under its exact name (it may be read case-insensitively), e.g.
  `c["severity"] = @p OR NOT IS_STRING(c["severity"])`, and Spark applies the filter again. Only
  filters on `id`, `_rid`, `_self`, `_etag` (STRING), `_ts` (BIGINT) and `pk = '<string>'` (a string
  that is not also JSON text such as `5` or `true`) are left to the service alone. Measured on an
  account, `severity = 'critical'` (1% of 200,000 documents) read 1,942 documents for 81 RU in
  4 s, where the full scan that Spark would filter costs 4,603 RU and 66 s.
- **`query`** must be `SELECT * FROM <alias> [WHERE ...]`: the filters are added to its `WHERE`.
  Other queries (projections, `VALUE`, `JOIN`, `ORDER BY`, `TOP`, ...) push nothing; Spark filters.
- **One `load()` per filtered query.** Spark 4.1 to 4.3 store the read planned with pushed filters
  on the loaded relation and reuse it for every other scan of that DataFrame or view: a later query
  without pushable filters, or a union or self-join with different filters, reads only the
  documents of the earlier filter, and returns too few rows. Load a new DataFrame for each query
  that filters differently. (Spark's main branch scopes the planned read to its scan.)
- Not pushed: TIMESTAMP, DATE, DECIMAL, FLOAT, BINARY and complex-type columns, `body`,
  `feed_range`, hierarchical `pk`, integers beyond ±2^53, range comparisons with strings containing
  characters from U+D800 up, and `IS NOT NULL` on properties.
- Projections are not pushed: the Python data source API passes no required columns. Use a
  projecting `query` instead.
- Documents a pushed filter excludes are not converted, so with `FAILFAST` a malformed document
  that the filter excludes no longer fails the read.
- PySpark 4.0 has no pushdown API, and the option does nothing there. Where the conf cannot be set
  (Databricks serverless restricts Spark confs), `auto` reads without pushdown and `true` fails.
  The generated query is logged at INFO (without parameter values).

### Logical partitions and point reads

`partition_key` reads one logical partition as one task, with the query routed by partition key;
`query` still applies. `item_ids` reads given documents with point reads (`read_items`), one task
per physical partition that holds some of them; ids that do not exist return no row. For many
identities, `read_many` reads those listed in a DataFrame:

```python
from cosmosdb_datasource import read_many

one_device = spark.read.format("cosmosdb").options(**conn, container="events", partition_key="dev-0000001").load()
wanted = spark.table("lookups").select("id", "deviceId")  # the key in its JSON type
events = read_many(wanted, id_column="id", pk_column="deviceId", **conn, container="events")
```

- With [filter pushdown](#filter-pushdown), `pk = 'v'` and `pk IN (...)` with string keys read only
  those logical partitions, the same way.
- Measured on an account (1M documents, 7 physical partitions): one logical partition of 50
  documents cost 4.3 RU as one task; 1,000 `item_ids` cost 301 RU. A query scoped by the key's
  EPK range instead (`feed_range_from_partition_key`) scans the whole physical partition: 2,386 RU.
- `read_many` returns the batch-read schema or `schema=` (a `StructType` or DDL), runs as a
  `mapInArrow` over the identities (which needs pandas, as Spark's `mapInArrow` does), and reads up
  to 1,000 identities per request. Repeated identities are read once per Arrow batch.
- A prefix of a hierarchical key can span several physical partitions; the service then scans
  them.

### Streaming reads (change feed)

| Option | Default | Description |
|---|---|---|
| `change_feed_mode` | `LatestVersion` | `LatestVersion` or `AllVersionsAndDeletes` (requires continuous backup with the all versions and deletes change feed enabled on the account). Set it on every `readStream`: it also selects the change-feed schema. |
| `start_from` | `Beginning` (LatestVersion), `Now` (AllVersionsAndDeletes) | `Beginning`, `Now` or an ISO-8601 timestamp such as `2026-10-01T00:00:00Z` (no zone means UTC). A timestamp delivers the changes with `_ts` at or after it, to the second; a timestamp in the future starts the stream at the current position and skips the changes made before it. `AllVersionsAndDeletes` supports only `Now`. Used only for a new checkpoint. |
| `max_items_per_trigger` | unlimited | Budget of change-feed **LSNs** per micro-batch, not a row cap: bulk and transactional-batch writes commit many documents under one LSN. Does not apply to the initial snapshot. See [Per-trigger limits](#per-trigger-limits). |
| `initial_snapshot` | `false` | Read the whole container in the first micro-batch, then the change feed from the position the stream started at |
| `initial_snapshot_delay_seconds` | `5` | No snapshot read starts earlier than this after the start position was resolved; protects against replica lag where session tokens do not apply (`ConsistentPrefix`, `Eventual`). `0` disables it. |
| `partitions_per_feed_range` | `1` | EPK sub-splitting of change-feed reads (an integer; `auto` means 1). The initial snapshot is split like a batch read. |
| `page_size` | `5000` | Maximum change-feed items per request |
| `change_feed_end_wait_seconds` | `120` | How long a task waits for a lagging replica to reach the micro-batch's end LSN before it fails (Spark then retries the task) |
| `end_at` | | Batch reads only: read the changes before this ISO-8601 time (LatestVersion) |
| `batch_checkpoint_location` | | Batch reads only: continue from the last committed read; see [Change feed as a batch read](#change-feed-as-a-batch-read) |
| `mode`, `raw_json_column`, `corrupt_record_column`, `log_metrics` | as above | |

`query` is rejected for change-feed streams; filter the stream in Spark.

### Snapshot streams

| Option | Default | Description |
|---|---|---|
| `snapshot_stream` | `false` | `true`: every micro-batch of the `readStream` is one complete snapshot of the container (see [Snapshot stream](#snapshot-stream)). Selects the snapshot-stream schema. |
| `snapshot_interval_seconds` | `0` | Minimum time between the `snapshot_ts` of two snapshots. `0`: a new snapshot whenever Spark asks for new data (one per trigger). Only with `snapshot_stream=true`. |

`query`, `query_parameters`, `partitions_per_feed_range` (`auto` included), `page_size` and the
connection, retry and conversion options apply as for batch reads. `change_feed_mode`,
`start_from`, `initial_snapshot`, `initial_snapshot_delay_seconds`, `max_items_per_trigger` and
`change_feed_end_wait_seconds` are rejected with `snapshot_stream=true`.

## Schemas and type conversion

### Default schemas

Batch reads:

| Column | Type | Content |
|---|---|---|
| `id` | STRING | Document id |
| `pk` | STRING | Partition key value, rendered identically on every row of a document (see [Partition key rendering](#partition-key-rendering)). Nested paths such as `/a/b` work. |
| `_ts` | BIGINT | Last-modified time (epoch seconds) |
| `_etag` | STRING | Document etag |
| `body` | STRING | The whole document as compact JSON, system properties included |

Change-feed streams:

| Column | Type | Content |
|---|---|---|
| `id` | STRING | Document id (for deletes, from the delete metadata) |
| `pk` | STRING | Partition key, rendered as above |
| `operation` | STRING | `upsert` (LatestVersion); `create`, `replace` or `delete` (AllVersionsAndDeletes); `snapshot` for initial-snapshot rows |
| `_ts` | BIGINT | Document `_ts`; for deletes, the delete's `crts` |
| `lsn` | BIGINT | Change LSN, monotonic per physical partition; `0` for snapshot rows |
| `lsn_seq` | INT | Position of the change among the changes of the same document with the same LSN, in feed order (`0`, `1`, ...); `0` for most rows. Several versions of one document share an LSN in a transactional batch, and a multi-region-write conflict arrives as a delete and a create at one LSN. |
| `crts` | BIGINT | Conflict-resolution timestamp (AllVersionsAndDeletes; null otherwise) |
| `ttl_expired` | BOOLEAN | Delete caused by TTL expiry (AllVersionsAndDeletes) |
| `body` | STRING | Document JSON (null for deletes) |
| `feed_range` | STRING | EPK range of the change (`"<min>-<max>"`); for snapshot rows, the feed range the snapshot partition was planned from |

Snapshot streams: the batch schema plus `snapshot_ts` (TIMESTAMP), the same value on every row of
one snapshot. With a user schema, `snapshot_ts` is a virtual column like `pk` (TIMESTAMP,
TIMESTAMP_NTZ or STRING).

Spark does not tell a data source whether `read` or `readStream` asks for its default schema, so
`snapshot_stream=true` selects the snapshot-stream schema, and the change-feed schema is used
whenever a change-feed option (`change_feed_mode`, `start_from`, `max_items_per_trigger`,
`initial_snapshot`, `initial_snapshot_delay_seconds`, `end_at`, `batch_checkpoint_location`) is set.
A batch `read` with them reads the change feed ([Change feed as a batch read](#change-feed-as-a-batch-read)).
A `readStream` without those options and without an explicit schema fails with an explanatory
error.

### Partition key rendering

| Partition key value | `pk` |
|---|---|
| string | the string itself |
| number | the IEEE double the service hashes, without a fraction if integral: `123`, `2.5`; `9007199254740993` renders as `9007199254740992`, because the service treats both as one logical partition and reports the double in delete metadata |
| `true`, `false` | `true`, `false` |
| JSON `null` | `null` |
| undefined (the document has no such property) | NULL |
| hierarchical key | a compact JSON array of the components, `{}` for an undefined component: `["tenant1",7]`, `["tenant1",{}]` |

JSON null and undefined are different logical partitions on the service, so they render
differently. A string that reads like another value (`"null"`, `"123"`) renders like that value.

### Schema inference

With `infer_schema=true` and no `.schema(...)`, the schema is inferred on the driver from up to
`sampling_size` results of `sampling_query` (default: `query`), spread evenly over the physical
partitions:

```python
events = spark.read.format("cosmosdb").options(**conn, container="events", infer_schema="true").load()
events.printSchema()  # id, pk, then every property: seq bigint, device struct<...>, tags array<string>, ...
```

- Columns: `id`, `pk` (if the sampled documents have them), then every property in the order it
  first appears. System properties and `_ts` are left out unless `infer_system_properties` or
  `infer_timestamp` is set. The raw JSON column is not included; declare a schema to have both.
- Types: strings STRING, booleans BOOLEAN, integers BIGINT, other numbers DOUBLE, objects STRUCT,
  arrays ARRAY. Values of one property merge to the tightest type both fit: BIGINT and DOUBLE give
  DOUBLE, structs merge their fields, arrays their elements, and any other mix (for example a
  number in one document and a string in another) gives STRING, which holds numbers, objects and
  arrays as JSON text. A property that is null or an empty array in every sample is STRING.
- Every column is nullable, and the read converts each document to the inferred types like a
  declared schema. A value of another type in a document outside the sample is null, or handled
  as `mode` says.
- The service returns an integral number without a fraction (`140.0` is read as `140`), so a
  property whose sampled values are all integral is BIGINT even if other documents hold
  fractions; those read as null. Raise `sampling_size` or declare the schema.
- Timestamps stored as ISO strings are STRING; convert them in Spark. The sample costs about one
  query page per physical partition (about 23 RU per 1,000 documents of 0.55 KB) when `load()`
  runs, and favours the oldest documents of each partition (queries return them in insertion
  order).

### User-specified schemas

With `.schema(...)` each column is resolved as follows:

- **Virtual columns**: `pk`, `feed_range`, `body` (or the `raw_json_column` name) and the
  corrupt-record column. `body` declared as STRING holds the JSON text; declared as a STRUCT or
  MAP it holds the parsed document. Change-feed streams also have `id`, `_ts`, `operation`, `lsn`,
  `lsn_seq`, `crts` and `ttl_expired`; snapshot streams have `snapshot_ts`.
- **Every other column** is a top-level document property, matched by exact name, then
  case-insensitively. STRUCT, ARRAY and MAP types are converted recursively.
- **Missing properties** are null.

If documents have a property called `body`, set `raw_json_column` to another name.

```python
schema = """id STRING, customerId STRING, seq BIGINT, createdAt TIMESTAMP,
            tags ARRAY<STRING>, address STRUCT<city: STRING, zip: STRING>, _corrupt_record STRING"""
df = spark.read.format("cosmosdb").schema(schema).options(**conn, container="orders").load()
```

Conversions are safe: a value that cannot be represented exactly is an error, never a guess.

| Spark type | Accepted JSON values |
|---|---|
| STRING | any value (objects and arrays as compact JSON; booleans as `true`/`false`) |
| BOOLEAN | `true`/`false`, `"true"`/`"false"` (any case), `0`/`1` |
| TINYINT to BIGINT | integers, integral floats (`42.0`), integer strings; range-checked |
| FLOAT, DOUBLE | numbers and numeric strings |
| DECIMAL(p,s) | numbers and numeric strings, rounded half-up to `s`; precision-checked |
| TIMESTAMP | ISO-8601 strings (7-digit .NET fractions accepted; no zone means UTC), or epoch seconds, milliseconds or microseconds (chosen by magnitude) |
| TIMESTAMP_NTZ | as TIMESTAMP, keeping the wall-clock time |
| DATE | ISO dates or timestamps (the date part as written), epoch numbers |
| BINARY | base64 strings |
| ARRAY, MAP, STRUCT | JSON arrays and objects, recursively |
| VARIANT, intervals | not supported: read as STRING and use `parse_json()` or `from_json()` |

On a conversion failure, `PERMISSIVE` sets the field to null and puts the document JSON into the
corrupt-record column (if it is in the schema), `DROPMALFORMED` skips the row, and `FAILFAST`
fails the read with the document id and the field name. Values are never copied into error
messages.

## Authentication

| `auth_type` | Use for |
|---|---|
| `key` | development and tests; read the key from a secret scope |
| `service_principal` | production jobs and pipelines, on Databricks with the client secret from a secret scope; the only Entra ID mode that refreshes tokens on Databricks |
| `access_token` | tokens minted outside the data source, for example from a Unity Catalog service credential; no refresh, so a run must end within the token's lifetime (about one hour) |
| `default` | local development (`az login`) or compute with a managed identity (`DefaultAzureCredential`); not on Databricks, see below |

Entra ID identities need a Cosmos DB **data-plane** role, such as *Cosmos DB Built-in Data Reader*
(`00000000-0000-0000-0000-000000000001`), on the account, a database or a single container
(`/dbs/<database>/colls/<container>`, enough for every read mode); control-plane roles are not
enough.

```python
# Service principal (needs the identity extra)
opts = {
    "auth_type": "service_principal",
    "tenant_id": "<tenant-id>",
    "client_id": "<client-id>",
    "client_secret": dbutils.secrets.get("<scope>", "<secret-name>"),
}

# Service principal with a certificate: the base64 of the PEM or PKCS#12 file
opts = {
    "auth_type": "service_principal",
    "tenant_id": "<tenant-id>",
    "client_id": "<client-id>",
    "client_certificate": dbutils.secrets.get("<scope>", "<certificate-base64>"),
}

# Unity Catalog service credential: mint a token on the driver (valid for about one hour)
cred = dbutils.credentials.getServiceCredentialsProvider("<credential-name>")
tok = cred.get_token("https://<account>.documents.azure.com/.default")
opts = {"auth_type": "access_token", "access_token": tok.token, "access_token_expires_on": str(tok.expires_on)}
```

For every Entra ID mode, a token for `https://<account>.documents.azure.com/.default` is minted on
the driver when the read is planned: once per batch read, and for every micro-batch of a stream.
It travels inside the input partitions, so executors normally make no Entra ID calls. When a
long-running task gets within 10 minutes of the token's expiry, `service_principal` and `default`
build the credential on the executor, and `access_token` fails with a clear message (no refresh is
possible).

On Databricks (verified on serverless, classic dedicated and standard access mode):
- **Unity Catalog service credentials cannot be used by the data source itself**, so
  `auth_type=service_credential` is rejected with this guidance. Spark plans Python data source
  reads in worker processes where none of the lookups works: `databricks.service_credentials`
  only works in UDF and task processes, and `dbutils` needs a notebook or job context. Planning
  needs a token as well. Mint a token on the driver as shown above (verified on all three compute
  types, with least privilege), or use a service principal for streams that run longer than an
  hour.
- **`default`** finds no customer identity: IMDS is blocked on standard access mode and on
  serverless, and on dedicated mode it returns the workspace's Databricks-managed identity, shared
  by every cluster of the workspace.

Secrets and tokens are never logged and never appear in the `repr()` of options, readers or input
partitions; SDK exception messages pass through a redaction filter. Spark serializes data source
options into the task closures, so a secret passed as an option is in executor memory. Prefer
Entra ID over account keys in production, and disable key authentication on the account once
Entra ID works.

## Change feed

[docs/MODES.md](docs/MODES.md) describes each mode in detail.

### Modes

| | `LatestVersion` | `AllVersionsAndDeletes` |
|---|---|---|
| Rows | the latest version of each created or updated document | every create, replace and delete, including TTL expiry |
| Deletes | not captured | `operation = 'delete'`, `body` null, `pk` and `id` from the metadata |
| Start | `Beginning` (default), `Now` or a timestamp | `Now` only |
| Account | any | continuous backup with the all versions and deletes change feed, enabled after the account was created; history is kept for the backup retention (7 or 30 days). Works on serverless and multi-region-write accounts. |

### Guarantees

- Offsets hold, per EPK range, the LSN up to which changes were handed to Spark. A micro-batch
  reads exactly the changes with `start < lsn <= end` of each range, so a retried or replayed
  batch reads the same changes. With a checkpointed, idempotent sink (Delta tables, SDP streaming
  tables) the result is effectively exactly-once for `AllVersionsAndDeletes`.
- `LatestVersion` keeps only the newest version of a document: if a document changes again between
  a failed attempt and its replay, the replay omits it and the next batch delivers the newer
  version. The final state is correct.
- The first micro-batch's start is stored in its end offset, so a replay after a crash uses the
  original start, even with `start_from=Now`.
- Partition splits re-key the offsets to the new partitions (children inherit the parent's LSN),
  and reads that race a split stay within their own EPK range. Verified against the service:
  streams in both modes, with and without `initial_snapshot` and sub-split ranges, ran through two
  consecutive splits (1 to 2 to 3 partitions) on local Spark and on Databricks serverless, with
  no change lost or delivered twice. Partition merges are not tested.
- If the replica serving a task is behind the batch's end LSN, the task waits (up to
  `change_feed_end_wait_seconds`) instead of returning a short batch.
- A checkpoint belongs to one container instance and one mode: a re-created container or a changed
  `change_feed_mode` fails with an error; use a new checkpoint location.

### Per-trigger limits

- `max_items_per_trigger` is a budget of LSNs, not rows. Bulk and transactional-batch writes commit
  up to 100 documents under one LSN, so a micro-batch can hold up to about 100 times the budget
  (measured: a budget of 50 gave micro-batches of 5,000 rows, and a budget of 500 a first
  micro-batch of 20,300 rows). The LSN advance stays within the budget.
- **OSS PySpark 4.0 and 4.1, and classic DBR 15.4 LTS to 17.3 LTS** have no admission control for
  Python sources: the budget limits each micro-batch's LSN advance, spread across ranges in
  proportion to their backlog, and `trigger(availableNow=True)` runs a single micro-batch per run.
  With `initial_snapshot`, the first run's single batch is the snapshot. For AvailableNow jobs on
  these runtimes, leave `max_items_per_trigger` unset so that each run reads the whole backlog, or
  bound the micro-batches with a `processingTime` trigger instead. Do not combine
  `max_items_per_trigger` with the internal conf
  `spark.sql.streaming.triggerAvailableNowWrapper.enabled=true` on these runtimes.
- **OSS PySpark 4.2, classic DBR 18 and later, serverless (environment versions 2 to 5) and SDP**
  use admission control (`ReadMaxRows`) and native `Trigger.AvailableNow`: one run makes several
  capped micro-batches up to the position captured at its start, then stops. With
  `initial_snapshot`, that position is moved once, right after the snapshot (see
  [Initial snapshot](#initial-snapshot)).
- To work through a large backlog in capped steps in one job on PySpark 4.0 and 4.1 (DBR 17.3),
  loop over [batch reads with a checkpoint](#change-feed-as-a-batch-read) instead.

### Change feed as a batch read

`spark.read` with change-feed options reads the changes between two positions per feed range,
with the schema and rows of a change-feed stream: from `start_from`, or from where the last
committed read of `batch_checkpoint_location` ended, to the position at planning, or to the last
change before `end_at` (LatestVersion only). `max_items_per_trigger` caps one read (an LSN budget).

```python
from cosmosdb_datasource import commit_batch_checkpoint

location = "/Volumes/<catalog>/<schema>/<volume>/orders_cf"
while True:
    changes = (
        spark.read.format("cosmosdb")
        .options(**conn, container="orders", change_feed_mode="LatestVersion", batch_checkpoint_location=location)
        .option("max_items_per_trigger", "100000")
        .load()
    )
    changes.write.mode("append").saveAsTable("<catalog>.<schema>.orders_changes")
    if commit_batch_checkpoint(location):  # True once a read reached the position it was planned to reach
        break
```

- A read plans its range into `offsets/<n>` before reading. Until `commit_batch_checkpoint` writes
  `commits/<n>`, every read with the checkpoint reads that range again, so a failed job repeats
  the same changes (at least once; use an idempotent sink or `MERGE` for exactly once). Spark
  gives a batch read no signal after its output is written, so the commit is the job's call.
- The checkpoint is a directory the driver opens with Python file APIs: a local path, or
  `/Volumes/...` or `/dbfs/...` on Databricks (`dbfs:/` is mapped to `/dbfs/`). Use one job per
  checkpoint. A checkpoint of another container instance or change-feed mode is rejected.
- Without a checkpoint, a read is one bounded range. `AllVersionsAndDeletes` starts at `Now`, so it
  needs a checkpoint: the first read is empty and later reads return the changes since.
- Measured on an account: after 300 creates and 5 deletes, capped reads of 100 LSNs returned the
  295 live documents in 4 reads; an `AllVersionsAndDeletes` read returned 300 creates and 5
  deletes.
- `initial_snapshot` belongs to streams and is rejected; `end_at` and `batch_checkpoint_location`
  are rejected by `readStream`, which keeps its position in its own checkpoint.

### Keys and ordering for CDC

- **Keys**: `["pk", "id"]`.
- **Ordering**: `struct("lsn", "lsn_seq")`, the order of the change feed. With `initial_snapshot`:
  `struct(operation <> 'snapshot', lsn, lsn_seq)`, so that every change outranks every snapshot row.
  `_ts` and `crts` do not order versions that share an LSN, and on multi-region-write accounts `_ts`
  can be older than the conflict resolution it survived (see
  [docs/MODES.md](docs/MODES.md#ordering-sequence_by)).
- **Deletes** (`AllVersionsAndDeletes`): `apply_as_deletes = expr("operation = 'delete'")`.

### Initial snapshot

- The first micro-batch contains only snapshot rows (`operation = 'snapshot'`, `lsn = 0`,
  `crts` null, `ttl_expired = false`), split like a batch read. Its change-feed end equals its
  start, so change rows start with the second micro-batch; `max_items_per_trigger` does not limit
  the snapshot.
- With `Trigger.AvailableNow` on the runtimes with admission control (OSS PySpark 4.2, classic DBR
  18 and later, serverless, SDP), Spark captures the run's end position before the start position
  is resolved. The reader moves it once, right after the snapshot, to the current position of
  every range, so the first run also reads the changes made while the snapshot was read, and one
  triggered update leaves the merged result complete. On OSS PySpark 4.0 and 4.1 and classic DBR
  15.4 to 17.3 a run is a single micro-batch, so the first run ends after the snapshot.
- The snapshot is read only after the start position was resolved and written to Spark's offset
  log. A retry or restart before the first micro-batch is committed reads the snapshot again from
  the same start position (verified with job retries on Databricks serverless and classic: one
  snapshot, and every change made between the attempts); after it is committed, no snapshot is read
  again. A new checkpoint (or a full refresh) starts over with a new start position and a new
  snapshot.
- The option only takes effect for a new checkpoint: enabling it on an existing stream does nothing.
- With `Session` consistency each snapshot query carries the session token recorded when the start
  position was resolved, so no replica behind that position serves it. `Strong` and
  `BoundedStaleness` are not affected by replica lag. With `ConsistentPrefix` or `Eventual` only
  `initial_snapshot_delay_seconds` mitigates replica lag (best effort; 8 of 8 runs with writes
  during the snapshot converged on an account with `Eventual` default consistency, 5 of them with
  no delay, which is evidence, not a guarantee).
- With `LatestVersion`, a document deleted after the start position stays in the merged result if
  the snapshot read it, as deletes are not in that feed.

The correctness argument is in [docs/DESIGN.md](docs/DESIGN.md#initial-snapshot).

## Snapshot stream

Every micro-batch of a `snapshot_stream=true` stream is one complete snapshot of the container:
the rows of a batch read plus `snapshot_ts`. An append flow into a bronze table then keeps a
history of container states, which a separate `create_auto_cdc_from_snapshot_flow` can turn into
SCD2 ([docs/MODES.md](docs/MODES.md#6-snapshot-stream), mode 6).

```python
snapshots = spark.readStream.format("cosmosdb").options(**conn, container="customers", snapshot_stream="true").load()
(
    snapshots.writeStream.option("checkpointLocation", "/Volumes/<catalog>/<schema>/<volume>/customers_snapshots")
    .trigger(availableNow=True)  # one new snapshot per run
    .toTable("<catalog>.<schema>.customers_snapshots")
)
```

- **One snapshot per run.** With `Trigger.AvailableNow`, and in every triggered SDP update, a run
  plans exactly one new snapshot; a run that first replays an uncommitted batch of a failed run
  commits that one as well. Other triggers plan a new snapshot whenever `snapshot_interval_seconds`
  has passed since the previous one. Set it for a continuously running stream, otherwise every
  micro-batch reads the whole container (a WARNING is logged when two snapshots are planned less
  than 60 s apart).
- **`snapshot_ts`** is chosen when the batch is planned (`max(now, previous + 1 ms)`, UTC, whole
  milliseconds), stored in the batch's end offset and copied to every row. A retried task or a
  replay keeps the value; the replayed rows are re-read and reflect the container at replay time.
  Milliseconds, because SDP hands a timestamp snapshot version back to a snapshot-CDC source
  function truncated to milliseconds (observed on DBR 18.3).
- **Atomicity.** One micro-batch is one Delta commit, so a reader sees all rows of a snapshot or
  none.
- **Offsets**: `{"v": 1, "kind": "snapshot", "seq": n, "snapshot_ts": "...", "rid": "..."}`. A
  change-feed checkpoint is rejected by a snapshot stream and vice versa. A re-created container
  (different `_rid`) fails with an explanatory error; reset the stream's checkpoint to continue
  (the target table's history can be kept).
- **Planning** is the batch reader's. Each query carries the session token of a `Now` probe of its
  feed range taken when the batch is planned (about 1 RU per feed range), so with Session
  consistency a snapshot sees every write acknowledged before it was planned.
- **Limits.** Not a point-in-time read, like batch reads. A snapshot of an empty container has no
  rows, so it is invisible downstream. Read limits (`ReadMaxRows`) are ignored: a snapshot is never
  split across micro-batches. Every snapshot costs a full read and a full copy in the target table.

## Throughput control

- `priority=Low` lets OLTP traffic win when the container is throttled (priority-based execution).
- `throughput_bucket=N` sends every request in bucket N; the service caps the bucket at the share
  of the container's throughput configured for it, across every client that uses the bucket.
- `target_ru_per_second` caps one read. Planning splits the budget evenly over the read's tasks;
  the tasks on one executor share a token bucket (a file under the temp directory, locked with
  `flock`) charged with each page's request charge, and sleep while it is in debt. A read stays at
  or below the budget however its tasks are spread, and tasks on one executor use each other's
  unused share; tasks that run in later waves get their share only then. Measured on an account:
  a 1,601 RU read capped at 20 RU/s ran at 18.8 RU/s (85 s; 11.6 s unthrottled).

```python
gentle = (
    spark.read.format("cosmosdb")
    .options(**conn, container="orders", target_ru_per_second="2000", priority="Low")
    .load()
)
```

There is no global throughput control: nothing coordinates the budgets of separate reads, jobs
or other clients of the container. Use throughput buckets, priority, or a container's own
throughput for that.

## Sizing and performance

- **Parallelism**: one task per physical partition. `partitions_per_feed_range=N` splits each into
  N EPK sub-ranges (hash V1, V2 and hierarchical keys are balanced), but the service answers every
  EPK-filtered query by scanning the whole physical partition: each sub-range costs about the RU of
  a scan of the partition, and the scan is not faster. Measured on 10M documents in 17 partitions
  (100,000 RU/s): N = 2, 8 and 16 cost 2.0, 6.3 and 12 times the RU of N = 1, and took as long or
  longer because of throttling. Sub-split only when one task per partition is too slow on the
  Spark side (row conversion) and the RU are affordable; a WARNING states the cost. `auto` reads
  each physical partition as one task. Sub-splitting of change-feed reads has not been measured.
- **Request units**: throughput is shared by all tasks and capped per physical partition (10,000
  RU/s). Measured on 400 RU/s containers with one task per partition: `SELECT *` costs about 23 RU
  per 1,000 documents of 0.55 KB and about 39 RU per 1,000 documents of 1.4 KB; the change feed about 58 RU per 1,000
  changes (LatestVersion from `Beginning`). A full read of 1M documents of 0.55 KB is therefore
  about 23,000 RU, about one minute at 400 RU/s; the same applies to an initial snapshot.
- Raise (auto)scale throughput temporarily for initial loads, use `priority=Low` to protect OLTP
  traffic, and raise `throttle_max_wait_seconds` for long scans of throttled containers.
  `priority` needs priority-based execution enabled on the account; serverless accounts do not
  support it, and the option has no effect there.
- On a high-latency link a request occasionally stalls until the SDK's socket read timeout (65 s)
  before it is retried. `read_timeout_seconds=20` shortens such stalls.
- Planning stays small with many partitions. For 50 physical partitions, `partitions()` took 0.07
  to 0.5 s on Databricks once the client existed, also for 3,200 input partitions
  (`partitions_per_feed_range=64`, about 80 bytes each when pickled). Streams probe every range
  once per trigger, in parallel (about 1 RU each), and their offsets take about 150 bytes per
  range.
- A projecting `query` (`SELECT c.id, c.status, c._ts FROM c`) cuts RU, bandwidth and JSON work
  when the whole document is not needed. A selective filter cuts them far more: push it with
  [filter pushdown](#filter-pushdown) or write it into `query`.
- **Client-side speed**: with one task per physical partition, a task waits for the next page
  and converts the current one. `prefetch_pages=1` overlaps the two, Arrow batches cut PySpark's
  per-value conversion, and pages of 5,000 need a fifth of the round trips. Measured on DBR 17.3 (one
  16-core node, 7 tasks, 1M documents of 0.55 KB, client-bound): a raw read went from 7.6 s to
  5.3 s (131,000 to 190,000 rows/s), a 16-column typed read from 9.2 s to 5.9 s (109,000 to
  170,000 rows/s). The read-ahead thread costs about 15% more CPU; set `prefetch_pages=0` where
  executors' cores, not the service, are the limit.
- Set `preferred_regions` to the region closest to the Spark cluster.
- `log_metrics=true` logs one line per partition with rows, pages, RU, retries and duration
  (for queries also `replans`, splits handled during the read, and `duplicates`, re-created
  documents dropped).
- A query read records 8 bytes per document it returns, to return each key at most once: in
  memory up to about 32 MiB per task, beyond that in an anonymous temporary file (about 400 MB for
  a task of 50 million documents).

## Limitations

- Read-only: there is no writer.
- Batch reads, the initial snapshot and snapshot streams are not point-in-time snapshots: a
  document modified during the scan may be read before or after the change, and one created during
  the scan may be missing. Each `(pk, id)` is returned at most once per read, also when a partition
  splits during the read; a document deleted and re-created during the read is returned in the
  version the read reached first. A projecting `query` without `id` is not de-duplicated.
- `query` runs per partition; global `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP`/`OFFSET` and
  aggregates belong in Spark. Filter pushdown is opt-in, needs PySpark 4.1+ and a Spark conf, and
  needs a fresh `load()` per filtered query on Spark 4.1 to 4.3 (see
  [Filter pushdown](#filter-pushdown)).
- `LatestVersion` does not emit deletes. `AllVersionsAndDeletes` starts only at `Now`, and its
  history is bounded by the continuous-backup retention: a stream stopped longer than that needs a
  new checkpoint.
- `snapshot_stream`: every snapshot is a full read; with `snapshot_interval_seconds=0` a
  continuously running stream reads the whole container in every micro-batch.
- Change-feed requests use Gateway mode (the Python SDK has no Direct mode).
- Partition merges have not been tested.
- `service_principal` with a client secret was run on Databricks (serverless, classic dedicated and
  standard access mode); with a certificate it is unit-tested only. `default` was tested with the
  Azure CLI credential. Unity Catalog service credentials are only usable through a token minted on
  the driver (`auth_type=access_token`).
- No global throughput control across reads or jobs, and no Spark UI custom metrics (the Python
  Data Source API has no hook for them).
- VARIANT and interval types are not produced directly; read STRING and use `parse_json()`.

## Databricks notes

- On serverless compute, Python data source workers run the engine's PySpark (`_engine_pyspark`)
  while `import pyspark` in the same process gives the environment's client PySpark
  (databricks-connect). The streaming runner then passes an engine `StructType` and uses the
  PySpark 4.2 admission-control protocol, which client PySpark 4.0 lacks.
  [`_compat.py`](src/cosmosdb_datasource/_compat.py) bridges both (observed with environment
  version 4: server Spark 4.2.0, client PySpark 4.0.0).
- In SDP `.py` source files, `from databricks.sdk.runtime import dbutils` and
  `dbutils.secrets.get(...)` work (observed on DBR 18.3).
- `reportLatestOffset()` returns the positions probed by the last trigger. Spark shows them as
  `latestOffset` in the query progress and in SDP `stream_progress` events; the difference to
  `endOffset` is the backlog in LSNs per range.
- After a full refresh of only the bronze table of an AUTO CDC flow, the flow fails with
  `DELTA_SOURCE_IGNORE_DELETE` (observed on DBR 18.3): refresh the target together with its source.
- Classic DBR 18 and later takes the admission-control path from its own
  `pyspark.sql.streaming.datasource` (no `_engine_pyspark`). DBR 15.4 to 17.3 take the path of
  OSS PySpark 4.0 and 4.1 (see [Per-trigger limits](#per-trigger-limits)). Standard access mode
  runs batch reads and streams (DBR 17.3).
- Serverless jobs retry a failed task even with `max_retries: 0`, unless the task sets
  `disable_auto_optimization: true`. With `initial_snapshot`, a retry before the first micro-batch
  commits reads the whole container again: correct, but it costs a full read.
- Checkpoints move between runtimes: a stream checkpointed on DBR 17.3 continued on serverless and
  then on DBR 16.4, so a stream can migrate from classic compute to serverless without a new
  checkpoint.
- Also verified on Databricks: an SDP full refresh of an `initial_snapshot` pipeline with writes
  during the refresh converged to the container (classic, DBR 18.3); two streams with separate
  checkpoints on one container did not interfere; two live splits between micro-batches
  (serverless).
- Network: the data source uses Gateway mode only, so it needs HTTPS on port 443 to
  `<account>.documents.azure.com` and to the regional endpoints
  (`<account>-<region>.documents.azure.com`), not the Direct-mode ports of the Java connector. On
  serverless compute a private account is reached through a private endpoint rule in the
  workspace's network connectivity configuration (NCC), with the account's resource ID and
  sub-resource `Sql`; this has not been tested.

## Troubleshooting

| Message | Cause and fix |
|---|---|
| `spark.readStream.format('cosmosdb') needs the change-feed schema` | Set `change_feed_mode` on the stream |
| `... only apply to spark.readStream` | Remove `initial_snapshot`, `initial_snapshot_delay_seconds` and `snapshot_stream` from the batch read |
| `The batch checkpoint has no planned read to commit` | `commit_batch_checkpoint` was called before a read with that `batch_checkpoint_location` was planned |
| `The checkpointed offset belongs to a different container instance` | The container was re-created; use a new checkpoint |
| `checkpoint was created with change_feed_mode=...` | The mode changed; use a new checkpoint |
| `Option(s) ... cannot be combined with snapshot_stream=true` | Remove the change-feed options from the snapshot stream |
| `The checkpoint was written by a change-feed stream, but this stream uses snapshot_stream=true` (or the reverse) | The read mode changed on an existing checkpoint; use a new checkpoint |
| `The checkpointed snapshot stream belongs to a different container instance` | The container was re-created; reset the stream's checkpoint |
| `did not reach LSN ... within 120s` | A replica lagged for a long time; Spark retries the task. Increase `change_feed_end_wait_seconds` or use stronger consistency. |
| HTTP 404 with sub-status 1002, retried while reading the initial snapshot | No replica had caught up with the recorded session token yet; it is retried with backoff (`max_retries`). Persistent failures mean the replicas lag longer than the retry budget. |
| `initial_snapshot=true, but the first micro-batch of this checkpoint was planned without a snapshot` | The checkpoint's first batch was planned without the option; use a new checkpoint location to read a snapshot |
| `Spark called getDefaultReadLimit() ... no SupportsTriggerAvailableNow class was found` | The engine uses the PySpark 4.2 protocol, but no AvailableNow mixin was found at import, so `Trigger.AvailableNow` may run one micro-batch per trigger. Report the modules listed in the message. |
| HTTP 429 after retries | Too little RU for the parallelism: raise throughput or `throttle_max_wait_seconds`, reduce `partitions_per_feed_range`, or use `priority=Low` |
| `azure-identity is required` | Install `azure-identity` or the `identity` extra |
| `auth_type=service_credential is not supported` | Mint a token from the service credential on the driver and use `auth_type=access_token`, or use `auth_type=service_principal` |
| `PIP_INSTALL_NOT_AT_TOP_OF_NOTEBOOK` (SDP on classic compute) | Move the `%pip install` cell to the top of the notebook source |

## Documentation

- [docs/MODES.md](docs/MODES.md): every read mode, including the snapshot stream, with examples, guarantees and the matching
  target-table pattern; choosing between a snapshot and the change feed; ordering, full-refresh
  rules and costs.
- [docs/LHP-GUIDE.md](docs/LHP-GUIDE.md): using the data source with Lakehouse Plumber.
- [examples/databricks-bundle/](examples/databricks-bundle/): a Declarative Automation Bundle with
  snapshot SCD2, LatestVersion SCD1 and AllVersionsAndDeletes + `initial_snapshot` SCD1 pipelines,
  and a reconcile job.
- [examples/lhp/](examples/lhp/): the same patterns as a Lakehouse Plumber project.
- [docs/PARITY.md](docs/PARITY.md): read-side parity with the Azure Cosmos DB Spark connector.
- [docs/DESIGN.md](docs/DESIGN.md): SDK and PySpark findings, offsets, the determinism and
  initial-snapshot arguments, and the test strategy.
- [CHANGELOG.md](CHANGELOG.md), [CONTRIBUTING.md](CONTRIBUTING.md), [RELEASING.md](RELEASING.md),
  [SECURITY.md](SECURITY.md).

## License and trademarks

Licensed under the [Apache License 2.0](LICENSE).

This project is not affiliated with, endorsed by, or sponsored by Microsoft. Microsoft, Azure and
Azure Cosmos DB are trademarks of the Microsoft group of companies. Apache Spark and PySpark are
trademarks of the Apache Software Foundation. Databricks is a trademark of Databricks, Inc.
