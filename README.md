# cosmosdb-spark-datasource

A pure-Python PySpark data source for **Azure Cosmos DB for NoSQL**, built on the PySpark Python
Data Source API (`pyspark.sql.datasource`). It works where JVM connectors such as
`azure-cosmos-spark` cannot be used: **Databricks serverless** notebooks and jobs, and
**Lakeflow Spark Declarative Pipelines (SDP)**.

- **Batch reads**: `spark.read.format("cosmosdb")` runs one Spark task per physical partition (feed range), with optional sub-splitting of large partitions by effective partition key (EPK).
- **Streaming reads** of the **change feed**: `spark.readStream.format("cosmosdb")`, in `LatestVersion` or `AllVersionsAndDeletes` mode. Micro-batches are LSN-bounded, deterministic and replayable. With a checkpointed, idempotent sink (Delta tables, SDP streaming tables) the result is effectively exactly-once for `AllVersionsAndDeletes`; `LatestVersion` delivers the latest state per key, not every version (see [Guarantees](#guarantees)).
- **Initial snapshot** (`initial_snapshot=true`): the first micro-batch of a stream reads the whole container, so one stream yields the current state plus every later change, including deletes with `AllVersionsAndDeletes` (see [Initial snapshot](#initial-snapshot-initial_snapshot)).
- **Auth**: account key, Entra ID service principal, pre-minted access token, Unity Catalog service credential, or `DefaultAzureCredential`. Short-lived tokens are minted on the driver and shipped to tasks.
- **Behaviour**: rows are streamed (never a whole partition in memory), one client is reused per worker process, retries use backoff with jitter and honor `retry-after`, and per-partition metrics (rows, RU, duration) are available.

Distribution: `cosmosdb-spark-datasource` (pure Python `py3-none-any` wheel). Import package:
`cosmosdb_datasource`. Data source short name: `cosmosdb`.

---

## Contents

1. [Requirements](#requirements)
2. [Installation](#installation)
3. [Quick start](#quick-start)
4. [Options](#options)
5. [Schemas and type conversion](#schemas-and-type-conversion)
6. [Authentication](#authentication)
7. [Streaming (change feed)](#streaming-change-feed)
8. [Lakeflow Spark Declarative Pipelines](#lakeflow-spark-declarative-pipelines)
9. [Sizing and performance for large containers](#sizing-and-performance-for-large-containers)
10. [Limitations](#limitations)
11. [Databricks runtime notes](#databricks-runtime-notes)
12. [Troubleshooting](#troubleshooting)
13. [Development](#development)

---

## Requirements

| Component | Version |
|---|---|
| Spark / PySpark | 4.0 or later (Python Data Source API). The test suite passes on OSS PySpark 4.0.1, 4.1.1 and 4.2.0 (Python side, no JVM). **Verified on Databricks** (2026-10-14): serverless jobs with environment version 4 (server Spark 4.2.0, client PySpark 4.0.0, engine `_engine_pyspark` 4.4.0.dev0), and Lakeflow SDP serverless on DBR 18.3. **Use 0.1.1 or later on Databricks** (see [Databricks runtime notes](#databricks-runtime-notes)). |
| Python | 3.10 – 3.12 |
| `azure-cosmos` | `>=4.14.0,<5` (installed automatically). Integration-tested with 4.17.1 (full suite) and 4.14.0 (subset). |
| `azure-identity` | `>=1.19` (optional extra `identity`; needed for `service_principal` and `default` auth) |

PySpark is **not** a dependency of the wheel; Databricks provides it.

## Installation

Build the wheel:

```bash
cd datasource
uv build          # -> dist/cosmosdb_spark_datasource-0.2.0-py3-none-any.whl
```

Upload it to a Unity Catalog volume. Bump the version for every change you deploy: serverless
caches environments by their dependency list, so a new wheel under an old file name is not
guaranteed to be installed. Example path:
`/Volumes/main/cosmos/libs/cosmosdb_spark_datasource-0.2.0-py3-none-any.whl`.

**Serverless notebook / job.** Add the wheel to the notebook's *Environment* side panel
(Dependencies). Add `azure-identity` as well for AAD auth. Alternatively:

```python
%pip install /Volumes/main/cosmos/libs/cosmosdb_spark_datasource-0.2.0-py3-none-any.whl azure-identity
```

**Lakeflow Spark Declarative Pipelines (serverless).** Declare the wheel as a pipeline
environment dependency. Pipeline settings JSON:

```json
"environment": {
  "dependencies": [
    "/Volumes/main/cosmos/libs/cosmosdb_spark_datasource-0.2.0-py3-none-any.whl",
    "azure-identity~=1.19"
  ]
}
```

Avoid `<` and `>` in pipeline dependency strings: on DBR 18.3 SDP failed to install
`azure-identity>=1.19,<2` (`ENVIRONMENT_PIP_INSTALL_ERROR`), while `azure-identity~=1.19` (the same
range) installs.

In a Declarative Automation Bundle this goes under `resources.pipelines.<name>.environment.dependencies`.

## Quick start

```python
from cosmosdb_datasource import CosmosDBDataSource

spark.dataSource.register(CosmosDBDataSource)  # once per Spark session

conn = {
    "endpoint": dbutils.secrets.get("cosmos-ingest", "endpoint"),
    "database": "mydb",
    "auth_type": "key",
    "account_key": dbutils.secrets.get("cosmos-ingest", "account-key"),
}

# Batch: the whole container, one task per physical partition
consults = spark.read.format("cosmosdb").options(**conn, container="consults").load()

# Batch: server-side filter and projection with a parameterized query
recent = (
    spark.read.format("cosmosdb")
    .options(
        **conn,
        container="events",
        query="SELECT * FROM c WHERE c._ts > @since",
        query_parameters='[{"name": "@since", "value": 1790000000}]',
    )
    .load()
)

# Streaming: change feed, LatestVersion, from the beginning
changes = (
    spark.readStream.format("cosmosdb").options(**conn, container="events", change_feed_mode="LatestVersion").load()
)
(
    changes.writeStream.option("checkpointLocation", "/Volumes/main/cosmos/chk/events")
    .trigger(availableNow=True)
    .toTable("main.cosmos.events_changes")
)
```

## Options

Option names are case-insensitive. Unknown options are logged as a warning and ignored.

### Connection and auth

| Option | Default | Description |
|---|---|---|
| `endpoint` | **required** | Account URI, `https://<account>.documents.azure.com:443/` |
| `database` | **required** | Database name |
| `container` | **required** | Container name |
| `auth_type` | inferred | `key`, `service_principal`, `access_token`, `service_credential` or `default`. Inferred when exactly one credential option is set; `default` must be explicit. See [Authentication](#authentication). |
| `account_key` | – | Account key (`auth_type=key`) |
| `tenant_id`, `client_id`, `client_secret` | – | Entra ID service principal (`auth_type=service_principal`) |
| `access_token`, `access_token_expires_on` | – | Pre-minted bearer token. Expiry is in epoch seconds or ISO-8601; if omitted it is read from the JWT `exp` claim (`auth_type=access_token`). |
| `service_credential` | – | Unity Catalog service credential name (`auth_type=service_credential`) |
| `preferred_regions` | account default | Comma-separated read regions, e.g. `Australia East,Australia Southeast` |
| `consistency_level` | account default | `Strong`, `BoundedStaleness`, `Session`, `ConsistentPrefix` or `Eventual` (can only weaken the account level) |
| `priority` | – | `Low` or `High`: [priority-based execution](https://learn.microsoft.com/azure/cosmos-db/priority-based-execution). Use `Low` for bulk reads so OLTP traffic wins under contention. |
| `connection_timeout_seconds` | SDK default | HTTP request timeout |
| `read_timeout_seconds` | SDK default (65) | Socket read timeout |
| `throttle_max_retries` | `9` | SDK retries on HTTP 429 (`retry_throttle_total`) |
| `throttle_max_wait_seconds` | `60` | Cumulative SDK wait on 429 per request (`retry_throttle_backoff_max`) |
| `max_retries` | `8` | Outer retries after the SDK gives up (408/410/429/449/5xx, 404 with sub-status 1002, and network errors). Reads resume from the last completed page. The same bounded policy covers the driver's metadata requests (container properties, feed ranges, change-feed position probes), so throttling does not fail query planning. |
| `retry_max_backoff_seconds` | `60` | Cap for the outer exponential backoff (jittered; `x-ms-retry-after-ms` is honored) |

### Batch reads

| Option | Default | Description |
|---|---|---|
| `query` | `SELECT * FROM c` | Cosmos NoSQL query executed **per input partition**. Use it for server-side filters and projections. `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP` and aggregates apply per partition, not globally, so do those in Spark. |
| `query_parameters` | – | JSON list of `{"name": "@p", "value": ...}` |
| `page_size` | `1000` | Max items per request (the service also caps a page at about 4 MB) |
| `partitions_per_feed_range` | `1` | Split every physical partition into N EPK sub-ranges, giving N times more Spark tasks. `auto` sizes it from the container's stored size (about 1 GiB of documents per task, max 64 per feed range). |
| `mode` | `PERMISSIVE` | Conversion error handling: `PERMISSIVE` (null plus corrupt record), `DROPMALFORMED` or `FAILFAST` |
| `raw_json_column` | `body` | Name of the virtual column carrying the whole document |
| `corrupt_record_column` | `_corrupt_record` | Name of the virtual column receiving the raw JSON of malformed rows (only if present in the schema) |
| `log_metrics` | `false` | Log per-partition metrics (rows, pages, RU, retries, duration) at WARNING so they appear in executor logs |

### Streaming reads (change feed)

| Option | Default | Description |
|---|---|---|
| `change_feed_mode` | `LatestVersion` | `LatestVersion` or `AllVersionsAndDeletes` (AVAD; requires continuous backup on the account). **Set it on every `readStream`**; it also selects the change-feed schema (see [Schemas](#schemas-and-type-conversion)). |
| `start_from` | `Beginning` (LatestVersion), `Now` (AVAD) | `Beginning`, `Now` or an ISO-8601 timestamp (e.g. `2026-10-01T00:00:00Z`; a naive timestamp means UTC). AVAD supports only `Now`. Used only when the checkpoint is new. |
| `max_items_per_trigger` | unlimited | Budget of change-feed **LSNs** per micro-batch, not an item cap. Bulk and transactional-batch writes commit many documents under one LSN, so a batch can hold many more rows than the budget: with a budget of 200, batches of about 2,158 rows were observed (about 10.8×), and bulk-loaded data held up to about 50 documents per LSN. It does not apply to the initial snapshot. See [Per-trigger limits](#per-trigger-limits). |
| `initial_snapshot` | `false` | Read the whole container in the first micro-batch, then the change feed from the position the stream started at. See [Initial snapshot](#initial-snapshot-initial_snapshot). |
| `initial_snapshot_delay_seconds` | `5` | No snapshot read starts earlier than this many seconds after the start position was resolved. Best-effort protection against replica lag where session tokens do not apply (`ConsistentPrefix`, `Eventual`). `0` disables the wait. |
| `partitions_per_feed_range` | `1` | EPK sub-splitting. Change-feed reads take an integer (`auto` means 1 for them); the initial snapshot is split like a batch read, `auto` included. |
| `page_size` | `1000` | Max change-feed items per request |
| `change_feed_end_wait_seconds` | `120` | How long a task waits for a lagging replica to reach the micro-batch's end LSN before failing (Spark then retries the task) |
| `mode`, `raw_json_column`, `corrupt_record_column`, `log_metrics` | as above | |

`query` is not supported for streams; filter the stream in Spark.

## Schemas and type conversion

### Default batch schema

| Column | Type | Content |
|---|---|---|
| `id` | STRING | Document id |
| `pk` | STRING | Partition key value. A single path renders as the string itself, or JSON for numbers and booleans (`123`, `true`). **Hierarchical keys** render as a compact JSON array, e.g. `["tenant1","user7"]`. Nested paths like `/a/b` are supported. |
| `_ts` | BIGINT | Server last-modified time (epoch seconds) |
| `_etag` | STRING | Document etag |
| `body` | STRING | The full document as compact JSON (system properties included) |

### Default change-feed schema

| Column | Type | Content |
|---|---|---|
| `id` | STRING | Document id (for deletes, from the delete metadata) |
| `pk` | STRING | Partition key, rendered as above |
| `operation` | STRING | `upsert` (LatestVersion), or `create` / `replace` / `delete` (AVAD); `snapshot` for rows of the initial snapshot |
| `_ts` | BIGINT | Document `_ts`; for deletes, the delete's `crts` |
| `lsn` | BIGINT | Change LSN (`_lsn` / `metadata.lsn`), monotonic per physical partition; `0` for snapshot rows |
| `crts` | BIGINT | Conflict-resolution timestamp (AVAD; null in LatestVersion and for snapshot rows) |
| `ttl_expired` | BOOLEAN | AVAD delete caused by TTL expiry (`false` for snapshot rows) |
| `body` | STRING | Document JSON (`null` for deletes) |
| `feed_range` | STRING | EPK range of the change (`"<min>-<max>"`), useful for lineage. For snapshot rows, the feed range the snapshot partition was planned from. |

Spark does not tell a data source whether it is being used by `read` or `readStream` when it asks
for the default schema. The change-feed schema is therefore used whenever a change-feed option
(`change_feed_mode`, `start_from`, `max_items_per_trigger`, `initial_snapshot`,
`initial_snapshot_delay_seconds`) is set. `readStream` without those
options and without an explicit schema fails with an explanatory error. A batch `read` with
change-feed options fails too.

### User-specified schemas

Pass `.schema("...")` to get typed columns. Each column resolves as follows:

- **Virtual columns**: `pk`, `feed_range`, `body` (or the `raw_json_column` name) and the corrupt-record column.
  - `body` declared as STRING gives the JSON text. Declared as a STRUCT or MAP, it gives the parsed document.
  - Streams also have `id`, `_ts`, `operation`, `lsn`, `crts` and `ttl_expired`.
- **Everything else** is a **top-level document property**, matched by exact name, then case-insensitively. Nested STRUCT, ARRAY and MAP types are converted recursively.
- **Missing properties** become null.

If a document property is called `body`, set `raw_json_column=_raw` to free that name.

```python
schema = """id STRING, patientId STRING, seq BIGINT, createdAt TIMESTAMP,
            tags ARRAY<STRING>, patient STRUCT<name: STRING, dob: DATE>, _corrupt_record STRING"""
df = spark.read.format("cosmosdb").schema(schema).options(**conn, container="consults").load()
```

Conversions are **safe**: a value that cannot be represented exactly is an error, never a guess.

| Spark type | Accepted JSON values |
|---|---|
| STRING | any value (objects and arrays become compact JSON; booleans `true`/`false`) |
| BOOLEAN | `true`/`false`, `"true"`/`"false"` (any case), `0`/`1` |
| TINYINT … BIGINT | integers, integral floats (`42.0`), integer strings; range-checked |
| FLOAT, DOUBLE | numbers and numeric strings |
| DECIMAL(p,s) | numbers and numeric strings, rounded half-up to `s`; precision-checked |
| TIMESTAMP | ISO-8601 strings (7-digit .NET fractions OK; no zone means UTC), or epoch seconds, milliseconds or microseconds (chosen by magnitude) |
| TIMESTAMP_NTZ | as TIMESTAMP, keeping the wall-clock time |
| DATE | ISO dates or timestamps (the date part as written), epoch numbers |
| BINARY | base64 strings |
| ARRAY / MAP / STRUCT | JSON arrays / objects, recursively |
| VARIANT, intervals | not supported: read as STRING and use `parse_json()` / `from_json()` |

On a conversion failure:
- **`PERMISSIVE`** (default): the field becomes null and `_corrupt_record` (if in the schema) receives the document JSON.
- **`DROPMALFORMED`**: the row is skipped.
- **`FAILFAST`**: the read fails with the document id and field name. Values are not echoed into error messages.

## Authentication

| `auth_type` | Use for | Integration-tested |
|---|---|---|
| `key` | quick starts and spikes. Read the key from a secret scope. | **yes** |
| `default` | local development (`az login`) or compute with a managed identity. Uses `DefaultAzureCredential`. | **yes** (Azure CLI credential) |
| `service_principal` | production jobs and pipelines | no (unit-tested only) |
| `service_credential` | production on Databricks with a Unity Catalog service credential | no (unit-tested only) |
| `access_token` | any case where a token is minted outside Spark | yes, indirectly (same token path as `default`) |

AAD identities need a Cosmos DB **data-plane** RBAC role such as *Cosmos DB Built-in Data
Reader* (`00000000-0000-0000-0000-000000000001`) on the account or database scope. Control-plane
roles are not enough.

### How tokens flow

For every AAD mode, a token for `https://<account>.documents.azure.com/.default` is minted **on
the driver** when the read is planned: once per batch read, and again **every micro-batch** for
streams. It is embedded in each input partition, so executors normally make no Entra ID calls.
If a long-running task gets within 10 minutes of the token's expiry:

- `service_principal`, `default`: the task builds the credential locally and continues.
- `service_credential`: the task looks the credential up again with `databricks.service_credentials`.
- `access_token`: no refresh is possible, so the read fails with a clear message when the token expires. Prefer the other modes for multi-hour scans.

### Examples

```python
# Key from a secret scope (spike / dev)
opts = {"auth_type": "key", "account_key": dbutils.secrets.get("cosmos-ingest", "account-key")}

# Service principal (requires the identity extra / azure-identity)
opts = {
    "auth_type": "service_principal",
    "tenant_id": "<tenant-guid>",
    "client_id": "<app-id>",
    "client_secret": dbutils.secrets.get("cosmos-ingest", "sp-secret"),
}

# Unity Catalog service credential
opts = {"auth_type": "service_credential", "service_credential": "cosmos-reader"}

# Token minted in the notebook (works wherever dbutils works)
cred = dbutils.credentials.getServiceCredentialsProvider("cosmos-reader")
tok = cred.get_token("https://<account>.documents.azure.com/.default")
opts = {"auth_type": "access_token", "access_token": tok.token, "access_token_expires_on": str(tok.expires_on)}
```

`service_credential` resolution tries, in this order:
1. `databricks.service_credentials.getServiceCredentialsProvider(name)`
2. `pyspark.dbutils.DBUtils(spark).credentials`
3. `databricks.sdk.runtime.dbutils`

Whether these are reachable inside Python data source worker processes on serverless and SDP has
**not been verified**. If none is available the read fails with instructions; use the
`access_token` pattern above instead.

### Security guidance

- Keep secrets in a secret scope or service credential, never literals in notebooks. Prefer AAD over account keys for production, and disable local (key) auth on the account once AAD works.
- Secrets and tokens are never logged and never appear in `repr()` of options, readers or input partitions. Exception messages from the SDK are passed through a redaction filter.
- Spark serializes data source options into task closures, so a secret passed as an option is present in executor memory (it is not written to logs). Account keys are long-lived; AAD tokens expire in about 1 hour.

## Streaming (change feed)

### Modes

| | `LatestVersion` | `AllVersionsAndDeletes` |
|---|---|---|
| What you get | the latest version of each created or updated document | every create, replace and delete, including TTL expiry |
| Deletes | **not captured** | `operation = 'delete'`, `body = null`, `pk`/`id` from metadata |
| Start | `Beginning` (default), `Now`, timestamp | `Now` only |
| Account requirement | none | continuous backup with the AVAD change feed enabled; history is retained for the backup retention (7 or 30 days) |

```python
events = (
    spark.readStream.format("cosmosdb")
    .options(
        **conn,
        container="events",
        change_feed_mode="AllVersionsAndDeletes",  # start_from defaults to Now
        max_items_per_trigger="200000",
    )
    .load()
)
```

### Guarantees

- Offsets record, per EPK range, the change LSN up to which data has been handed to Spark. Each micro-batch reads exactly the changes with `start < lsn <= end` per range, so every micro-batch is deterministic and replayable: a restarted or retried batch reads the same range again. With a checkpointed, idempotent sink (Delta tables, SDP streaming tables), the result is therefore **effectively exactly-once for AllVersionsAndDeletes**. LatestVersion delivers the latest state per key, not every version (next point). Behaviour after partition **merges** is unverified (see [Limitations](#limitations)).
- **LatestVersion replay nuance**: the service keeps only the newest version of a document. If a document changes again between a failed attempt and its replay, the replay omits it and the next batch delivers the newer version. The final state is correct; intermediate versions are not part of LatestVersion by definition.
- **Restart before the first batch commits**: the first micro-batch's start is stored in its end offset, so a replay after a crash uses exactly the original start, even with `start_from=Now`.
- **Partition splits** are handled: offsets are re-keyed to the new partitions (children inherit the parent's LSN). Reads that race a split complete per child range and stay within their own EPK range. This is verified against a fake service that reproduces the SDK's behaviour, not against a live partition split.
- **Lagging replicas**: if the replica serving a task is behind the batch's end LSN, the task waits (up to `change_feed_end_wait_seconds`) instead of returning a short batch.
- A checkpoint is tied to the container instance and the mode. A deleted and re-created container, or a changed `change_feed_mode`, fails fast; use a new checkpoint location.

### Per-trigger limits

- `max_items_per_trigger` is a budget of **LSNs**, not of rows: bulk and transactional-batch writes commit many documents under a single LSN (a budget of 200 gave batches of about 2,158 rows, about 10.8×; bulk-loaded data held up to about 50 documents per LSN). Size it in LSNs and expect larger batches after bulk loads.
- **Spark 4.0 / 4.1** (no admission control in the Python API): `max_items_per_trigger` limits each micro-batch's **LSN advance**, spread across ranges in proportion to their backlog. With `trigger(availableNow=True)`, Spark falls back to a **single batch per run** for Python sources on these versions: one capped batch, or everything if no cap is set. The next run continues where it stopped. If the previous run died after planning a batch, the next run only replays that batch, so schedule runs frequently enough. With `initial_snapshot`, the first run's single batch is the snapshot; change-feed rows follow in the next run.
- **Spark 4.2+, Databricks serverless and SDP**: the source supports admission control (`ReadMaxRows`) and `Trigger.AvailableNow` natively. AvailableNow runs several capped batches up to the position captured at start, then stops. Verified on Databricks: one `availableNow` run made 9 capped micro-batches on serverless jobs, and one triggered SDP update made 19.

### Recommended CDC keys and ordering

- **Keys**: `["pk", "id"]`. A document is identified by its partition key plus id.
- **Ordering**: `sequence_by = struct("_ts", "lsn")`. `_ts` is the server timestamp (seconds) and `lsn` breaks ties within the same second. LSNs increase monotonically for a given key, including across splits (children continue the parent's sequence; verified against a fake service that reproduces the SDK's behaviour, not against a live partition split). Ordering by `_ts` first is also meant to protect against LSN discontinuities after partition merges (unverified). With `initial_snapshot`, put the source first: `struct(operation <> 'snapshot', _ts, lsn)` (see [Initial snapshot](#initial-snapshot-initial_snapshot)).
- **Deletes** (AVAD only): `apply_as_deletes = expr("operation = 'delete'")`. For delete rows `_ts` is the delete's `crts`, so `sequence_by` is never null. Verified on Databricks: no AVAD delete row had a null `_ts` (600 in a pipeline's bronze table, 35 in the smoke test), and `_ts = crts` held on every create, replace and delete row.

### Initial snapshot (`initial_snapshot`)

`AllVersionsAndDeletes` can only start at `Now`, so on its own it never returns documents that were
not changed after the stream started. With `initial_snapshot=true` the stream returns them too:
its **first micro-batch reads the whole container** (a snapshot), and every later micro-batch reads
the change feed from the position the stream started at (P0). One stream therefore yields the full
current state plus every later change, deletes included.

```python
events = (
    spark.readStream.format("cosmosdb")
    .options(**conn, container="events", change_feed_mode="AllVersionsAndDeletes", initial_snapshot="true")
    .load()
)
```

It works in both modes and with every `start_from`. With `start_from=Beginning` it is legal but
redundant (the change feed from the beginning already holds the latest version of every document);
an INFO line is logged.

**What the first micro-batch (batch 0) contains.**
- Only snapshot rows: `SELECT * FROM c` for every current feed range, split into
  `partitions_per_feed_range` EPK sub-ranges like a batch read (`auto` included). The `query` option
  stays rejected for streams.
- Snapshot rows use the change-feed schema: `operation = 'snapshot'`, `lsn = 0`, `crts = NULL`,
  `ttl_expired = false`; `id`, `pk`, `_ts` and `body` come from the document; `feed_range` is the
  feed range the snapshot partition was planned from.
- Its change-feed end equals its start, so change-feed rows start with batch 1. Batch 0 always runs,
  even if nothing changed since P0, and `max_items_per_trigger` does not limit it.
- With `Trigger.AvailableNow` on Spark 4.2+, Databricks serverless and SDP, the run reads batch 0
  (the snapshot), then capped change-feed batches up to the position captured when the run started,
  then stops. Spark captures that position before P0 is resolved, so the first run usually ends
  after the snapshot; the next run reads the changes from P0 on.

**Guarantees.**
- The snapshot is read only after P0 was resolved and written to Spark's offset log: batch 0's end
  offset carries P0 and the snapshot record before its read tasks start.
- Retry or restart **before batch 0 is committed**: Spark replays batch 0. The snapshot is read
  again (it then reflects the current state), with the session tokens recorded at the original P0,
  and the change feed still starts at the original P0. The decision to read a snapshot comes from
  the logged batch-0 offset, so changing the option between the attempts does not change the replay.
- **After batch 0 is committed**, no later batch and no restart reads a snapshot again.
- **New checkpoint or full refresh**: the stream starts over, with a new P0 and a new snapshot in its
  batch 0. Snapshot and P0 are always reset together, so no change between them can be lost.
- The option only takes effect for a checkpoint whose batch 0 has not been planned yet. Enabling it
  on an existing stream does nothing (a WARNING is logged if the first batch is replayed); offsets
  written by 0.1.x mean "no snapshot".

**Merging snapshot and change rows.** Order the versions of a key (`pk`, `id`) by
`struct(operation <> 'snapshot', _ts, lsn)` and keep the last one; if that is a `delete`, the
document is absent. Every change-feed row outranks every snapshot row, whatever the timestamps. With
`AllVersionsAndDeletes` the merged result equals the container's state as of the last change read:
- A document **changed after P0** has every one of those changes in the change feed. The latest of
  them outranks any snapshot row of the document, and it is the current version (or its delete).
- A document **not changed after P0** has no change-feed row. Its current version is its version at
  P0, and the snapshot returns it: the snapshot is read after P0 from replicas that have applied
  every write up to P0 (see the consistency notes below), and a query that runs to completion
  returns every document that is not modified while it runs. The second point is how the service's
  query continuation behaves (it resumes after the last document returned); it is consistent with
  the live test but not a documented service guarantee.

With `LatestVersion` the same holds for documents that exist, but deletes are not in that feed: a
document deleted after P0 stays in the merged result if the snapshot read it, as deleted documents
do with `LatestVersion` in general. The rule is verified by a property test against the fake
service (random inserts, updates, deletes and re-creates interleaved with the snapshot's page
reads) and by a live test that writes concurrently with the snapshot and compares the merged state
with the container.

**Consistency and replica lag.** With `Session`, `ConsistentPrefix` or `Eventual` consistency a read
may be served by a replica that has not applied every committed write. A snapshot query served by a
replica that is behind P0 would miss writes made just before P0, and those writes would be in
neither source.
- **Session** (the default consistency of most accounts): when P0 is resolved, the session token
  returned with each start probe is recorded in the offsets (its LSN is the probe's LSN), and every
  snapshot query sends it. The service serves such a query only from a replica that has applied
  those writes and otherwise answers 404/1002, which is retried. Verified against the service: a
  token ahead of the service is refused with 404/1002, and the live test checks that every snapshot
  query carries the recorded token.
- **Strong** and **Bounded Staleness** use quorum reads, which see every acknowledged write, so they
  are not affected.
- **ConsistentPrefix** and **Eventual** (as the account default, or through `consistency_level`):
  the service ignores session tokens. The only mitigation is `initial_snapshot_delay_seconds`
  (default 5): no snapshot read starts earlier than that many seconds after P0 was resolved, which
  gives lagging replicas time to catch up. This lowers the risk; it is not a guarantee. Prefer
  Session or stronger for the stream.
- After a **partition split** between P0 and the snapshot, the parent partition's session token is
  sent for the child ranges, as the SDK itself does. Whether the service applies a parent's token to a
  child partition was not verified (a split could not be triggered on the test account).

**Cost and time.** The snapshot costs what a batch read of the container costs: about 23 RU per
1,000 documents of about 0.55 KB and about 39 RU per 1,000 documents of about 1.4 KB (see
[Sizing](#sizing-and-performance-for-large-containers); RU scales roughly with document size). For
1M documents of about 0.55 KB that is about 23,000 RU: about one minute at 400 RU/s if nothing else
uses the throughput, and correspondingly less with more throughput and parallelism (feed ranges ×
`partitions_per_feed_range` tasks). Add `initial_snapshot_delay_seconds` once. Changes keep
accumulating in the change feed while the snapshot runs; batch 1 starts at P0 and is limited by
`max_items_per_trigger`. With `AllVersionsAndDeletes`, batch 1 must start within the account's
continuous-backup retention (7 or 30 days) of P0. Use `priority=Low` to protect OLTP traffic.

## Lakeflow Spark Declarative Pipelines

Register the data source at the top of the pipeline source file. Pipelines run the file in
their own session. The examples read the key with `dbutils.secrets.get`. If your pipeline runtime
does not provide `dbutils` in plain `.py` source files, import it with
`from databricks.sdk.runtime import dbutils`, or use a service credential / service principal.

### Change-feed pattern: one stream with an initial snapshot, then `create_auto_cdc_flow`

Full current state plus deletes, from a single stream: AVAD with `initial_snapshot=true` lands in a
bronze streaming table (batch 0 is the snapshot, later batches are changes), and an AUTO CDC flow
applies it to the target with the merge rule from [Initial snapshot](#initial-snapshot-initial_snapshot).

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct
from cosmosdb_datasource import CosmosDBDataSource

spark.dataSource.register(CosmosDBDataSource)

COSMOS = {
    "endpoint": spark.conf.get("cosmos.endpoint"),
    "database": "mydb",
    "auth_type": "key",
    "account_key": dbutils.secrets.get("cosmos-ingest", "account-key"),
}


@dp.table(name="events_bronze", comment="Cosmos DB events: initial snapshot (batch 0), then the AVAD change feed")
def events_bronze():
    return (
        spark.readStream.format("cosmosdb")
        .options(
            **COSMOS,
            container="events",
            change_feed_mode="AllVersionsAndDeletes",  # start_from defaults to Now
            initial_snapshot="true",
            max_items_per_trigger="200000",  # LSN budget for change-feed batches; the snapshot is not capped
            priority="Low",
        )
        .load()
    )


dp.create_streaming_table("events_current")
dp.create_auto_cdc_flow(
    target="events_current",
    source="events_bronze",
    keys=["pk", "id"],
    # rank 0 = snapshot row, 1 = change-feed row: every change outranks every snapshot row
    sequence_by=struct((col("operation") != "snapshot").cast("int").alias("rank"), col("_ts"), col("lsn")),
    apply_as_deletes=expr("operation = 'delete'"),
    except_column_list=["operation", "crts", "ttl_expired", "feed_range"],
    stored_as_scd_type="1",
)
```

Parse the JSON downstream, e.g. `parse_json(body)` (VARIANT), or `from_json(body, schema)`, or
declare a typed `.schema(...)` on the reader.

- No separate backfill flow is needed: the snapshot is part of the stream's first micro-batch, so a
  full refresh of `events_bronze` (which resets its checkpoint) always takes a new P0 and a new
  snapshot together. Refresh `events_current` together with `events_bronze` (for example with a full
  refresh of the pipeline), as for any AUTO CDC target whose source table is rewritten: after a full
  refresh of the bronze table alone, the AUTO CDC flow fails at the next update with
  `DELTA_SOURCE_IGNORE_DELETE` (observed on DBR 18.3).
- This pattern was run end to end on Databricks serverless SDP (DBR 18.3) with 1M documents, writes
  during the snapshot, a stop during the snapshot and reconciliation against the container; see the
  bundle's `docs/TEST-REPORT.md`.
- For delete rows `_ts` is the delete's `crts`, so `sequence_by` is never null.
- Without deletes (or with soft deletes), a `LatestVersion` stream from `Beginning` gives the
  current state without a snapshot; `sequence_by=struct("_ts", "lsn")` is then enough.

### Snapshot pattern: `create_auto_cdc_from_snapshot_flow` (periodic full snapshots)

Use this for small and medium containers, or when deletes must be detected without AVAD. Every
pipeline update reads the whole container and the flow diffs it against the previous snapshot.

```python
@dp.temporary_view(name="consults_snapshot")
def consults_snapshot():
    return spark.read.format("cosmosdb").options(**COSMOS, container="consults", priority="Low").load()


dp.create_streaming_table("consults_current")
dp.create_auto_cdc_from_snapshot_flow(
    target="consults_current",
    source="consults_snapshot",
    keys=["pk", "id"],
    stored_as_scd_type="1",  # or 2 to keep history
)
```

A full snapshot costs RU proportional to the container size on every update (see below). Prefer
the change-feed pattern above a few GB.

## Sizing and performance for large containers

**Parallelism.**
- Each physical partition (feed range) becomes one Spark task. `partitions_per_feed_range=N` splits each partition into N EPK sub-ranges; the service filters by EPK, so the sub-ranges are disjoint and complete.
- Use `auto` to target about 1 GiB of stored documents per task.
- Stop increasing N once the RU budget, not task count, is the bottleneck.
- Balanced splitting supports hash V1 (common for older or CLI-created containers), V2 and hierarchical keys.

**RU budget.** Throughput is shared by all tasks and capped per physical partition (10,000 RU/s each). Measured on the test account (400 RU/s containers); RU scales roughly with document size:
- `SELECT *`: about **23 RU per 1,000 documents** of about 0.55 KB (543 bytes of user properties, 748 bytes of JSON with system properties), and about **39 RU per 1,000 documents** of about 1.4 KB (1,353 / 1,558 bytes).
- Change feed: about **57.6 RU per 1,000 changes**, measured on a `LatestVersion` initial load (1M documents of about 0.55 KB, change feed from `Beginning`). The per-change cost of `AllVersionsAndDeletes` was not measured separately.
- Projection queries are cheaper per byte (`SELECT c.id`: about 68 RU per 5,000 documents).
- A full read of 1M documents of about 0.55 KB therefore needs about 23k RU: about 1 minute at 400 RU/s if nothing else runs, versus seconds at 20k RU/s. The same applies to an initial snapshot.
- For initial loads of tens or hundreds of GB, temporarily raise (auto)scale throughput.
- Use `priority=Low` (with priority-based execution enabled) to protect OLTP traffic.
- Raise `throttle_max_wait_seconds` (e.g. 300) for long scans on throttled containers.

**Other knobs.**
- `page_size`: 1000 is a good default. Larger pages mean fewer round trips; the service caps a page at about 4 MB, so very large documents need smaller pages to stay responsive.
- **Projection.** Use a projecting `query` (e.g. `SELECT c.id, c.status, c._ts FROM c`) when the whole document is not needed; it cuts bandwidth and JSON work.
- **Streaming backfills**: use `partitions_per_feed_range` for parallelism and `max_items_per_trigger` for bounded micro-batches. The first batch from `Beginning` is a full read of the container's latest versions; so is the first batch with `initial_snapshot`.
- **Regions.** Set `preferred_regions` to the region closest to the workspace.
- **Consistency.** Reading at `Strong`/`BoundedStaleness` (if the account allows it) reduces replica-lag waits in streams. `Session`/`Eventual` are fine; lag is waited out automatically.
- **Monitoring**: `log_metrics=true` logs one line per partition with rows, pages, RU, retries and duration.

## Limitations

- Read-only (no writer).
- Batch reads, and the initial snapshot, are not point-in-time snapshots: documents modified during a scan may be read before or after the change. Use the change feed for consistent incremental ingestion; with `initial_snapshot`, the change feed resolves those documents (see the merge rule).
- `query` runs per partition. Global `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP`/`OFFSET` and aggregates must be done in Spark. Spark filters are not pushed down; put server-side filters in `query`. Filter pushdown in PySpark 4.1+ requires a Spark conf that serverless does not expose, and enabling it partially would break reads.
- LatestVersion does not emit deletes. AVAD supports only `start_from=Now`, and its history is bounded by the continuous-backup retention. A stream stopped longer than that must restart with a new checkpoint (with `initial_snapshot=true` the new stream reads a new snapshot).
- `initial_snapshot` against `ConsistentPrefix` or `Eventual` consistency is protected only by `initial_snapshot_delay_seconds` (best effort); see [Initial snapshot](#initial-snapshot-initial_snapshot). Session tokens across a partition split are unverified.
- `max_items_per_trigger` is an LSN budget, not a row cap. On PySpark 4.0/4.1 it must not be combined with the internal conf `spark.sql.streaming.triggerAvailableNowWrapper.enabled=true`.
- Change feed requests are made in Gateway mode (the Python SDK has no Direct mode).
- Partition **merge** (preview) has not been tested against the service. Partition **splits** are tested against a fake service that reproduces the SDK's behaviour, not against a live split.
- `service_principal` and `service_credential` auth are unit-tested but **not integration-tested**. Service credentials inside Python data source workers on serverless/SDP are unverified.
- VARIANT and interval types are not produced directly. Read STRING and use `parse_json()`.

## Databricks runtime notes

Observed on Azure Databricks on 2026-10-14 (`../docs/TEST-REPORT.md` has the evidence).

- **Two PySpark builds in one worker (serverless jobs).** Python data source workers run the engine's PySpark, packaged as `_engine_pyspark` (4.4.0.dev0). `import pyspark` in the same process resolves to the environment's client PySpark (databricks-connect, 4.0.0). As a result:
  - The streaming runner passes an *engine* `StructType` to `DataSource.streamReader()`. The batch planner converts it to client classes; the streaming runner does not.
  - The engine speaks the 4.2+ admission-control protocol. It calls `getDefaultReadLimit()`, `latestOffset(start, limit)` and `reportLatestOffset()`, and checks its own `SupportsTriggerAvailableNow`. Client PySpark 4.0 has none of these.

  0.1.0 failed every `readStream` there. 0.1.1 bridges both gaps in `cosmosdb_datasource/_compat.py`:
  - it converts foreign schemas through JSON;
  - it takes `ReadLimit` and `SupportsTriggerAvailableNow` from `_engine_pyspark` when that is loaded, then from `pyspark`, then from local stand-ins with the same names and fields;
  - it implements `reportLatestOffset()`.

  `tests/test_databricks_engine_compat.py` replays this setup.
- **Lakeflow SDP (serverless, DBR 18.3).** Pipeline Python runs in a classic PySpark session behind a Py4J allow-list. Some catalog calls are blocked, for example `spark.catalog.tableExists`; `spark.sql(...)` works. `from databricks.sdk.runtime import dbutils` plus `dbutils.secrets.get(...)` work in `.py` pipeline sources. The pipeline's `environment.dependencies` installs the wheel and `azure-cosmos`.
- **Progress reporting.** `reportLatestOffset()` returns the head positions probed by the last trigger. Spark shows them as `latestOffset` next to `endOffset` in the query progress, and in SDP `stream_progress` events. The difference is the backlog in LSNs per range.

## Troubleshooting

| Message | Cause / fix |
|---|---|
| `Spark type string is not supported by the cosmosdb source` on `readStream` (Databricks serverless) | 0.1.0 with the engine's foreign `StructType`: upgrade to 0.1.1 or later |
| `'CosmosStreamReader' object has no attribute 'reportLatestOffset'` (Databricks) | 0.1.0 with the engine's admission-control protocol: upgrade to 0.1.1 or later |
| `spark.readStream.format('cosmosdb') needs the change-feed schema` | Set `change_feed_mode` (e.g. `LatestVersion`) on the stream |
| `... only apply to spark.readStream` | Remove `change_feed_mode` / `start_from` / `max_items_per_trigger` from a batch read |
| `The checkpointed offset belongs to a different container instance` | The container was re-created; use a new checkpoint |
| `checkpoint was created with change_feed_mode=...` | Mode changed; use a new checkpoint |
| `did not reach LSN ... within 120s` | A replica lagged for a long time; Spark retries the task. Increase `change_feed_end_wait_seconds` or use stronger consistency. |
| HTTP 404 with sub-status 1002 retried while reading the initial snapshot | No replica had caught up with the session token recorded at the start yet; retried with backoff (`max_retries`). Persistent failures mean the replicas lag for longer than the retry budget. |
| `initial_snapshot=true, but the first micro-batch of this checkpoint was planned without a snapshot` | The checkpoint's first batch was planned without the option (or by 0.1.x). No snapshot is read; use a new checkpoint location to get one. |
| `Spark called getDefaultReadLimit() ... no SupportsTriggerAvailableNow class was found` | The engine speaks the 4.2+ protocol but the reader was imported without the engine's AvailableNow mixin, so `Trigger.AvailableNow` may run one micro-batch per trigger. Report the modules listed in the message. |
| HTTP 429 after retries | RU budget too small for the parallelism. Raise throughput or `throttle_max_wait_seconds`, reduce `partitions_per_feed_range`, or use `priority=Low`. |
| `azure-identity is required` | Install `azure-identity` (or the `[identity]` extra) |
| `service credential ... is not available in this Python process` | Use `auth_type=access_token` with a token minted in the notebook |

## Development

```bash
cd datasource
uv sync                                    # creates .venv with dev dependencies (pyspark, pytest, ruff, pyarrow)
uv run python -m pytest                    # unit + JVM-free Spark-protocol tests (integration excluded by default)
uv run python -m pytest -m integration     # real account: needs ../config/cosmos.local.json and az login / COSMOS_KEY;
                                           # reads consults/events, writes only to temporary it_* containers
uv run ruff check && uv run ruff format --check
uv build                        # dist/*.whl (py3-none-any)
```

- `tests/harness.py` replays the PySpark driver/executor call sequence without a JVM (pickling, JSON offsets, restart semantics, Arrow row validation).
- `tests/fake_cosmos.py` is a service-faithful fake: EPK routing, LSN logs, LatestVersion/AVAD, splits, replica lag (change feed and queries), session tokens, resume-key query paging, throttling.
- `tests/test_initial_snapshot.py` covers `initial_snapshot`, including a property-style convergence test (random writes interleaved with the snapshot's page reads, merged with the documented rule) that also checks that sabotaged merges are detected.
- Local SparkSession tests run when Java 17+ is installed.
- `docs/DESIGN.md` records the SDK and PySpark source research, the offset design and the determinism argument.
