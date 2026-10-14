# Read modes

The `cosmosdb` data source reads a container in five ways. This page describes what each mode
emits, what it guarantees, when to use it and which target-table pattern goes with it, followed by
a decision guide, the ordering rule, the full-refresh rules and costs. Option details are in the
[README](../README.md#options); the Lakehouse Plumber setup is in [LHP-GUIDE.md](LHP-GUIDE.md).

| # | Mode | Options | `operation` | Deletes | Account requirements | Target pattern |
|---|---|---|---|---|---|---|
| 1 | Batch read | `spark.read` (optional `query`) | (no column) | by diffing two reads | none | `create_auto_cdc_from_snapshot_flow` (SCD1 or SCD2) |
| 2 | LatestVersion | `change_feed_mode=LatestVersion`, `start_from=Beginning\|Now\|<ISO-8601>` | `upsert` | no | none | `create_auto_cdc_flow`, `sequence_by struct(_ts, lsn)` |
| 3 | AllVersionsAndDeletes (AVAD) | `change_feed_mode=AllVersionsAndDeletes` (`start_from=Now` only) | `create`, `replace`, `delete` | yes | continuous backup with the AVAD change feed; no partition merge | an append-only change log |
| 4 | AVAD + initial snapshot | mode 3 plus `initial_snapshot=true` | `snapshot`, then `create`, `replace`, `delete` | yes | as mode 3 | `create_auto_cdc_flow`, `sequence_by struct(rank, _ts, lsn)`, `apply_as_deletes` |
| 5 | LatestVersion + initial snapshot | mode 2 plus `initial_snapshot=true` | `snapshot`, then `upsert` | no | none | `create_auto_cdc_flow`, `sequence_by struct(rank, _ts, lsn)` |

What has run where:

| Mode | Databricks (serverless SDP, DBR 18.3) | Azure Cosmos DB account | Emulators in CI |
|---|---|---|---|
| 1 | yes | yes | both |
| 2 | yes | yes | both |
| 3 | yes | yes | no (no emulator supports AVAD) |
| 4 | yes | yes | no |
| 5 | no | no | both |

All modes are also covered by unit tests against a service fake (`tests/fake_cosmos.py`).

## Common setup

The examples assume these definitions. In a notebook or job:

```python
from cosmosdb_datasource import CosmosDBDataSource

spark.dataSource.register(CosmosDBDataSource)  # once per session
conn = {
    "endpoint": dbutils.secrets.get("<scope>", "endpoint"),
    "database": "<database>",
    "auth_type": "key",
    "account_key": dbutils.secrets.get("<scope>", "account-key"),
}
```

In a Lakeflow Spark Declarative Pipelines (SDP) source file:

```python
from databricks.sdk.runtime import dbutils
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from cosmosdb_datasource import CosmosDBDataSource

spark.dataSource.register(CosmosDBDataSource)
COSMOS = {
    "endpoint": dbutils.secrets.get("<scope>", "endpoint"),
    "database": "<database>",
    "auth_type": "key",
    "account_key": dbutils.secrets.get("<scope>", "account-key"),
}
```

With Lakehouse Plumber, a preset adds the connection to every `custom_datasource` load, and each
flowgroup only sets the container and the mode options (see [LHP-GUIDE.md](LHP-GUIDE.md)).

Schemas:

- Batch reads: `id, pk, _ts, _etag, body`.
- Change-feed streams: `id, pk, operation, _ts, lsn, crts, ttl_expired, body, feed_range`.
- `body` is the whole document as JSON; keys are `(pk, id)`.

In streams, `operation` is an ordinary string column. Once it lands in bronze, silver uses it for
the ordering rank (see [Ordering](#ordering-sequence_by)) and for the delete condition
`operation = 'delete'`.

## 1. Batch read

**Emits** every document of the container, one row each, without an `operation` column. The
optional `query` (for example `SELECT * FROM c WHERE c.type = 'order'`) runs once per input
partition, so `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP` and aggregates apply per partition.

**Guarantees and limitations.**
- One Spark task per physical partition, optionally split by EPK (`partitions_per_feed_range`,
  `auto` targets about 1 GiB per task). Rows are streamed page by page; transient errors resume
  from the last page.
- Not a point-in-time snapshot: a document changed during the scan may be read before or after the
  change. A document that is not modified during the scan is read exactly once; this is how the
  service's query continuation behaves in tests, not a documented service guarantee.
- Deletes show up only as a key missing from the next read.

**Use it for** small and medium containers read on a schedule, deletes without AVAD, and
reconciliation (a full compare of a container with a table, see the
[reconcile job](../examples/databricks-bundle/README.md#reconcile-job)).

```python
customers = (
    spark.read.format("cosmosdb")
    .options(**conn, container="customers", partitions_per_feed_range="auto", priority="Low")
    .load()
)
```

SDP, snapshot to SCD2 ([example](../examples/databricks-bundle/src/pipelines/customers_scd2/customers_scd2.py)):

```python
@dp.temporary_view(name="customers_snapshot")
def customers_snapshot():
    # Document data only: an ingest timestamp here would create a new SCD2 version of every row on every update.
    return spark.read.format("cosmosdb").options(**COSMOS, container="customers").load()


dp.create_streaming_table("customers_scd2")
dp.create_auto_cdc_from_snapshot_flow(
    target="customers_scd2",
    source="customers_snapshot",
    keys=["pk", "id"],
    stored_as_scd_type=2,
)
```

**Target pattern.** AUTO CDC FROM SNAPSHOT diffs each read with the previous one by `(pk, id)`: a
new key opens a current row, changed columns close the current row and open a new one, and a
missing key closes the row (a hard delete). `__START_AT` and `__END_AT` hold the update time. There
is no bronze history, so a full refresh restarts the history from the current state. A read that
returns too few documents (a wrong container, a truncated source) closes every missing row; guard
against it, for example by comparing the container's document count with the table's current rows
before the update.

**Cost.** The whole container on every run: about 23 RU per 1,000 documents of 0.55 KB, about 39
RU per 1,000 documents of 1.4 KB.

## 2. LatestVersion change feed

**Emits** the latest version of every created or updated document since the start position, with
`operation = 'upsert'`; `crts` is null. With `start_from=Beginning` (the default) the first
micro-batch returns every live document; with `Now` or a timestamp, only later changes.

**Guarantees and limitations.**
- Micro-batches are bounded by LSN per feed range, deterministic and replayable.
- Latest state, not every version: if a document changes again between a failed attempt and its
  replay, the replay skips it and the next micro-batch has the newer version.
- Hard deletes never appear, so deleted documents stay in the target table.
- `max_items_per_trigger` is an LSN budget, not a row cap: bulk and transactional writes put many
  documents under one LSN.
- No account requirements: the latest-version feed is always on for the NoSQL API.

**Use it for** large containers without hard deletes (soft deletes, or TTL handled by a periodic
reconciliation).

```python
changes = (
    spark.readStream.format("cosmosdb")
    .options(**conn, container="orders", change_feed_mode="LatestVersion", start_from="Beginning")
    .load()
)
(
    changes.writeStream.option("checkpointLocation", "/Volumes/<catalog>/<schema>/<volume>/orders_latest")
    .trigger(availableNow=True)
    .toTable("<catalog>.<schema>.orders_changes_latest")
)
```

SDP, bronze plus SCD1 ([example](../examples/databricks-bundle/src/pipelines/orders_latest/)):

```python
@dp.table(name="orders_changes_latest")
def orders_changes_latest():
    return (
        spark.readStream.format("cosmosdb")
        .options(**COSMOS, container="orders", change_feed_mode="LatestVersion", start_from="Beginning")
        .load()
    )


dp.create_streaming_table("orders_latest")
dp.create_auto_cdc_flow(
    target="orders_latest",
    source="orders_changes_latest",
    keys=["pk", "id"],
    sequence_by=F.struct("_ts", "lsn"),
    stored_as_scd_type=1,
    except_column_list=["operation", "crts", "ttl_expired", "feed_range"],
)
```

**Target pattern.** Append-only bronze, then SCD1 with `sequence_by struct(_ts, lsn)` and no
deletes.

**Cost.** The initial load from `Beginning` measured about 58 RU per 1,000 documents of 0.55 KB
(about 58,000 RU for 1M documents). Later micro-batches cost O(changes) plus about 1 RU per feed
range for the position probes. Mode 5 makes the initial load a query read instead.

## 3. AllVersionsAndDeletes (AVAD) change feed

**Emits** every create, replace and delete after the start position: `operation` is `create`,
`replace` or `delete`. For deletes `body` is null, `pk` and `id` come from the delete metadata and
`_ts` is the delete's `crts`. TTL deletes set `ttl_expired = true` (documented by the service; not
observed in tests, because TTL purges are asynchronous).

**Guarantees and limitations.**
- Micro-batches are deterministic and replayable, and every version is kept, so a checkpointed,
  idempotent sink gets each change effectively exactly once.
- Starts at `Now` only: on its own it never returns documents that were not changed after the
  stream started. It is a change log, not a replica; use mode 4 for a replica.
- The account needs continuous backup with the AVAD change feed enabled. Accounts that have ever
  merged a partition, or have partition merge enabled, are not supported by the service.
- History is readable only within the continuous-backup retention (7 or 30 days). A stream stopped
  for longer must start over with a new checkpoint: alert on staleness.

**Use it for** processing changes after "now", for example as an audit log of new writes.

```python
log = (
    spark.readStream.format("cosmosdb")
    .options(**conn, container="orders", change_feed_mode="AllVersionsAndDeletes")  # start_from defaults to Now
    .load()
)
(
    log.writeStream.option("checkpointLocation", "/Volumes/<catalog>/<schema>/<volume>/orders_log")
    .trigger(availableNow=True)
    .toTable("<catalog>.<schema>.orders_change_log")
)
```

**Target pattern.** An append-only change log. A current-state table built from it alone misses
every document not changed since the start.

**Cost.** O(changes) plus about 1 RU per feed range per micro-batch for the position probes. The
per-change cost of AVAD has not been measured.

## 4. AVAD + initial snapshot

**Emits**
- in micro-batch 0, a snapshot of the whole container: `operation = 'snapshot'`, `lsn = 0`,
  `crts` null, `ttl_expired = false`, with `id`, `pk`, `_ts` and `body` from the document;
- from micro-batch 1 on, the AVAD changes (`create`, `replace`, `delete`) from the start position
  P0.

One stream yields the full current state plus every later change, deletes included.

**Guarantees.**
- P0 is durable before the snapshot is read: it is resolved first, recorded in micro-batch 0's end
  offset in Spark's offset log, and only then is the snapshot read.
- A retry or restart before micro-batch 0 commits reads the snapshot again from the same P0. After
  it commits, no snapshot is read again.
- A full refresh or a new checkpoint gives a new P0 and a new snapshot together, so no change
  between the two can be lost.
- With Session consistency, the snapshot queries carry the session tokens observed at P0, so no
  replica behind P0 serves them. With ConsistentPrefix or Eventual consistency,
  `initial_snapshot_delay_seconds` (default 5) is a best-effort wait.
- Merge rule: per key keep the row with the largest `struct(rank, _ts, lsn)` (see
  [Ordering](#ordering-sequence_by)); a `delete` means the document is gone. The merged result
  equals the container's state as of the last change read: a document changed after P0 has all
  those changes in the feed, and a document not changed after P0 is in the snapshot. The
  correctness argument is in [DESIGN.md](DESIGN.md#initial-snapshot).

**Limitations.**
- The snapshot is not a point-in-time read; the change feed resolves documents changed during it.
- Micro-batch 1 must start within the backup retention of P0.
- `max_items_per_trigger` does not limit the snapshot.
- Account requirements as mode 3.

With `Trigger.AvailableNow` (and triggered SDP updates), the target position is captured before P0
is resolved, so the first run usually contains only the snapshot, and changes made during it
arrive in the next run.

**Use it for** a current-state replica with hard deletes, on an account with continuous backup.

```python
orders = (
    spark.readStream.format("cosmosdb")
    .options(
        **conn,
        container="orders",
        change_feed_mode="AllVersionsAndDeletes",  # start_from defaults to Now
        initial_snapshot="true",
        max_items_per_trigger="200000",  # LSN budget for change micro-batches; the snapshot is not capped
        priority="Low",
    )
    .load()
)
```

SDP, bronze plus SCD1 with deletes ([example](../examples/databricks-bundle/src/pipelines/orders_avad/)):

```python
dp.create_streaming_table("orders_changes_avad")


@dp.append_flow(target="orders_changes_avad", name="orders_avad_stream")
def orders_avad_stream():
    return (
        spark.readStream.format("cosmosdb")
        .options(**COSMOS, container="orders", change_feed_mode="AllVersionsAndDeletes", initial_snapshot="true")
        .load()
    )


@dp.temporary_view(name="orders_changes_avad_ranked")
def orders_changes_avad_ranked():
    rank = F.when(F.col("operation") == "snapshot", F.lit(0)).otherwise(F.lit(1))
    return spark.readStream.table("orders_changes_avad").withColumn("_source_rank", rank)


dp.create_streaming_table("orders_avad")
dp.create_auto_cdc_flow(
    target="orders_avad",
    source="orders_changes_avad_ranked",
    keys=["pk", "id"],
    sequence_by=F.struct("_source_rank", "_ts", "lsn"),
    apply_as_deletes=F.expr("operation = 'delete'"),
    except_column_list=["_source_rank", "crts", "ttl_expired", "feed_range"],
    stored_as_scd_type=1,
)
```

Without SDP, a `foreachBatch` sink with a `MERGE` that applies the same ordering and delete rule
does the same job (not part of the examples).

**Target pattern.** Append-only bronze, then SCD1 with `apply_as_deletes`, keys `(pk, id)` and
`sequence_by struct(rank, _ts, lsn)`. The target can be rebuilt from bronze at any time, because
bronze holds the snapshot before any change.

**Cost.** One query read for the snapshot (about 23 RU per 1,000 documents of 0.55 KB), then
O(changes).

## 5. LatestVersion + initial snapshot

**Emits** a snapshot in micro-batch 0 (`operation = 'snapshot'`, `lsn = 0`), then `upsert` rows
from the start position.

**Why.** With `start_from=Now` or a recent timestamp the stream skips the change feed's history,
and the snapshot supplies every document that has not changed since. The initial load is then a
query read instead of a change-feed read: about 23 instead of about 58 RU per 1,000 documents of
0.55 KB. With `start_from=Beginning` the option is accepted but redundant (an INFO line is logged).

**Guarantees and limitations.** As mode 4, except that deletes are not in this feed: a document
deleted after P0 stays in the merged result if the snapshot read it.

**Use it for** what mode 2 is used for, when the initial load of a large container should cost a
query read rather than a full change-feed scan.

```python
changes = (
    spark.readStream.format("cosmosdb")
    .options(**conn, container="orders", change_feed_mode="LatestVersion", start_from="Now", initial_snapshot="true")
    .load()
)
```

**Target pattern.** As mode 4 with `sequence_by struct(rank, _ts, lsn)`, without
`apply_as_deletes`, and with `operation` in `except_column_list`.

**Cost.** One query read, then O(changes).

## Decision guide: snapshot or change feed

The LatestVersion feed is always on but has no deletes. AVAD has deletes but requires continuous
backup with the feature enabled, no partition merge, and a stream that never falls behind its
retention.

Prefer a **batch snapshot** (mode 1) when:
- deletes are needed, but the account has no continuous backup or has used partition merge;
- outages may exceed the AVAD retention;
- the container is small and runs are infrequent;
- history "as of each run" (SCD2 per snapshot) is what is wanted;
- a reconciliation baseline is wanted.

Prefer the **change feed** (mode 2, mode 4 with deletes, mode 5 for a cheaper start) when:
- the container is large or runs are frequent (a snapshot is a full read and a full diff on every
  run);
- versions between runs matter (a snapshot only sees the state at each run);
- the order of changes per document matters.

Rebuilding the target is not a reason to choose a snapshot: with append-only bronze, the target of
modes 2, 4 and 5 can be rebuilt from bronze.

| Requirement | Mode |
|---|---|
| Current state with hard deletes, account with continuous backup | 4 |
| Current state, no hard deletes (or soft deletes) | 2, or 5 for a cheaper initial load |
| Current state with deletes, no continuous backup, small or medium container | 1 |
| SCD2 history as of each run | 1 with `stored_as_scd_type=2` |
| Every version and delete (audit log) | 4 (bronze holds every change), or 3 for changes after "now" only |

### Why not a separate one-time snapshot flow with AVAD

The obvious alternative to `initial_snapshot` is two flows into one bronze table: the AVAD stream
from `Now`, and a one-time batch snapshot flow (`@dp.append_flow(once=True)`). SDP gives no
ordering between two flows of one update, which breaks it in three ways:

1. On the first run, the snapshot flow can read a page before the stream has fixed P0. A document
   changed after that page was read but before P0 is in neither source.
2. A full refresh resets both flows, and they race again.
3. If the stream fails before its first checkpoint, the snapshot flow has already committed and
   does not run again. The retry resolves `Now` later than the snapshot, and the changes in between
   are lost without an error.

`initial_snapshot=true` makes the snapshot the stream's own micro-batch 0, read after P0 is in the
offset log; a replay reuses P0 and a reset renews both. A one-time snapshot flow is still correct
for a LatestVersion stream that starts from a fixed timestamp earlier than the snapshot: the feed's
start does not move on retry or refresh, so every change after the snapshot read is in the feed.

## Ordering (`sequence_by`)

Keys are always `(pk, id)`. The ordering used in every example:

```
sequence_by = struct(CASE WHEN operation = 'snapshot' THEN 0 ELSE 1 END, _ts, lsn)
```

- The rank comes first, so every change row outranks every snapshot row of the same key, whatever
  the timestamps (equal `_ts` within one second, clocks across regions).
- Then `_ts`, the server time in seconds. For AVAD deletes the data source sets `_ts` to the
  delete's `crts`, so it is never null.
- Then `lsn`, which breaks ties within a second. LSNs increase per document, also across partition
  splits (tested against the service fake). Ordering by `_ts` first is also meant to protect
  against LSN discontinuities after partition merges (not verified).
- Without snapshot rows (modes 2 and 3): `struct(_ts, lsn)`.

`struct(rank, lsn)` would be finer than the 1-second `_ts`, but it assumes a single write region:
with multi-region writes, conflict resolution follows `crts`, and LSNs from different regions are
not a global order.

Deletes: `apply_as_deletes = "operation = 'delete'"` (modes 3 and 4). AUTO CDC keeps delete
tombstones for `pipelines.cdc.tombstoneGCThresholdInSeconds` (2 days by default); an older row for
a deleted key is ignored as long as it is processed within that window, which holds within one
stream.

## Full-refresh rules

| Situation | Rule |
|---|---|
| A target streams from bronze (modes 2, 4, 5) | Never refresh bronze alone. After a full refresh of only the bronze table, the target's flow fails at the next update with `DELTA_SOURCE_IGNORE_DELETE` (observed on DBR 18.3). Refresh the whole pipeline instead. |
| Rebuild only the target (modes 2, 4, 5) | Safe: the target is a function of the append-only bronze. Use it after changing target columns or `sequence_by`. |
| Reset only the stream checkpoint (modes 4, 5) | Never. Bronze would mix two stream incarnations: the new snapshot (rank 0) loses against older change rows (rank 1), and the changes in the gap never arrive. Refresh the whole pipeline: a new P0 and a new snapshot, with bronze and the target rebuilt together. |
| AVAD stream stopped longer than the retention | Full refresh of the whole pipeline, then reconcile. |
| Mode 1 (snapshot to SCD2, no bronze) | A refresh restarts the SCD2 history from the current state; older versions are lost. |

## Costs

Measured on 400 RU/s containers; RU scales roughly with document size.

| Read | RU per 1,000 documents of 0.55 KB | Notes |
|---|---|---|
| Batch `SELECT *` (mode 1; the snapshot of modes 4 and 5) | about 23 | about 39 for documents of 1.4 KB |
| LatestVersion initial load from `Beginning` (mode 2) | about 58 | |
| AVAD, per change (modes 3 and 4) | not measured | |
| Position probes of a stream | about 1 RU per feed range and micro-batch | |
| Projection `SELECT c.id` | about 14 | 68 RU per 5,000 documents |

The duration of a read is at least documents × RU per document ÷ provisioned RU/s. Throughput is
split evenly across physical partitions, and each partition serves at most 10,000 RU/s.
