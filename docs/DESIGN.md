# cosmosdb-spark-datasource: design notes

This document records what was verified in the library sources and against the service, and why
the data source is built the way it is. Line numbers refer to **azure-cosmos 4.17.1** and to the
Apache Spark tags **v4.0.1, v4.1.0, v4.2.0** (`python/pyspark/...` and `sql/core/.../python/...`).
"Verified (service)" means it was observed against a real account (single write region, default
consistency Session, continuous backup).

## 1. Research findings: azure-cosmos

| Topic | Finding | Source |
|---|---|---|
| Feed ranges | `ContainerProxy.read_feed_ranges()` returns one **dict per physical partition**: `{"Range": {"min", "max", "isMinInclusive": True, "isMaxInclusive": False}}`. These are plain JSON-serializable values. The docstring says "base64 encoded string", which is inaccurate. The SDK warns the dict is opaque, so the package builds it only for the documented `feed_range=` argument. | `container.py:2056-2096` (`FeedRangeInternalEpk(...).to_dict()` at 2089) |
| EPK ordering | EPKs are upper-case hex strings compared lexicographically. Ranges are half-open, with sentinels `""` and `"FF"`. | `_routing/routing_range.py:151` (`Range.contains`), `:161` |
| Hash versions | V2 and MultiHash clear the top 2 bits of the first hash byte (`&= 0x3F`), so real EPKs are `< "40"`. V1 encodes `[float(murmur3_32), components...]`; verified EPKs look like `05C1xx...`, unevenly spread. A missing `version` means **V1**. | `partition_key.py:363, 386, 256, 570` |
| Query by feed range | `query_items(feed_range=...)` normalizes the EPK range (`_cosmos_client_connection.py:3389`). When the range is a strict sub-range of a physical partition it adds `x-ms-start-epk`/`x-ms-end-epk` plus `x-ms-read-key-type: EffectivePartitionKeyRange`. Verified (service): two EPK halves of one partition returned disjoint documents whose union is the whole container. | `_routing/feed_range_continuation.py:856-899` |
| Change feed API | `query_items_change_feed(feed_range=, start_time="Beginning"\|"Now"\|datetime, mode="LatestVersion"\|"AllVersionsAndDeletes", continuation=, max_item_count=, priority=, response_hook=)`. A `continuation` overrides every other scope or start parameter. | `container.py:654-745` (`:723`, `:725`), `_change_feed/change_feed_iterable.py:78-81, 148` |
| Continuation format | `base64(json(ChangeFeedStateV2.to_dict()))`: `{"v":"v2","containerRid","mode","startFrom":{"Type":...},"continuation":{"v":"v2","rid","continuation":[{"token":"\"<lsn>\"","range":{...}}],"Range":{...}}}`. Each child `token` is the server etag of one physical (sub)range. Verified (service): the etag is the quoted LSN, e.g. `"1686"`. | `_change_feed/change_feed_state.py:225`, `feed_range_composite_continuation_token.py:58` |
| Obtaining the continuation | `ChangeFeedIterable._unpack` reads `client.last_response_headers['etag']` (`change_feed_iterable.py:94`). `ChangeFeedFetcherV2` overwrites that header **in place** with the base64 composite state after every request, including 304s (`change_feed_fetcher.py:172-189`). So the token is available via `by_page().continuation_token` after a page, and via `container.client_connection.last_response_headers['etag']` after the pager stops on 304. `last_response_headers` is per-client and **not thread-safe** (`_cosmos_client_connection.py:3332`). `response_hook` receives the **raw** per-request headers (`:3339`). Verified (service). | as listed |
| Current position without reading data | `start_time="Now"` sends `If-None-Match: *` (`change_feed_start_from.py:161`). The service answers 304 with `etag` = current LSN. Verified (service): about 1 RU per probe. | |
| Split / merge | On 410/1002 the V2 fetcher calls `handle_feed_range_gone` (`change_feed_fetcher.py:146`). For a **split**, the composite drops the parent and appends one child token per new partition, **each carrying the parent's etag**. For a **merge**, the token is kept as-is and the server filters by EPK. After a split the SDK round-robins the children (`_move_to_next_token`). | `feed_range_composite_continuation_token.py:101-122, 161` |
| Split of a sub-range token | `handle_feed_range_gone` builds each child token from the **full** child partition range (`Range.PartitionKeyRangeToRange(child_range)`); it does not intersect it with the token's own range. A change-feed read scoped to *part* of a partition is therefore widened to the whole child partitions after a split. The query path does intersect (`_derive_initial_feedranges`, `_routing/feed_range_continuation.py:395-424`), so batch reads are not affected. Reproduced with the real SDK classes. | `feed_range_composite_continuation_token.py:113-122` |
| Sub-partition change feed | If the token's range is smaller than the physical partition, EPK filter headers are added. Verified (service). | `change_feed_state.py:251-269` |
| Item metadata | LatestVersion items carry `_lsn` and `_ts`. AVAD items are `{"current": {...}, "metadata": {"lsn","crts","operationType", "previousImageLSN"?}}`. A **delete** has `"current": {}` plus `metadata.id` and `metadata.partitionKey` (e.g. `{"pk": "x"}`; `{"pk": null}` for a JSON-null key, `null` for an undefined one; a number above 2^53 as the double the service hashes, e.g. `9007199254740992` for a document holding `9007199254740993`). All verified (service). `metadata.timeToLiveExpired` is documented for TTL deletes but was **not observed** in 120 s (TTL purge is asynchronous). | |
| LSN grouping | Verified (service): bulk / transactional-batch writes commit many documents under **one LSN** (49 documents at one `_lsn`), and pages contain whole LSN groups regardless of `max_item_count`. | |
| SDK bug | Resuming a continuation whose `startFrom` is `PointInTime` fails: `ChangeFeedStartFromPointInTime.from_json` passes milliseconds to `datetime.fromtimestamp` (`ValueError: year 57971 is out of range`). Tokens built by this package never use `PointInTime`. | `change_feed_start_from.py:198` |
| Retries | Throttling: `ResourceThrottleRetryPolicy`, default 9 attempts / 30 s cumulative wait (`_retry_options.py:44-46`). It honors `x-ms-retry-after-ms` (`_resource_throttle_retry_policy.py:52`) and is configurable via `retry_throttle_total` / `retry_throttle_backoff_max` (`cosmos_client.py:126-134`). 503, 408 and 5xx have their own policies (`_retry_utility.py:198-250`). Timeouts are set with `connection_timeout` and `read_timeout`. | |
| AAD | Any object passing `isinstance(x, TokenCredential)` (a runtime-checkable protocol) is accepted (`cosmos_client.py:75`). Scope is `https://<host>/.default` (`_base.py:912`, `_cosmos_client_connection.py:252`), with fallback to `https://cosmos.azure.com/.default` on `AADSTS500011` (`_auth_policy.py:57-62`). azure-core asks for a new token once fewer than about 300 s remain. | |
| Version differences | `CosmosClient.close()` does not exist in 4.14.0; the client cache falls back to `__exit__`. Everything else used here behaves the same in 4.14.0 and 4.17.1: the integration tests (batch, V1 sub-split, LatestVersion and AVAD streams) passed against an account on both, and CI runs the unit tests on both. | |
| Container size | `container.read(populate_quota_info=True)` returns `x-ms-resource-usage: ...;documentsSize=<KB>;documentsCount=<n>;collectionSize=<KB>`. Verified (service). | |
| Session tokens | `query_items(feed_range=..., session_token=...)` sends the token as `x-ms-session-token` when the request's consistency is Session (`_base.set_session_token_header`, `:489-517`); for other levels the SDK does not send it. The client's own session container only knows tokens seen by that process (`_session.py:54`); for a pk range without a token it merges the tokens of the range's split parents (`_resolve_partition_local_session_token`, `:384`). 404/1002 (`READ_SESSION_NOTAVAILABLE`) is retried once by `_SessionRetryPolicy` in a single-write-region account, then raised (`_session_retry_policy.py`, `_retry_utility.py:199-204`). Verified (service, Session default): a `start_time="Now"` change-feed probe returns `x-ms-session-token: 0:-1#6` together with etag `"6"` (the token's global LSN equals the etag LSN, in both modes); a feed-range query sends an explicit token; a token 10^6 LSNs ahead is refused with 404/1002. | as listed |

Measured costs (service, 400 RU/s containers; RU scales roughly with document size):
- `SELECT *` page of 1,000 documents: about 23 RU for documents of 543 bytes of user properties (748 bytes of JSON with system properties), about 39 RU for documents of 1,353 / 1,558 bytes.
- `SELECT c.id` page of 5,000 of the smaller documents: about 68 RU.
- Change feed: about 57.6 RU per 1,000 changes, measured on a `LatestVersion` initial load (1M of the smaller documents, change feed from `Beginning`). The per-change cost of `AllVersionsAndDeletes` was not measured.

## 2. Research findings: PySpark Python data source API

| Topic | Finding | Source |
|---|---|---|
| Construction | `DataSource(options=CaseInsensitiveDict)`: keys are lower-cased (`worker/create_data_source.py:129, 137`). `schema()` is called **only when no user schema** is supplied (`:144-145`) and may return a DDL string or a `StructType`. **No batch/stream signal** is passed. The instance is pickled and re-created in other workers. | v4.0.1 |
| Batch planning | The read function capturing the reader is pickled **before** `partitions()` runs (`worker/plan_data_source_read.py:322` vs `:327`). Anything computed in `partitions()` must therefore travel inside `InputPartition`s. `partitions()` must return a `list`. An empty list becomes `[None]`, so `read(None)` is called (`:332, 346-348`). | v4.0.1 (same in 4.1/4.2) |
| Row conversion | `records_to_arrow_batches` accepts tuples, lists, `Row` and `pyarrow.RecordBatch`, and raises on Python values that do not match the Spark type (`:62`). The tests run produced rows through this same function. | |
| `InputPartition.__repr__` | Prints every attribute (`datasource.py:270`), so tokens are wrapped in `SecretToken` with a redacted `repr`. | v4.0.1 |
| Stream reader 4.0/4.1 | `latestOffset()` is called **without arguments** (`streaming/python_streaming_source_runner.py:65-66`). The JVM class implements `AcceptsLatestSeenOffset`: on restart it calls `partitions(offset, offset)` with the last logged offset (`PythonMicroBatchStream.scala:86-89`). `planInputPartitions` may run several times per micro-batch (`:47`). The streaming runner is a long-lived Python process; read tasks use a **different, fresh** reader instance. | v4.0.1 = v4.1.0 |
| Call order 4.0/4.1 | **New query:** `latestOffset()` is called **before** `initialOffset()`: `constructNextBatch` calls `s.latestOffset()` for a plain `MicroBatchStream`, and `initialOffset` is only used when the batch plan is built (`MicroBatchExecution.scala` v4.0.1 `:736`, `getStartOffset` `:573-583`). The reader therefore resolves and **caches** the initial offset inside `latestOffset()` when it has no start, so Spark's later `initialOffset()` returns the identical offset. **Restart:** `initializeExecution` calls `setLatestSeenOffsetOnSources(offsetLog.getLatest())` before `populateStartOffsets` (`:317-319`), so the start is always known. **`Trigger.AvailableNow`** on a source without `SupportsTriggerAvailableNow` falls back to a **single batch** unless `spark.sql.streaming.triggerAvailableNowWrapper.enabled` is set (`:98-128`, SPARK-45178). | v4.0.1 |
| Admission control | Only in **4.2**: `latestOffset(start, limit)`, `getDefaultReadLimit()`, `ReadAllAvailable` / `ReadMaxRows` / ..., and `SupportsTriggerAvailableNow` (`datasource.py:715, 769`; `pyspark/sql/streaming/datasource.py`). The runner turns it on when `inspect.signature(reader.latestOffset)` has parameters (`python_streaming_source_runner.py:142-163`), and the JVM then uses `PythonMicroBatchStreamWithAdmissionControl` / `...WithTriggerAvailableNow` (`PythonMicroBatchStream.scala:118, 151`), which have no `setLatestSeenOffset`. | v4.2.0 |
| Filter pushdown | `pushFilters` exists from 4.1 (`datasource.py:519`), behind `spark.sql.python.filterPushdown.enabled` (**default false**, `SQLConf.scala:5452`). If a reader overrides `pushFilters` while the conf is off, planning **fails** with `DATA_SOURCE_PUSHDOWN_DISABLED` (`plan_data_source_read.py:383`, 4.1 and 4.2). Serverless restricts Spark confs, so `pushFilters` is **deliberately not implemented**. Server-side filtering is done with the `query` option. | v4.1.0 |
| `SimpleDataSourceStreamReader` | Reads data inside the driver-side runner (no parallelism), so it is unsuitable for large containers and not used. | |
| **Databricks serverless** (environment version 4) | Data source workers run the engine PySpark `_engine_pyspark` (`__version__` 4.4.0.dev0, `/databricks/spark/python/_engine_pyspark.zip`), while `import pyspark` in the same process gives the environment's client PySpark 4.0.0 (databricks-connect). `pyspark is _engine_pyspark` is False. The engine's batch planner converts the schema to client classes (`_client_parse_datatype_json_string`, a Databricks addition), but `streaming/python_streaming_source_runner.py` passes `_engine_pyspark.sql.types.StructType` to `streamReader()`. The runner enables admission control by the `latestOffset` signature. It serializes `getDefaultReadLimit()` as `dataclasses.asdict(limit) \| {"_type": class name}`, rebuilds limits from a name registry of *its own* classes, calls `reportLatestOffset()` (not in the client 4.0 base class), and checks `isinstance(reader, _engine_pyspark...SupportsTriggerAvailableNow)`. Handled in `_compat.py` (§5, "Databricks engine compatibility"). | engine sources read from a serverless worker |
| **Lakeflow SDP serverless** (DBR 18.3) | Pipeline Python is a classic PySpark session behind a Py4J allow-list (`spark.catalog.tableExists` is rejected with `Py4JSecurityException`). Triggered updates run capped streams as many AvailableNow batches. | pipeline event logs |

## 3. Architecture

```
cosmosdb_datasource/
  datasource.py   CosmosDBDataSource: name "cosmosdb", schema selection, reader/streamReader
  options.py      parse & validate options (pure, no I/O); secrets excluded from repr
  auth.py         token minting on the driver, PartitionCredential (executor), SecretToken
  client.py       process-wide CosmosClient cache keyed by endpoint + principal + settings
  backend.py      the ONLY module calling the SDK data plane (SdkBackend)
  epk.py          EPK sub-splitting, covers, overlay
  batch.py        CosmosBatchReader / CosmosQueryPartition; feed-range planning and the resumable
                  query loop, shared with the initial snapshot
  stream.py       CosmosStreamReader / CosmosChangeFeedPartition / CosmosSnapshotPartition, budget allocation
  snapshot_stream.py  CosmosSnapshotStreamReader / CosmosSnapshotStreamPartition (snapshot_stream=true)
  changefeed.py   continuation-token codec, LSN-bounded read loop
  offsets.py      Offset model (incl. the initial-snapshot record), re-keying after splits; SnapshotStreamOffset
  documents.py    partition-key paths, change-feed item normalization
  schema.py       default schemas, safe conversion, PERMISSIVE/DROPMALFORMED/FAILFAST
  retry.py        transient classification, retry-after, backoff with jitter
```

Flat inheritance: `CosmosReaderBase` plus the PySpark interface (`DataSourceReader` /
`DataSourceStreamReader`, and `SupportsTriggerAvailableNow` on PySpark >= 4.2). Reader objects hold
only immutable configuration (`CosmosOptions`). The SDK is imported lazily inside executor code
paths (`client.py`, `backend.py`).

## 4. Batch reads

1. `partitions()` (driver):
   1. Mint a token for AAD modes.
   2. Read container properties (partition key paths, kind, version).
   3. Call `read_feed_ranges(force_refresh=True)`.
   4. Sub-split every feed range into `partitions_per_feed_range` EPK sub-ranges; `auto` is 1. An EPK-filtered query (`x-ms-read-key-type: EffectivePartitionKeyRange`) costs about the RU of a scan of the whole physical partition, whatever the sub-range: measured on 10M documents in 17 partitions, 2, 8 and 16 sub-ranges cost 2.0, 6.3 and 12 times the RU of 1, and each extra sub-range about 10,500 RU, the cost of a full scan of one partition (about 590k documents). The Java connector's `Aggressive` partitioning shows the same (3.4 times for 4 sub-ranges). The scan was not faster (N = 2: 28.7 s against 27.8 s), so size-based sub-splitting was dropped.
   5. Return one `CosmosQueryPartition` per sub-range, carrying the token and partition key definition.
2. `read()` (executor): `batch.query_documents` runs `query_items(query, parameters, feed_range=<scope>, max_item_count=page_size)` page by page, with every scope inside one physical partition; rows are yielded as documents arrive, so a partition is never materialized. After the SDK's own retries are exhausted, transient errors resume from the last fully consumed page's continuation. Splits and duplicates: next section.
3. Queries run independently per sub-range. `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP` and aggregates are therefore per partition (a warning is logged).
4. Batch reads are **not snapshot-isolated**: documents changed during the scan may be read before or after the change. Use the change feed for consistent incremental ingestion.

### Splits and duplicates during a query

Requirement: at most one row per `(pk, id)` per read (snapshot CDC downstream needs unique keys),
with memory that does not grow with the partition, which can hold tens of GB.

What the service and the SDK do (verified against the service unless noted):
- A `SELECT *` scoped to one physical partition returns documents in `_rid` order, and the backend
  continuation is `-RID:~<last _rid>#...`: the next page resumes after the last `_rid` returned. A
  replace keeps a document's `_rid` (and place); a delete followed by a re-create of the same key
  makes a new document with a new, higher `_rid`, which the scan reaches again at its end. Every
  "modified document returned twice" in the live runs of 0.2.0 was such a re-creation.
- After a split, the children hold the parent's documents with their `_rid`s, so a parent's
  continuation resumes correctly on a child: sub-ranges that fell inside one child after a split
  were read without loss or duplicates.
- azure-cosmos 4.17.1, scope inside one partition, split mid-read: the SDK retries the 410/1002
  itself, and if the scope now spans several children it cannot carry the single-partition
  (legacy) continuation over, logs "restarting this feed_range query from the beginning" and reads
  the range again (`__QueryFeed`, `_should_bridge_legacy_continuation`). One read returned 17,998
  duplicates this way. If the scope falls inside one child, the SDK continues correctly.
- azure-cosmos 4.14.0 raises the 410/1002 (`PartitionKeyRangeGoneRetryPolicy` refreshes the routing
  map and gives up), and for a scope spanning several partitions sends the same continuation to each
  of them in one call and keeps only the last one's (`__QueryFeed`, read from the source).

Options considered:
1. *Resume children from the parent's continuation.* Needs the split to be detected before a
   restarted page is passed on, and every SDK query to stay within one partition (also because of
   4.14.0). Zero memory, no extra RU. Does not address re-created documents.
2. *A deterministic ordering plus a resume key*, e.g. `ORDER BY c.id` with `c.id > @last` after a
   split. The SDK runs `ORDER BY` through its query-plan pipeline, whose targets come from the
   plan's ranges rather than from `feed_range`; ordering by `_rid` is rejected by the service
   ("Primary key order-by is not supported"); an ordered index scan per EPK sub-range costs more RU;
   and a user `query` cannot be rewritten safely. Rejected.
3. *Per-task de-duplication.* Exact de-duplication needs a record of every emitted key, because a
   document deleted and re-created during the scan can have any key. A set in memory grows with the
   partition.

Choice: 1 for splits, and 3 restricted to the documents that can be duplicates.
- Each task keeps a queue of scopes, each inside one physical partition, with the continuation of
  its last consumed page. After every page it checks the partition key range id(s) that served it
  (`x-ms-documentdb-partitionkeyrangeid`, via the response hook) and the cached routing map. If the
  page came from more than one range, from a different range than the scope's earlier pages, or the
  scope no longer lies in one partition, the page is dropped (the SDK may have restarted), the map is
  refreshed, and the scope is replaced by its children, each resumed from the scope's last
  continuation. A 410 that reaches the package (4.14.0, or exhausted SDK retries) also refreshes the
  map. Ten re-plans without a consumed page fail the task with a clear error.
- Re-created documents: a document whose `_ts` is at or after the server time of the task's first
  response (the `Date` header) minus 120 s may have been written during the read, and is dropped if
  its key was emitted already. Documents written before the read can only be returned once.
  `dedup.SeenKeys` holds 64-bit fingerprints of the emitted keys (`blake2b` of the partition key,
  with a missing value distinct from null, and the id) in sorted runs, merged like a binary counter;
  runs of 512 Ki entries move to an anonymous temporary file. Memory stays below about 32 MiB; the
  file grows by 8 bytes per emitted document; a membership test (only for documents written during
  the read) is a binary search per run. A fingerprint collision can only drop a document written
  during the read, with probability about n / 2^64 for n emitted documents.

Tested (`tests/test_query_splits.py`): the fake serves queries in `_rid` order and reproduces both
SDK behaviours (a test checks that it returns rows twice across a split without the package's
handling). Splits before the first page and mid-read, with 1, 3 and 4 sub-ranges per partition, two
splits in one read, the initial snapshot and snapshot streams, deletes and re-creates of documents
already read and not yet read, replaces, and a randomized mix of splits and writes; with the
previous query loop 23 of these tests fail.

### EPK sub-splitting correctness

For any total order and any boundaries `lo < p1 < ... < pk < hi`, the half-open intervals
`[lo,p1), [p1,p2), ..., [pk,hi)` are pairwise disjoint and their union is `[lo, hi)`. The service
and `Range.contains` use plain lexicographic comparison of upper-case hex strings, so correctness
only requires strictly increasing boundaries strictly inside `(lo, hi)`. `epk.split_range`
guarantees that by construction (`_filter_increasing`) whatever method chose the candidates.
Generated boundaries are whole bytes (even length).

Balance (not needed for correctness):
- **V2 / MultiHash**: numeric interpolation over a fixed hex width (>= 32 digits). The upper bound `"FF"` is clamped to `"40"` because of the `0x3F` mask. If a range is narrower than the resolution, the width grows; an interval that contains a single point (e.g. `["1A", "1A00")`) stays unsplit.
- **V1**: interpolation over hex is badly skewed (EPKs cluster in `05C1...`). Boundaries come from a grid of 4,095 EPK prefixes of evenly spaced 32-bit hash values (`encode([float(h)])`, using the SDK's own V1 encoder), quantile-picked inside the range.

Tests (`tests/test_epk.py`):
- 1,000 random ranges with variable-length and odd-length bounds, the `""`/`"FF"` sentinels and up to 64 splits.
- Every probe string, including boundaries, their prefixes and extensions, and real EPKs, lies in exactly one sub-range per `Range.contains`.
- Balance within ±15% for V1, V2 and MultiHash.
- Integration: a 4-way V1 split of a 2,000-document container must be balanced within ±50% and complete.

## 5. Streaming (change feed)

### Offsets

```json
{"v": 1, "rid": "<container _rid>", "mode": "LatestVersion",
 "ranges": {"-05C1CFFFFFFFF8": 5430, "05C1CFFFFFFFF8-05C1DFFFFFFFF8": 5367, "...": 0}}
```

- Keys are `"<min>-<max>"` EPK bounds (stable, derived from the range). The ranges always tile `["", "FF")`.
- **Batch 0**: Spark's offset log stores only *end* offsets, and Spark re-calls `initialOffset()` in a new process when it replays an uncommitted batch 0. With `Now` or a timestamp that would resolve to a later position. Initial offsets therefore carry `"initial": true`, and the end offset of the batch starting there carries the original start as `"init": {...}`. `partitions()` uses `init` instead of the re-resolved start, so a batch-0 replay is identical (tested for both call orders).
- **Initial snapshot** (`initial_snapshot=true`): the initial offset carries `"snapshot": {"session": {"<range>": "<session token>"}}`, and the end offset of batch 0 copies it next to `init`. Every later offset omits it. See "Initial snapshot" below.
- Each value means "every change in this range with LSN <= value has been handed to Spark".
- `rid` and `mode` detect a re-created container or a mode switch on an existing checkpoint, and fail clearly.
- Keys and ranges are emitted in a fixed order, so an unchanged position yields **byte-identical JSON**. Spark compares offset JSON to decide "no new data".

### Positions and the bounded read

- `initialOffset()` resolves the start per feed range:
  - `Now`: the etag of a `start_time="Now"` 304.
  - `Beginning`, or an ISO timestamp (LatestVersion only): first item's `_lsn - 1` from a 1-item probe, or the 304 etag if the range is empty.
  
  Every offset value is therefore a concrete integer.
- `latestOffset()`:
  1. Take the known start: the argument on 4.2, the last planned or initial offset on 4.0/4.1.
  2. **Re-key** it to the current physical partitions (a split copies the parent LSN to the children, like the SDK).
  3. Probe the current LSN of each range (`Now` 304, about 1 RU each, in parallel).
  4. Clamp each value to >= start.
  5. Apply the per-trigger budget and, if set, the AvailableNow target.
- `partitions(start, end)`:
  - The overlay (common refinement) of the start and end covers gives pieces `(lo, hi, s, e)`.
  - Every piece with `e > s` becomes an input partition, optionally EPK sub-split.
  - `start == end` returns `[]` without I/O. This also handles `setLatestSeenOffset`.
- `read(piece)`:
  - Build a single-range continuation `(lo, hi, LSN = s)`. The SDK parses it (unit-tested through `ChangeFeedState.from_json`), and the service accepted arbitrary LSNs (verified).
  - Iterate the change feed page by page and **emit items with `s < lsn <= e`**. LSN is `_lsn` in LatestVersion and `metadata.lsn` in AVAD.
  - The read finishes when **every child position** in the SDK's composite token is >= `e`.

### Determinism argument (replayable micro-batches)

Spark logs `(start, end)` before running a batch and replays exactly that pair after a failure. Every
micro-batch is therefore deterministic and replayable. With a checkpointed, idempotent sink (Delta,
SDP streaming tables) that makes the result effectively exactly-once for AllVersionsAndDeletes;
LatestVersion delivers the latest state per key, not every version (point 5). Points 1-6 assume no
partition merge, which is unverified (see "Partition splits").
`read()` depends only on `(range, s, e, mode)` from the partition and on the service's change feed:

1. Within a physical partition the change feed is LSN-ordered and resuming after LSN `x` yields exactly the changes with LSN > `x`. Every change with LSN <= `e` was committed when `e` was observed, because `e` came from a 304 etag. So the set `{changes in range: s < lsn <= e}` is fixed once `e` is logged.
2. The loop stops only when all child positions are >= `e`. Items beyond `e` are skipped, never emitted, and belong to the next batch, which starts at `e`. Adjacent batches therefore neither overlap nor leave gaps, per range.
3. Transaction groups share one LSN and are never split: the bound is inclusive and pages hold whole groups.
4. **AllVersionsAndDeletes**: every version and delete is retained (within the account's continuous-backup retention). A replay returns identical rows in identical order. Tested with the fake and against the service.
5. **LatestVersion caveat**: the service keeps only the newest version of a document. If document X changed at LSN 10 (<= e) and again at LSN 30 (> e) before a replay, the replay omits X and the next batch delivers X@30. The *final* state is correct and no change newer than X@30 is lost; intermediate versions are, by definition, not part of LatestVersion. Tested (`test_latest_version_replay_semantics`). Use AVAD when every version matters.
6. Spark may call `partitions()` several times for the same pair. Results are identical because planning depends only on `(start, end)` and the current topology, and topology changes only affect how pieces are scheduled, not their contents.

### Initial snapshot

Goal: full current state plus every later change from one stream, although AllVersionsAndDeletes can
only start at "Now" (P0). Running the snapshot as a separate flow races: after a reset of the stream's
checkpoint the snapshot can be read before the new stream's P0 is durable, and a stream that fails
before logging batch 0 re-resolves "Now" later than the snapshot. Both lose the changes in between.
The snapshot is therefore part of the stream: it is the content of the micro-batch whose start is P0.

**Offsets.**

```json
{"v": 1, ..., "ranges": {"-FF": 1234}, "initial": true, "snapshot": {"session": {"-FF": "0:-1#1234"}}}
{"v": 1, ..., "ranges": {"-FF": 1234}, "init": {"-FF": 1234}, "snapshot": {"session": {"-FF": "0:-1#1234"}}}
```

The first is the initial offset (never logged by Spark), the second the end offset E0 of batch 0
(logged). Rule: snapshot partitions are planned iff the start is an initial offset (`"initial": true`)
and the end carries `snapshot`. Only batch 0 can match: no other batch starts at an initial offset, and
no other end offset carries the field. Because the decision and the session tokens come from the
logged E0, batch 0's content is a function of its logged end offset alone: a replay reads the snapshot
again with the original tokens even if the option was changed in between, and enabling the option for
a checkpoint whose E0 was logged without it has no effect (WARNING). This deviates slightly from the
simpler "start pending, end not" encoding, whose decision would depend on the option value in the
replaying process. Offsets without the field mean "no snapshot".
Session tokens are LSN vectors, not credentials.

**Batch 0 is snapshot only.** `latestOffset()` returns `E0.ranges = P0.ranges`, so batch 0 has no
change-feed pieces and change-feed rows start with batch 1 at P0. This is simpler than mixing both
kinds of rows in batch 0: no per-trigger budget or AvailableNow target applies to batch 0, the
`max_items_per_trigger` cap trivially applies to the change feed only, and both kinds of batches keep
their own invariants. Correctness does not depend on the choice: the change-feed start is fixed (P0)
before the snapshot is read, which is all the merge argument needs. Batch 0 always runs: Spark runs the
first batch whenever no offset is committed, and E0's JSON differs from the initial offset anyway.
With AvailableNow, the target captured by `prepareForTriggerAvailableNow()` only limits change-feed
ends. Spark captures it before `initialOffset()` resolves P0, so it is below P0, and batch 1 would have
nothing to read: the changes made while the snapshot was read would wait for the next run, and a first
or full-refresh update would never leave the target exact. The first `latestOffset` whose start is
batch 0's end offset therefore replaces the target once with the positions it has just probed
(`_reported`, no extra request). The run then drains every change up to the moment after batch 0, in
capped batches, and terminates at that fixed position. The merge argument does not change (the target
only bounds a run), and termination holds because the target moves at most once per run
(`_target_refreshed`, reset by `prepareForTriggerAvailableNow`). On PySpark 4.0/4.1 a run is a single
batch, so the first run ends after the snapshot. Tested: changes before, during and after batch 0
within one run; the move happens once even when Spark asks twice for batch 1; no move without a
snapshot; the engine-protocol simulation.

**Planning and reading.** `partitions(P0, E0)` plans one or more `CosmosSnapshotPartition`s per
*current* feed range with the batch reader's helper (`batch.split_feed_ranges`, `auto` included), so a
split between P0 and planning only adds ranges. `read()` runs `SELECT * FROM c` with the batch reader's
resumable query loop (`batch.query_documents`), which handles splits during the query as for batch
reads (§4). Rows: `operation='snapshot'`, `lsn=0`, `crts=NULL`, `ttl_expired=false`, `feed_range` =
the planned feed range.

**Ordering guarantees.** P0 is resolved in `initialOffset()` (all probes complete) before
`latestOffset()` builds E0; Spark writes E0 to the offset log before it runs batch 0's tasks, and only
those tasks read the snapshot. So every snapshot page is read after P0 was resolved and made durable
(tested by recording the fake service's call order and the harness's offset log at the first page).

**Merge argument.** Consumers keep, per `(pk, id)`, the row with the largest
`struct(operation <> 'snapshot', _ts, lsn)`; a delete means absence. Let F be the change feed after P0
and S the snapshot rows. For a key k:
1. k changed after P0: all of its changes after P0 are in F (determinism argument), each outranks every
   row of S, and among them `(_ts, lsn)` picks the last one, which is k's current state or its delete.
2. k not changed after P0: no row of F. If k existed at P0 its current version is its P0 version. S
   contains it, provided (a) every snapshot query is served by a replica that has applied all writes up
   to P0 (next paragraph) and (b) a query run to completion returns every document not modified during
   the query. (b) is the service's `_rid` continuation: a document that is not deleted keeps its
   position, also across a split (§4). If k did not exist at P0 and was not created later, it is in neither source.
So the merged result equals the container state as of the last change read. Rows of S for documents
modified during the snapshot may hold either version; case 1 covers them. With LatestVersion, case 1
lacks deletes, so documents deleted after P0 may remain. Tested: a property test with random inserts,
updates (several per document), deletes and delete-then-recreate interleaved with the snapshot's page
reads, splits in some seeds, `_ts` ties; the same test detects each of three sabotaged merges (snapshot
outranks changes, snapshot dropped, changes dropped). Against an account: concurrent writes during the snapshot,
merged state equal to the container (ids and `_ts`), with at least one snapshot row overtaken by a
change and one snapshot key deleted afterwards.

**Replica lag (condition 2a).** With Session, ConsistentPrefix or Eventual consistency a read may be
served by a replica behind the one that answered the P0 probe; a write committed before P0 but not yet
applied there would be in neither source. Mitigations:
- *Session tokens* (preferred, verified). Each start probe's response carries a session token whose
  global LSN equals the probe's LSN (§1). The tokens are recorded per P0 range in the snapshot record,
  and each snapshot partition sends the compound token of the P0 ranges overlapping its feed range. The
  service then serves the query only from a replica at or beyond that LSN, otherwise it answers
  404/1002, which the SDK retries once and the package's retry layer retries with backoff
  (`retry.is_transient`). Verified (service): a token ahead of the service is refused with 404/1002,
  and in the integration test against an account every snapshot query of a fresh client carried the recorded token. Tested with
  the fake (a lagging query replica loses pre-P0 writes without the token and serves them with it).
- *Delay* (best effort). `partitions()` of batch 0 blocks until `initial_snapshot_delay_seconds`
  (default 5) after this process resolved the initial offset (monotonic clock). On a replay the
  original resolution happened earlier than this process's own re-resolution, so the wait is never too
  short; it is bounded by the delay, and no wall-clock time is stored in the offsets.
- *Consistency levels.* Strong and Bounded Staleness use quorum reads and are not affected. With
  ConsistentPrefix or Eventual the SDK does not send session tokens and the service would not honour
  them, so only the delay applies: lower risk, no guarantee.
- *Splits.* After a split between P0 and planning, the parent's token is sent for the child ranges;
  the SDK resolves parent tokens the same way client-side. Whether the gateway applies a parent's token
  to a child partition is **not verified** (no live split was triggered).

### Snapshot streams

Goal: one complete snapshot per micro-batch, so an append flow keeps a history of container states.
SDP has no other way to append a full snapshot per update: an append flow needs a streaming source,
`once` runs once, and a materialized view replaces its contents.

**Offsets** (`SnapshotStreamOffset`): `{"v": 1, "kind": "snapshot", "seq": n, "snapshot_ts": "...Z",
"rid": "..."}`; the initial offset is `{"v": 1, "kind": "snapshot", "seq": 0}` (no I/O, identical on
every call). The batch `(seq n-1, seq n]` reads snapshot n. `snapshot_ts` has a fixed format (six
fraction digits, `Z`), so unchanged offsets serialize byte-identically. `kind` makes each parser reject
the other mode's checkpoints.

**Planning (`latestOffset`)**, in order:
1. Asked again for the same start: return the end planned for it (PySpark 4.0/4.1 may call
   `latestOffset()` before `initialOffset()` on a new query).
2. AvailableNow, and the run's one snapshot is planned: return the start, so the run ends.
3. `snapshot_interval_seconds` not yet passed since `start.snapshot_ts`: return the start.
4. Otherwise check the container's `_rid` against the start's, and plan `seq + 1` with
   `snapshot_ts = max(now, previous + 1 ms)`, truncated to milliseconds.

`snapshot_ts` has millisecond precision because SDP's `create_auto_cdc_from_snapshot_flow` stores a
timestamp version with full precision in `__START_AT` but passes it back to the source function as a
naive UTC `datetime` truncated to milliseconds (observed on DBR 18.3: stored `...50.316879`, passed back
`...50.316000`). A consumer comparing microseconds would select the same snapshot forever. With whole
milliseconds the round trip is exact, and the +1 ms step keeps two snapshots from sharing a millisecond
after a clock regression.

**Determinism.** `snapshot_ts` lives in the logged end offset and is copied into every partition, so
retries and replays keep it. The rows are not a function of the offset (a replay re-reads the
container): a replayed batch is still a complete snapshot taken at or after `snapshot_ts`, and the
idempotent Delta sink commits it once.

**Reading.** `partitions(start, end)` re-checks the `_rid`, reads the current feed ranges, splits them
like a batch read, and probes a `Now` session token per feed range (LatestVersion, which every account
has; sequential, about 1 RU each). Each partition runs `query` through `batch.query_documents` with that
token, so with Session consistency the snapshot includes every write acknowledged before planning.
Offsets keep no per-range state, so splits between snapshots need no handling.

**Read limits** are ignored (`getDefaultReadLimit()` returns `ReadAllAvailable`): splitting a snapshot
across batches would break its atomicity.

### Partition splits

- **Between batches**: `latestOffset` re-keys the start to the new partitions (children inherit the LSN). `partitions` overlays start and end. Tested with children progressing at different LSN rates.
- **After planning, before or while reading**: the planned range now spans two partitions. The SDK splits the token internally and round-robins the children. LSNs are monotonic per child, not globally, so the loop judges completion **per child position** rather than stopping at the first item > e.
  - If the read was scoped to a strict sub-range (sub-splitting, or ranges kept after a merge), the SDK *widens* the children to full partitions (see §1). The loop detects child positions outside `[lo, hi)`, **discards that page**, rebuilds a composite token with every child clipped to `[lo, hi)` at its pre-page LSN (`changefeed.clip_positions`), and resumes. Clipped children make the SDK send EPK filter headers.
  - Tested with the fake, which reproduces the SDK's widening; without the clipping the regression test reads 499 instead of 300 rows.
  - A live split needs more than 10,000 RU/s per partition and was not triggered, so this path is **not verified against the service**.
- **During an initial-offset probe**: if a `Beginning` probe sees more than one child, the probe raises and is retried. The `Now` probe takes the minimum child etag, which is a valid bound for every child.
- **Merges** (preview feature): ranges are never coarsened. Several offset ranges map to one partition and are read with EPK filters, which is what the SDK itself does on a merge. **Not verified**: whether LSNs of the merged partition are comparable with pre-merge positions. This is the same assumption the SDK makes.

### Replica lag

The end LSN may come from a replica that is ahead of the one serving the executor's read. A
drained page (304) whose position is below `e` therefore means "not yet visible", **not** "no
more data". The loop sleeps (0.25 s, doubling up to 5 s) and resumes from the last token, for up
to `change_feed_end_wait_seconds` (default 120). After that it fails the task and Spark retries
it. Returning early would silently drop changes, because the next batch starts at `e`. Tested
with the fake.

### Databricks engine compatibility

`_compat.py` makes the reader independent of which PySpark build drives it (see §2):
- `local_struct_type()` converts a foreign `StructType` via `StructType.fromJson(schema.jsonValue())`. It is applied in `DataSource.reader/streamReader` and `CosmosReaderBase`, so type dispatch in `schema.py` always sees local classes.
- `streaming_api_module()` returns `_engine_pyspark.sql.streaming.datasource` only if `_engine_pyspark` is already in `sys.modules`; otherwise `pyspark.sql.streaming.datasource`; otherwise `None`. The engine is imported first in Databricks workers; importing it elsewhere is avoided. The stream reader's `SupportsTriggerAvailableNow` base and `getDefaultReadLimit()` use that module.
- Fallback stand-ins `ReadMaxRows(max_rows)` / `ReadAllAvailable()` are dataclasses with the same names and fields. The engine only sends name plus fields over the wire.
- `_cap()` reads limits by class *name* (`ReadMaxRows.max_rows`), so engine and client classes both work.
- If no `SupportsTriggerAvailableNow` was found when `stream.py` was imported, the reader gets the `NoTriggerAvailableNow` placeholder and Spark would treat AvailableNow as a single batch. On PySpark 4.0/4.1 that is expected and silent (they never call the protocol). If `getDefaultReadLimit()` or `latestOffset(start, limit)` is called with the placeholder in place, a WARNING is logged once per process, naming the loaded streaming modules (`_compat.describe_streaming_modules`).
- `reportLatestOffset()` returns the unclamped positions probed by the last `latestOffset()` call. It is reporting only, and makes no extra requests.

### Per-trigger limits

- **PySpark 4.0/4.1**: `max_items_per_trigger` caps the **LSN advance**, allocated proportionally to lag with >= 1 per lagging range. It is an LSN budget, not a row cap: bulk or transactional writes put many changes under one LSN (observed: a budget of 200 gave batches of about 2,158 rows, about 10.8×; bulk-loaded data held up to about 50 documents per LSN). With `Trigger.AvailableNow`, Spark's wrapper calls `latestOffset()` once, so the run processes **one capped batch** and stops; the next run continues.
- **PySpark 4.2, Databricks serverless and SDP** (verified there): admission control is detected by signature. `getDefaultReadLimit()` returns `ReadMaxRows(max_items_per_trigger)` or `ReadAllAvailable`. `prepareForTriggerAvailableNow()` captures a target offset, so AvailableNow runs **several** capped batches up to the target and terminates.

## 6. Authentication

| `auth_type` | Driver | Executor | Fallback before expiry |
|---|---|---|---|
| `key` | account key | account key | n/a |
| `service_principal` | `ClientSecretCredential.get_token(scope)` | shipped token | rebuild `ClientSecretCredential` |
| `default` | `DefaultAzureCredential` | shipped token | rebuild `DefaultAzureCredential` |
| `service_credential` | `databricks.service_credentials.getServiceCredentialsProvider(name)`, then `pyspark.dbutils`, then `databricks.sdk.runtime.dbutils` | shipped token | same lookup on the worker |
| `access_token` | caller's token (expiry from option or JWT `exp`) | shipped token | none: clear error when expired |

Notes:
- Tokens are re-minted on the driver for every batch read and **every micro-batch**, reusing a cached token while > 20 min remain.
- Executors switch to the fallback 10 min before expiry, which is before azure-core's 300 s refresh window.
- Spark pickles the DataSource (and its options) into the task closure, so secrets given as options are in executor memory regardless. They are never logged, never in `repr`, and redacted from wrapped exception messages (`errors.redact`).

## 7. Robustness and performance

- **One `CosmosClient` per Python worker process**, keyed by `(endpoint, principal identity, client settings)`. The key never contains token values (the key hash only for account-key auth). Fresh tokens are pushed into the cached credential. Clients are closed at exit, and in `stop()` for the streaming runner.
- **Two retry layers**: SDK (throttle 9 retries / 60 s by default here, configurable) plus an outer layer (8 attempts, exponential backoff with equal jitter capped at 60 s, honoring `x-ms-retry-after-ms`) on 408/410/429/449/5xx, 404/1002 and network errors. For reads, the outer layer resumes from the last fully consumed page. The driver's metadata requests (container properties, feed ranges, `Now`/start probes) use the same bounded, transient-only policy (`retry.call_with_retry`), so sustained throttling, for example by a concurrent snapshot, does not fail query planning.
- **Streaming of rows**: generators all the way down. Memory per task is about one page.
- **Priority-based execution** (`priority=Low`), so bulk reads do not starve OLTP traffic when the account feature is on.
- **Metrics**: per partition, rows, pages, RU (`x-ms-request-charge` via `response_hook`), retries, lag waits and duration. Logged at DEBUG, or WARNING with `log_metrics=true`, so they show in executor logs.

## 8. Schemas

Spark gives `schema()` no batch/stream signal. Therefore:
- `snapshot_stream=true` selects the snapshot-stream schema (the batch schema plus `snapshot_ts`).
- Otherwise, if any change-feed option (`change_feed_mode`, `start_from`, `max_items_per_trigger`, `initial_snapshot`, `initial_snapshot_delay_seconds`) is present, the default schema is the change-feed schema; otherwise it is the batch schema.
- `readStream` with the batch default schema fails with an actionable message.
- A batch read with change-feed options fails.
- With an explicit `.schema(...)`, a stream defaults to LatestVersion.

Virtual columns:
- Both modes: `pk`, `body` (renamable with `raw_json_column`), `feed_range`, and the corrupt-record column.
- Streams additionally: `id`, `_ts`, `operation`, `lsn`, `crts`, `ttl_expired`.

Every other column is a top-level document property (exact name, then case-insensitive),
converted safely. See README for the conversion table.

## 9. Test strategy

- Unit (no network): EPK properties, options/auth/redaction, conversion and modes, token codec through the **SDK's own parser**, scripted change-feed page sequences (splits, lag, throttling, budgets), offsets (byte-stable JSON, validation, re-keying).
- End-to-end without a JVM (`tests/harness.py`): replays PySpark's call sequence (pickling the DataSource and reader, pickling partitions, JSON offsets, `setLatestSeenOffset`, admission-control calls, `records_to_arrow_batches` validation) against a **fake service** (`tests/fake_cosmos.py`). The fake uses real EPK hashing, per-partition LSN logs, LatestVersion/AVAD views, 304 positions, splits, replica lag and 429 injection.
- CI matrix: Python 3.10 to 3.13 with PySpark 4.0, 4.1 and 4.2 (the 4.2-only admission-control tests are skipped on 4.0 and 4.1), plus the lowest (4.14.0) and the highest allowed azure-cosmos.
- Databricks engine compatibility (`tests/test_databricks_engine_compat.py`): a foreign copy of `pyspark.sql.types` reproduces the engine `StructType`. A subprocess loads a fake `_engine_pyspark`, strips client PySpark to the 4.0 API, and replays the engine runner's admission-control protocol (signature check, `asdict` limit wire format, `reportLatestOffset`, AvailableNow mixin). Without `_compat.py`, 7 of its 10 tests fail.
- Local Spark (`tests/test_spark_local.py`): batch and AvailableNow stream through a real local SparkSession against the fake; runs where Java 17+ is available, and CI fails if it is skipped.
- Initial snapshot (`tests/test_initial_snapshot.py`): batch-0-only snapshot rows, call order (start probes before any snapshot page; offsets logged first), replay before and after the batch-0 commit, both PySpark call orders, admission control with and without a cap, AvailableNow (also through the engine-protocol simulation) including the target move after batch 0, offsets without the snapshot field, a split between P0 and planning and one during the query, an empty batch 0, row schema and values, session-token enforcement against a lagging replica, the delay, the property-style convergence test and its sabotage check. Planning the snapshot from the start offset's field instead of the end offset fails 18 tests; dropping the session token in `read()` fails 4.
- Query splits and duplicates (`tests/test_query_splits.py`): see §4, "Splits and duplicates during a query".
- Snapshot streams (`tests/test_snapshot_stream.py`): offset shape and validation, mode mix-ups, one complete snapshot per batch with one `snapshot_ts`, one snapshot per AvailableNow run, interval gating on both PySpark paths, replays keep `seq` and `snapshot_ts`, a clock going back, container re-creation, session tokens against a lagging replica, splits between and during snapshots, user schemas, and the Databricks engine protocol in a subprocess. Stamping rows at read time, dropping the AvailableNow budget, the `_rid` check, the monotonic stamp or the per-start cache each fail at least one test.
- Integration (`tests/test_integration.py`, `-m integration`): configured by environment variables, against an Azure Cosmos DB account or an emulator. Every test creates and deletes its own containers and seeds its own data: batch reads with key auth, a user schema, a parameterized query, `auto` planning, a V1 sub-split, multi-partition batch reads and streams, offset stability, LatestVersion incremental reads and restarts, transactional-batch LSN grouping, caps and stream sub-splits, AVAD create/replace/delete and replay determinism, LatestVersion `initial_snapshot` under concurrent writes, and a local SparkSession batch and stream against the endpoint. The AVAD initial-snapshot test seeds 2,000 documents in a one-partition container, reads the snapshot over four EPK sub-ranges while a thread inserts, updates, deletes and re-creates documents, and requires the merged result to equal the container after the first AvailableNow run; it also checks that every snapshot query carries the recorded session token and that a token ahead of the service is refused. The snapshot-stream test takes one snapshot per AvailableNow run on both PySpark protocol paths, replays a logged batch with its `snapshot_ts`, and re-creates the container.
- Capability markers (`requires_avad`, `requires_epk_ranges`, `requires_multi_partition`, `requires_session_tokens`, `requires_aad`) skip what an endpoint cannot do. CI runs the suite against the Linux vNext emulator (no AVAD, no EPK sub-range filtering, one physical partition per container, no session-token enforcement) and the legacy Linux emulator (no AVAD), and against an account when its secrets are configured.

## 10. Known risks / not verified

1. **Live partition split or merge** during streaming: covered by the fake and by reasoning from SDK code, not by the service. This includes the initial snapshot's session tokens after a split (parent token sent for child ranges).
2. **Unity Catalog service credentials** inside Python data source workers on serverless/SDP (driver-side planner and executor processes): implemented with three lookups and a clear error, but **not run on Databricks**. `access_token` (minted in the notebook) is the documented fallback.
3. **PySpark on Databricks** is a fork. Serverless jobs (environment version 4) and SDP (DBR 18.3) use 4.2-style admission control with native AvailableNow, and serverless workers mix engine and client PySpark (§2), which `_compat.py` handles. Future runtimes may change this.
4. **Continuation-token format coupling**: tokens are built in the SDK's persisted "v2" format. A future SDK that changes the format would be detected by `decode_positions` / SDK parsing errors (clear exception), not silently misread. Upgrades should run `tests/test_changefeed_offsets.py` and the integration suite.
5. **`max_items_per_trigger` is an LSN budget**, not a row cap (bulk writes packed up to about 50 docs per LSN; a budget of 200 gave batches of about 2,158 rows).
6. **AVAD retention**: a stream stopped for longer than the continuous-backup retention (7 or 30 days) cannot resume deterministically. Restart with a new checkpoint (with `initial_snapshot=true` it reads a new snapshot).
7. **TTL deletes in AVAD** (`timeToLiveExpired`): mapped per documentation, not observed against the service.
8. **`spark.sql.streaming.triggerAvailableNowWrapper.enabled=true` on PySpark 4.0/4.1** (internal, off by default): the wrapper calls `latestOffset()` before `setLatestSeenOffset` on a restart. With `max_items_per_trigger` set, the target can then be computed from a fresh start rather than the checkpoint, and the run fails. This combination is not supported. The default single-batch fallback and PySpark 4.2's native AvailableNow are fine.
9. **Initial snapshot under weak consistency**: with ConsistentPrefix or Eventual consistency only the delay protects the snapshot from lagging replicas (best effort). The snapshot also assumes that a query run to completion returns every document not deleted during it (the service's `_rid` continuation, §4); this was observed against the service, including a parent's continuation resumed on a child after a split, but it is not documented by the service.
10. **Snapshot streams**: not point-in-time, like batch reads; a snapshot of an empty container has no rows and is invisible downstream; `snapshot_ts` uses the driver's wall clock, strictly increasing within a checkpoint but not across a checkpoint reset; with `snapshot_interval_seconds=0` a continuous stream reads the whole container in every micro-batch.
11. **Regression-tested failure modes** (each has a test): batch-0 replay after a restart; SDK split widening of sub-range change-feed tokens; DECIMAL(38,s) with Python's 28-digit default context; access-token clients shared across principals; timestamp offsets >= 24h or out-of-range years under PERMISSIVE; a thread-unsafe split check in parallel start probes; mode validation on replay; the AvailableNow wrapper conf on PySpark 4.0/4.1 (documented above); PySpark 4.0/4.1 calling `latestOffset()` before `initialOffset()` on a new query.
