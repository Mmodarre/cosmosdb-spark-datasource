# Read-side parity with the Azure Cosmos DB Spark connector

This page compares the read side of this data source (format `cosmosdb`) with Microsoft's JVM
connector `azure-cosmos-spark` (formats `cosmos.oltp` and `cosmos.oltp.changeFeed`), version
4.50.0 (`azure-cosmos-spark_4-0_2-13`). Writes are out of scope for this data source.

Connector options are quoted from its
[configuration reference](https://github.com/Azure/azure-sdk-for-java/blob/main/sdk/cosmos/azure-cosmos-spark_3/docs/configuration-reference.md)
and, where it is silent, from its source (`CosmosConfig.scala`, `CosmosPartitionPlanner.scala`,
`QueryFilterAnalyzer.scala`). Benchmark figures come from a head-to-head run on Databricks classic
compute (DBR 17.3 LTS, Spark 4.0.0, four 8-vCPU workers) against one account in the same region.

Status of this data source on `main`: **Supported** (equivalent capability), **Partially** (exists
with a material gap), **Missing** (not implemented), **N/A** (not applicable to a Python data
source).

## Summary

| Status | Rows |
|---|---|
| Supported | 27 |
| Partially | 11 |
| Missing | 3 |
| N/A | 4 |
| Not verified | 1 |

46 read-side rows; the 11 write rows of the connector are out of scope.

## 1. Authentication

| Capability | Connector | This data source | Notes |
|---|---|---|---|
| Account key | `spark.cosmos.auth.type=MasterKey`, `spark.cosmos.accountKey` | **Supported**: `auth_type=key`, `account_key` | |
| Service principal, secret | `ServicePrincipal`, `spark.cosmos.auth.aad.clientId`, `.clientSecret`, `spark.cosmos.account.tenantId` (plus subscription and resource group) | **Supported**: `auth_type=service_principal`, `tenant_id`, `client_id`, `client_secret` | Data-plane RBAC only: no subscription or resource group. Unit-tested. |
| Service principal, certificate | `spark.cosmos.auth.aad.clientCertPemBase64` | **Supported**: `client_certificate` (base64 PEM or PKCS#12), `client_certificate_password` | `CertificateCredential`; unit-tested with a real certificate, not against Entra ID. |
| Managed identity | `ManagedIdentity`, optional `spark.cosmos.auth.aad.clientId` / `resourceId` | **Partially**: `auth_type=default` (`DefaultAzureCredential`, which includes managed identity) | Tested with the Azure CLI credential, not with a managed identity. |
| Custom token provider | `AccessToken` with `spark.cosmos.accountDataResolverServiceName` (a JVM class) | **Supported**: `auth_type=access_token` (pre-minted token), `auth_type=service_credential` (Unity Catalog service credential) | |

## 2. Batch reads

| Capability | Connector | This data source | Notes |
|---|---|---|---|
| Raw JSON | `spark.cosmos.read.inferSchema.enabled=false` (`_rawBody, id, _ts`) | **Supported**: default schema `id, pk, _ts, _etag, body` | |
| Schema inference by sampling | `spark.cosmos.read.inferSchema.enabled` (default `true`), `.samplingSize` (1000), `.query`, `.includeSystemProperties`, `.includeTimestamp`, `.forceNullableProperties` | **Supported**: `infer_schema` (default `false`), `sampling_size` (1000), `sampling_query`, `infer_system_properties`, `infer_timestamp`; columns are always nullable | The same merge rules; integers are BIGINT (the connector infers INT or BIGINT), and the sample is spread over all physical partitions (the connector reads `TOP n` from the first ones). |
| User-specified schema | `.schema(...)`, `spark.cosmos.read.schemaConversionMode` `Relaxed` / `Strict` | **Supported**: `.schema(...)`, `mode` `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST`, corrupt-record column | Conversions are documented and never lossy. |
| Predicate pushdown | Automatic: `EqualTo`, `EqualNullSafe`, ranges, `In`, `IsNull`, `IsNotNull`, `StringStartsWith` / `EndsWith` / `Contains`, `And`, `Or`, `Not` | **Partially**: `filter_pushdown=true` or `auto` with `spark.sql.python.filterPushdown.enabled=true`, PySpark 4.1+; the same filters, each optionally negated | Spark's Python API pushes conjunctions of leaves only, never `Or`. Spark 4.1 to 4.3 reuse the read planned with pushed filters for other scans of the same loaded DataFrame, so each filtered query needs its own `load()`. A 1% equality filter over 200,000 documents: 81 RU instead of 4,603. |
| Custom query | `spark.cosmos.read.customQuery` | **Supported**: `query`, `query_parameters` | Run per input partition in both. |
| Projection pushdown | Not documented; the generated query is `SELECT * ... WHERE ...` | **Partially**: a projecting `query` | The Python API passes no required columns. `SELECT c.id, c._ts` cost 15.8 instead of 23.0 RU per 1,000 documents. |
| One task per physical partition | `spark.cosmos.read.partitioning.strategy=Default` / `Restrictive` | **Supported**: `partitions_per_feed_range=1` (default) | The fastest setting for both in the benchmark. |
| Storage-aligned split | `Aggressive` (about 5 GiB per partition, at least `1 + 2 x defaultParallelism`) | **Partially**: `partitions_per_feed_range=N`; `auto` reads one task per physical partition | An EPK sub-range query scans its whole physical partition, in both libraries (connector `Aggressive`: 3.4x the RU), so splitting only pays when the client, not the service, limits a partition. |
| Targeted partition count | `Custom`, `spark.cosmos.partitioning.targetedCount` | **Partially**: `partitions_per_feed_range=N` per feed range | |
| Logical-partition scoping | `spark.cosmos.partitioning.feedRangeFilter`; UDFs `GetFeedRangeForPartitionKeyValue`, `GetFeedRangeForHierarchicalPartitionKeyValues`, `GetOverlappingFeedRange` | **Supported**: `partition_key` (a key or a hierarchical prefix), and pushed `pk = 'v'` / `pk IN (...)` | One task, routed by partition key: 4.3 RU for a 50-document partition; an EPK-range query for the same key scans its physical partition (2,386 RU). |
| readMany / point reads | `spark.cosmos.read.readManyFiltering.enabled`, `_itemIdentity`, UDF `GetCosmosItemIdentityValue`; `readManyByPartitionKeys` | **Supported**: `item_ids` option; `read_many(identities, ...)` for a DataFrame of `(id, pk)` | Through `read_items`: 1,000 items for 301 RU. No `_itemIdentity` column; `readManyByPartitionKeys` maps to `partition_key`. |
| Runtime filtering | `spark.cosmos.read.runtimeFiltering.enabled` | **N/A** | No runtime-filter hook in the Python API. |
| Integrated cache | `spark.cosmos.read.maxIntegratedCacheStalenessInMS` | **Supported**: `max_integrated_cache_staleness_ms` on queries | Needs a dedicated-gateway endpoint; accepted, and ignored, by the standard gateway (checked on an account without one). |
| Page size | `spark.cosmos.read.maxItemCount` (1000) | **Supported**: `page_size` (5000) | Both libraries read fastest with 5,000-item pages in the benchmark. |
| Consistency | `spark.cosmos.read.forceEventualConsistency` (default `true`), `.consistencyStrategy` | **Supported**: `consistency_level` (default: the account's) | Same RU per read at Session and Eventual. |
| Preferred regions | `spark.cosmos.preferredRegionsList` | **Supported**: `preferred_regions` | |
| Transport | Direct mode (TCP) by default, `spark.cosmos.useGatewayMode` | **Partially**: Gateway (HTTPS) only | The Python SDK has no Direct mode. |

## 3. Change feed

| Capability | Connector | This data source | Notes |
|---|---|---|---|
| LatestVersion | `spark.cosmos.changeFeed.mode=Incremental` / `LatestVersion` | **Supported**: `change_feed_mode=LatestVersion` | |
| AllVersionsAndDeletes | `FullFidelity` / `AllVersionsAndDeletes` (documented as experimental) | **Supported**: `change_feed_mode=AllVersionsAndDeletes`, LSN-bounded, replayable micro-batches | Both returned identical changes in the benchmark. |
| Start position | `spark.cosmos.changeFeed.startFrom` `Beginning` / `Now` / point in time | **Supported**: `start_from` `Beginning` / `Now` / ISO-8601 (AllVersionsAndDeletes: `Now`) | A checkpoint wins in both. |
| Initial snapshot with CDC in one stream | none | **Supported**: `initial_snapshot=true` | This data source only. |
| Items per micro-batch | `spark.cosmos.changeFeed.itemCountPerTriggerHint` | **Partially**: `max_items_per_trigger`, a budget of LSNs | On Spark 4.0 / 4.1 a capped `Trigger.AvailableNow` run is one micro-batch (the connector's hint gave one batch of all changes); a loop of capped batch reads with a checkpoint works through a backlog there. |
| Streaming checkpoints | `checkpointLocation` | **Supported** | |
| Change feed as a batch read | `spark.read.format("cosmos.oltp.changeFeed")`, `spark.cosmos.changeFeed.batchCheckpointLocation` | **Supported**: `spark.read` with change-feed options; `end_at`; `batch_checkpoint_location` with `commit_batch_checkpoint()` | A read replays its range until the job commits it, and reports whether it caught up, so capped reads in a loop work through a backlog on Spark 4.0 and 4.1. |
| Adaptive micro-batch sizing | `spark.cosmos.changeFeed.performance.monitoring.enabled` | **Missing** | |
| Spark 2 offset migration | UDFs `CreateChangeFeedOffsetFromSpark2`, ... | **N/A** | |

## 4. Writes

Out of scope: upsert, create, conditional replace, delete, patch, read-modify-write, bulk,
transactional batch, point writes, serialization options and streaming writes
(`spark.cosmos.write.*`). Use the connector, or the Python SDK directly, to write.

## 5. Throughput control

| Capability | Connector | This data source | Notes |
|---|---|---|---|
| Local throughput control | `spark.cosmos.throughputControl.enabled`, `.name`, `.targetThroughput`, `.targetThroughputThreshold` | **Partially**: `target_ru_per_second`, an RU/s budget per read, enforced by token buckets shared by a read's tasks on each executor | No `targetThroughputThreshold` (a fraction of the provisioned RU/s). A read capped at 20 RU/s ran at 18.8 RU/s. |
| Global throughput control | `spark.cosmos.throughputControl.globalControl.*` (lease container) | **Missing** | Not provided: nothing coordinates budgets across reads or jobs. |
| Priority-based execution | `spark.cosmos.throughputControl.priorityLevel` | **Supported**: `priority=Low` / `High` | No throughput-control group needed. |
| Throughput buckets | `spark.cosmos.throughputControl.throughputBucket` | **Supported**: `throughput_bucket` (1 to 5) on every request | |
| Throttling (429) | SDK retries, `spark.cosmos.write.maxRetryCount` | **Supported**: `throttle_max_retries`, `throttle_max_wait_seconds`, `max_retries` with jittered backoff | |

## 6. Other

| Capability | Connector | This data source | Notes |
|---|---|---|---|
| Diagnostics | `spark.cosmos.diagnostics` `sampled` / `simple` / `feed` / `feed_details`, thresholds | **Partially**: `log_metrics=true` per partition; `slow_request_ms` logs slow page fetches with activity ids, partitions, RU, service time and throttling retries, at most `slow_request_log_per_minute` per worker | No per-request diagnostics strings of the SDK (the Python SDK has none). |
| Azure Monitor / OpenTelemetry | `spark.cosmos.diagnostics.azureMonitor.*` | **Missing** | |
| Spark UI custom metrics | change-feed performance metrics | **N/A** | The Python API reports only Spark's built-in Python metrics. |
| Catalog API | `CosmosCatalog`: databases, containers, throughput | **N/A** | Spark catalogs are JVM plugins. |
| Hierarchical partition keys | reads, writes, identity UDFs | **Partially**: reads (`pk` as a JSON array, MultiHash sub-splitting) | |
| Partition splits during reads | handled | **Partially**: handled, tested against a service fake, not against a live split | |
| Spark versions | one artifact per Spark minor (3.3 to 4.1) | **Supported**: one wheel for Spark 4.0+ | No Spark 3.x: the Python Data Source API starts with 4.0. |
| Databricks serverless and Lakeflow SDP | not available (no JVM libraries on serverless) | **Supported** | The main reason this data source exists. |
| Classic compute, dedicated access mode | supported | **Supported** | |
| Classic compute, standard access mode | needs the Maven coordinate on the allowlist | **Not verified** | |

## 7. Python Data Source API constraints

- `pushFilters` exists from Spark 4.1 (Databricks back-ports it to DBR 17.3) and is gated by
  `spark.sql.python.filterPushdown.enabled` (default `false`); a reader that implements it fails
  planning while the conf is off. Spark sends only leaves and negated leaves, and caches the read
  planned with pushed filters on the loaded relation up to 4.3 (see the README, "Filter pushdown").
- There is no column-pruning, limit-pushdown (before Spark's main branch) or runtime-filter hook,
  and no custom-metric hook for data sources.
- The Python SDK has no Direct mode, so every request goes through the gateway over HTTPS.
