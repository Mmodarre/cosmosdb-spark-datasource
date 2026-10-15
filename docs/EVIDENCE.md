# Test evidence

This document consolidates the testing of `cosmosdb-spark-datasource` from the first tested build (0.2.0) to the
1.0.0 release, on local Apache Spark, on Azure Databricks (classic compute, serverless compute, Lakeflow Spark
Declarative Pipelines) and against a live Azure Cosmos DB for NoSQL account, including a benchmark against the
Azure Cosmos DB Spark connector (`azure-cosmos-spark`). Times are UTC, October 2026. Raw evidence (run records,
logs, repro scripts) is kept with the test harness and is not part of this repository; the GitHub Actions runs
cited below are public.

**Last build tested:** release candidate RC2, `cosmosdb_spark_datasource-1.0.0-py3-none-any.whl`, sha256
`5db8b4eb190a8d36660326ac9bd799eb2343208cd238ef0a8bfcb0e5455b2003`, built unmodified from commit `002b65d`. The
findings of the RC2 round were fixed or documented after it (section 12); those changes are covered by CI.

**How the final round was run.** The full validation ran on a pre-release build of commit `59c8bd1`
(`0.4.0.dev1`, sha256 `b78091c019ca6ba68015713b19f7af64e19ff88654666dea312b56eded1b365b`, only the version string
changed). The defects it found were fixed for 1.0.0, and two release candidates were re-checked on the affected
paths: RC1 (`b4f987a`, sha256 `df0d0533c2ec2ebdd37b7f2c0a45d84b16392ad3f287e13728395041b0c4070f`) and RC2
(`002b65d`).

## Summary

| Area | Builds | What was tested | Result |
|---|---|---|---|
| Unit tests and CI | 0.2.0 to 1.0.0 | the package suite (service fake, JVM-backed local Spark) on Python 3.10-3.13 x PySpark 4.0-4.2 x azure-cosmos 4.14.0 / 4.17.1; emulator integration tests | **PASS** on every build; RC2: 703 passed (PySpark 4.2), 685 + 18 skipped (4.0, 4.1), coverage 95% |
| Local Apache Spark against the service | 0.2.0, 0.4.0.dev1 | OSS Spark 4.0.4 / 4.1.3 / 4.2.0: batch reads, streams, AllVersionsAndDeletes + initial snapshot under a logged writer, the options added since 0.2.0 | **PASS**; one defect on PySpark 4.2 (empty change-feed batch reads), fixed in 1.0.0 |
| Live QA against the service | 0.2.0 (34 cases), 0.4.0.dev1 (regression) | live splits, many partitions, re-creates, every key shape, extreme values, TTL, caps, transactional batches, consistency levels, multi-region writes, auth, 429s, network faults | core correctness held in both rounds; 8 defects found in 0.2.0, all fixed |
| Databricks runtime matrix | 0.2.0, 0.4.0.dev1, RC1, RC2 | DBR 14.3-19.x, standard access mode, serverless environments 1-5, auth paths, AvailableNow behaviour, SDP on classic | **PASS** on DBR 15.4 to 19.x and serverless 2-5; defects found on the way are fixed in 1.0.0 |
| Lakeflow SDP pipelines | 0.2.0, 0.3.x | CDC SCD1 with initial snapshot, LatestVersion, SCD2 snapshots, stops, replays, full refreshes, reconciled against Cosmos DB | **PASS** |
| Lakehouse Plumber project | 0.3.1 | generated pipelines and job, three rounds with mutations | **PASS** |
| Benchmark vs `azure-cosmos-spark` 4.50.0 | 0.2.0, 0.4.0.dev1, RC2 | 10M documents on classic DBR 17.3: full reads, filters, change feed, point reads, RU budgets | client-bound reads take **about 2x (raw) to 2.5x (typed)** the connector's time (0.2.0: 2.4x / 3.4x); RU equal or 8% lower; correctness exact; details and the reasons in section 7 |
| Databricks serverless | 0.4.0.dev1, RC1, RC2 | pushdown, Arrow, volume checkpoints, change feed batch reads, snapshot streams, initial snapshot, point reads | **PASS**; filter pushdown is not available on serverless (the Spark conf cannot be set); `arrow_batches=true` works |
| Managed identity (UC service credential) | 0.4.0.dev1, RC1, RC2 | token minted on the driver in a serverless job, a classic job and SDP pipeline code | **PASS** everywhere, least privilege shown; tokens last about 24 hours |

## Builds

| Build | sha256 | Used for |
|---|---|---|
| 0.2.0 | `2df5716c0a55f6699f3b9182285de48e57793e4cfba1931633b4a606f163b545` | first pipeline round, first live and Databricks QA rounds, first benchmark |
| 0.3.0 / 0.3.1 | `2848ff571d81b7efe4848548018935d1a73314d96ceab0e5efb00da13a7ed69b` / `1b777cb02733e4e510a9d50088034eacc764fffa8612bc7336ea725f8666d807` | second pipeline round, Lakehouse Plumber verification |
| 0.4.0.dev1 (`59c8bd1`) | `b78091c019ca6ba68015713b19f7af64e19ff88654666dea312b56eded1b365b` | final validation round |
| 1.0.0 RC1 (`b4f987a`) | `df0d0533c2ec2ebdd37b7f2c0a45d84b16392ad3f287e13728395041b0c4070f` | re-check of the fixes |
| **1.0.0 RC2 (`002b65d`)** | `5db8b4eb190a8d36660326ac9bd799eb2343208cd238ef0a8bfcb0e5455b2003` | re-check of the fixes, benchmark re-check, correctness |

## 1. Unit tests and CI

The package's own suite runs against a service-faithful fake of Cosmos DB (partition splits, 410s, 429s, session
tokens) and includes JVM-backed local Spark tests; integration tests run against both Cosmos DB emulators.

| Build | How | Result |
|---|---|---|
| 0.2.0 | 10 clean environments: Python 3.10-3.13 x PySpark 4.2.0, PySpark 4.0.4 and 4.1.3, azure-cosmos 4.14.0 and 4.17.1, with a JVM | PASS: 284 passed (4.2), 278 passed + 6 skipped (4.0, 4.1) |
| 0.3.0 | PySpark 4.2.0, 4.1.1, 4.0.1; live integration tests against an account | PASS: 321 passed (4.2), 304 passed + 19 skipped (4.0, 4.1) |
| `59c8bd1` (0.4.0.dev1) | GitHub Actions [CI run 38661091532](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/runs/38661091532): lint, type check, build, 15 test jobs (Python 3.10-3.13 x PySpark 4.0 / 4.1 / 4.2, azure-cosmos 4.17.1, plus 4.14.0 and latest on 4.2); [integration run 38660465891](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/runs/38660465891) on the same tree; the same suite against the built wheel in 10 clean local environments | PASS: 580 passed (4.2), 562 passed + 18 skipped (4.0, 4.1); coverage 94%; emulators: 16 and 22 integration tests passed |
| `b4f987a` (RC1) | [CI run 38678997303](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/runs/38678997303); integration on the pull request head | PASS: 695 passed (4.2), 677 passed + 18 skipped (4.0, 4.1); 6 emulator jobs passed |
| **`002b65d` (RC2)** | [CI run 38683029926](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/runs/38683029926) (19 jobs); [integration run 38681722665](https://github.com/Mmodarre/cosmosdb-spark-datasource/actions/runs/38681722665) on the pull request head | **PASS**: 703 passed (4.2), 685 passed + 18 skipped (4.0, 4.1); `test_spark_local` 10 passed in every job; coverage 95%; 6 emulator jobs (azure-cosmos minimum and latest) passed; the job that needs an Azure account was skipped |

## 2. Local Apache Spark against the service

Real local Spark (`local[N]`, JDK 17) on PySpark 4.0.4, 4.1.3 and 4.2.0 with azure-cosmos 4.17.1, reading a live
account.

| Build | What | Result |
|---|---|---|
| 0.2.0, 0.4.0.dev1 | smoke suite: batch (default, EPK sub-split, typed), LatestVersion AvailableNow (incremental, restart, idle run), transactional-batch LSNs, AllVersionsAndDeletes + `initial_snapshot` under a concurrent writer whose every acknowledged write is logged with its LSN, secret checks | **PASS**, 16/16 checks per PySpark version on both builds; every logged write delivered exactly once; merged state equal to the container |
| 0.4.0.dev1 | the options new since 0.2.0 on all three versions, against a 3,000-document container, RU from `x-ms-request-charge` | PASS: filter pushdown on 4.1 / 4.2 returns exactly the unpushed rows for 14 filter shapes and costs 4.48 RU instead of 86.33 for a 1% filter (no effect on 4.0; from 1.0.0, `filter_pushdown=true` fails there with guidance); Arrow on and off give identical rows; `partition_key` is one task (5.08 RU); `item_ids`, `read_many`, change feed as a batch read (capped loop, replay before commit), `snapshot_stream`, `target_ru_per_second` (0.88-0.90 of the budget), `throughput_bucket` and `infer_schema` behave as documented. Empty change-feed batch reads failed on PySpark 4.2 (fixed in 1.0.0, section 12) |

## 3. Live QA against the service

**0.2.0 round:** 34 test cases on local Spark 4.0 / 4.1 / 4.2 against one shared account plus temporary accounts
with Eventual default consistency, multi-region writes and serverless throughput:
- live partition splits 1 -> 2 -> 3 under a 16 ops/s writer, with streams running and every write logged (33,591 of
  33,591 delivered exactly once by AllVersionsAndDeletes);
- 30 physical partitions; container and database deletion and re-creation;
- every partition-key shape (hierarchical, numeric, boolean, null / missing, nested, very long);
- 2 MB documents, extreme numbers, field names, the three conversion modes;
- TTL deletes, `start_from` variants, empty containers, `max_items_per_trigger` on bulk data (up to 100x the LSN
  budget in rows), transactional batches;
- a 64-minute stream with a hard kill and restart;
- Eventual consistency, multi-region writes, serverless and autoscale accounts, priority-based execution;
- every auth mode, least-privilege RBAC, secret leakage in plans, logs and checkpoints;
- sustained 429s and network interruptions (connection resets, a 90 s black-hole).

Core correctness held throughout; 8 defects were found (section 13).

**Final round (0.4.0.dev1):** regression of the shape, value, large-document, TTL, `start_from` (including a
future time), empty-container, cap, transactional-batch, delete / re-create, auth, sustained-429 and
network-interruption suites on PySpark 4.2.0 (and 4.0.4 where the runtime path matters), with the documented
ordering `struct(operation <> 'snapshot', lsn, lsn_seq)`. **PASS**: every AllVersionsAndDeletes stream delivered
every logged write exactly once and every merge equalled the container; all 8 earlier defects are fixed. Not
repeated: the live split and 30-partition tests (cost), and the tests that needed the temporary accounts.

## 4. Azure Databricks runtime matrix

**0.2.0 round:** 35 one-time job runs (classic single-node job clusters and serverless jobs) plus one classic SDP
pipeline. Cases: batch default and typed, EPK sub-split, LatestVersion AvailableNow, capped streams,
AllVersionsAndDeletes + `initial_snapshot` under writes reconciled against the container, restart, Delta sink,
concurrent streams, auth paths, job retries during the first micro-batch, 50 physical partitions, checkpoint
portability across runtimes, a live partition split. **PASS on 11 configurations**: DBR 15.4, 16.4, 17.3, 17.3
standard access mode, 18.3, 19.x; serverless environments 2 to 5; a serverless notebook. DBR 14.3 and serverless
environment 1 do not have the streaming data source API (now a clear `ImportError`). Capped AvailableNow runs one
micro-batch per run on DBR 15.4-17.3 and several on DBR 18+, 19 and serverless.

**Final round (0.4.0.dev1)** on the rows the fixes touched: `service_credential` is rejected with guidance on
serverless 2-5, DBR 15.4, 16.4, 17.3, 17.3 standard, 18 and 19; the unsupported-runtime message names the
supported runtimes; capped AvailableNow is unchanged; with `initial_snapshot` the first AvailableNow run is complete
under a forced race on DBR 18, 19 and serverless (snapshot only in run 1 on DBR 15.4-17.3, as documented);
`spark.sql.streaming.triggerAvailableNowWrapper.enabled` is unset on every classic runtime and not readable on
serverless, and no stream stalled. Two defects were found and fixed for 1.0.0: the default batch read failed on DBR
15.4 (Arrow batches), and empty change-feed batch reads failed where the engine passes `InputPartition(None)`
(DBR 18, 19, serverless).

## 5. Lakeflow Spark Declarative Pipelines

Judged by a reconcile job that reads every document's `(pk, id, _ts, _etag)` from Cosmos DB and compares it with the
target table on a quiescent container: a result passes only with 0 missing, 0 extra, 0 stale and 0 duplicate keys.
The reconcile was shown to fail on deliberately wrong tables.

| Round | Build | What | Result |
|---|---|---|---|
| 1 | 0.2.0 | serverless SDP (DBR 18.3): AllVersionsAndDeletes + `initial_snapshot` into AUTO CDC SCD1 (1M documents), full refresh with writes during the snapshot, incremental and no-op updates, an update stopped mid-processing, a full refresh stopped during the snapshot and replayed, silver-only rebuild, bronze-only refresh, LatestVersion SCD1, SCD2 from snapshots, guards | **PASS** (every reconcile exact; the race between snapshot and changes was proven to happen) |
| 2 | 0.3.0, 0.3.1 | first-run completeness (snapshot and the changes made during it in one update), snapshot stream + SCD2 history, guard as a job task | **PASS** (1,001,000 = 1,001,000 after a single update; one atomic commit per snapshot; SCD2 versions equal to the mutations) |

## 6. Lakehouse Plumber project

Build 0.3.1: `lhp validate --strict` and `lhp generate` for two environments, `databricks bundle validate --strict`,
the generated job order; five generated pipelines and the job deployed and run for three rounds with mutations.
**PASS**: 10 of 11 reconciles passed; the remaining one exposed a job wiring error (duplicate task key), fixed and
reconciled with 0 differences. The generated code's assumptions (`dbutils` in SDP Python files, preset merge,
`stream(...)` SQL, `cluster_by`, table properties) held.

## 7. Benchmark against the Azure Cosmos DB Spark connector

**Set-up.** Classic Azure Databricks cluster, DBR 17.3 LTS (Spark 4.0.0), dedicated access mode, driver + 4 (then
8) `Standard_D8ds_v5` workers, in the same region as the account. Connector
`com.azure.cosmos.spark:azure-cosmos-spark_4-0_2-13` **4.50.0** (the latest release on Maven Central at the time).
Container: 10,000,000 documents of about 0.55 KB, 17 physical partitions, provisioned throughput 100,000 RU/s (and
3,000 RU/s for the RU-bound case). Each configuration: a warm-up per library, then 3 measured runs in alternating
order; medians reported. RU and throttled requests from Azure Monitor per minute-aligned run, cross-checked
against the `x-ms-request-charge` response headers (a full read: 212,037 RU from the headers, 212,141 from Azure
Monitor). Executor CPU from `/proc/stat` on every worker. Both libraries at their defaults unless stated (this
package: 5,000-item pages, one page read ahead, Arrow batches; the connector: 1,000-item pages).

| Workload | Connector 4.50.0 | 0.2.0 | 0.4.0.dev1 | 0.4.0.dev1 / connector | 0.2.0 / connector |
|---|---|---|---|---|---|
| Full read, raw JSON, 4 workers | 12.3 s | 27.8 s | **24.3 s** | **1.98x** | 2.40x |
| Full read, typed schema (16 columns, nested structs) | 10.6 s | 36.5 s | **26.3 s** | **2.48x** | 3.38x |
| Full read to Delta | 15.6 s | 34.3 s | 30.2 s | 1.94x | 2.14x |
| Full read raw, 8 workers | 10.8 s | 26.1 s | 20.0 s | 1.85x | 2.37x |
| Full read typed, 8 workers | 9.8 s | 31.8 s | 21.4 s | 2.18x | 3.18x |
| Best setting per library, 8 workers (connector 5,000-item pages; this package's defaults) | 9.3 s | 23.6 s | 20.1 s | 2.16x | 2.43x |
| RU-bound full read at 3,000 RU/s (median of 6) | 71.1 s | 73.6 s | **96.4 s** | **1.36x** | 1.02x |
| Same, page sizes swapped (this package 1,000, connector 5,000) | 91.2 s | - | 74.0 s | 0.81x | - |
| Selective filter (1% of documents): connector pushdown vs `filter_pushdown=true` | 0.9 s, 3,597 RU | (only `query`: 2.5 s, 3,434 RU) | **1.6 s, 3,207 RU** | 1.7x time, 0.89x RU | 2.3x |
| Same filter applied by Spark only (full scan) | - | 35.2 s, 230,022 RU | 26.1 s, 212,041 RU | - | - |
| LatestVersion change feed from the beginning, AvailableNow, 10M changes | 25.5 s | 43.2 s | 39.1 s | 1.53x | 1.66x |
| Change feed as a batch read, 10M changes | 21.1 s | - | 34.6 s | 1.64x | - |
| One logical partition | 0.3 s with `feedRangeFilter` (8 RU) | - | 0.3 s with a pushed `pk =` filter (6 RU); 1.1 s with `partition_key` (7 RU) | 1.0x / 3.7x | - |
| `readMany` / `read_many`, 1,000 and 10,000 identities | 0.4 / 0.4 s, 1,175 / 3,787 RU | - | 0.4 / 0.6 s, 1,174 / 3,786 RU | 1.0x / 1.5x | - |
| RU budget 2,000 and 5,000 RU/s: achieved rate | 500 / 1,200 RU/s (25% of the target; client-side throughput control with a global control container) | - | **1,946 / 4,835 RU/s (97%)** with `target_ru_per_second` | - | - |
| RU per 1,000 documents (scan) / per 1,000 changes | 23.02 / 57.53 | 23.0 / 57.6 | 21.21 / 57.52 | 0.92x / 1.00x | 1.00x / 1.00x |
| Executor CPU per raw full read | 146 CPU-s | 332 | 404 | 2.8x | 2.7x |
| **RC2: full read raw / typed, 4 workers (connector in the same session)** | 11.8 / 10.9 s | - | **23.7 / 27.3 s** (RC2) | **2.01x / 2.50x** | - |

**Where this package is still slower, and why.**
- **Per-document CPU in Python.** With one Spark task per physical partition, each task reads about 26,000
  documents/s at one full core (the limit of one Python worker process). Single-threaded, the azure-cosmos SDK alone
  needs about 19 us per document (page fetch and JSON decode with 5,000-item pages); row building and Arrow add 9 to
  10 us. The connector does the same work in the JVM at about 0.7 cores per task and twice the rate.
- **Parallelism is one task per physical partition** (17 here, on 32 or 64 cores). EPK sub-splitting does not help a
  full scan: each sub-range query costs about the RU of a scan of the whole physical partition, in both libraries.
- **Read-ahead trades CPU for time**: 22% more executor CPU than 0.2.0 for 13% less wall time. Ablation (raw read):
  0.2.0-equivalent settings 30.3 s; 1,000-item pages 28.4 s; without read-ahead 26.1 s; without Arrow 24.8 s;
  defaults 24.3 s. Typed: 33.7 s, without Arrow 30.5 s, defaults 26.3 s, so Arrow batches matter for typed reads.
- **Under throttling, 5,000-item pages use only about 75% of the provisioned throughput**, in both libraries (this
  package 96 s vs 74 s with 1,000-item pages; the connector 91 s vs 71 s). Use `page_size=1000` for reads that are
  limited by RU/s.
- The connector's change feed as a batch read with `spark.cosmos.changeFeed.batchCheckpointLocation` did not move
  forward between reads in this test (30 reads returned the same first 1,913,890 changes), and its checkpoint
  location cannot be a Unity Catalog volume on DBR 17.3. This package drained the 10M changes in 6 capped reads with
  `batch_checkpoint_location` on a volume, for 2% more RU than one uncapped read.
- The connector's client-side throughput control used a quarter of its target in this set-up (one control
  container, 4 executors); the cause was not investigated.

**Correctness.** On RC2, a full read of the 10M documents to Delta was compared with a plain-SDK
ground truth (`SELECT c.id, c._ts FROM c`, itself checked against the generator's id range): 10,000,000 rows,
10,000,000 distinct ids, 0 missing, 0 extra, 0 `_ts` mismatches, for this package and for the connector; 20,105
sampled document bodies were identical between the two. An AllVersionsAndDeletes capture of 12,400 point
operations (creates, replaces, deletes, delete-then-recreate), both libraries started at `Now`, gave 12,400 rows in
each, identical to each other and to the expected `(id, lsn, operation)` set taken from the session token of every
write. The connector reports `_ts = -1` for deletes; this package reports the delete's `crts`.

## 8. Databricks serverless

Serverless jobs on environment versions 2 to 5 (client PySpark 3.5.0, 3.5.2, 4.0.0, 4.1.0; server Spark 4.2.0),
the wheel as an environment dependency, the installed package files checked against the wheel in every run.
Build 0.4.0.dev1, re-checked on RC1 and RC2 (section 11).

| Case | Result |
|---|---|
| Filter pushdown | **Not available on serverless**: `spark.sql.python.filterPushdown.enabled` can be neither read nor set (`CONFIG_NOT_AVAILABLE`) on environments 2-5. `filter_pushdown=auto` reads without pushdown (correct rows, a full scan); `true` fails at planning with `DATA_SOURCE_PUSHDOWN_DISABLED`. The `query` option filters on the service (13 RU instead of 437 for a 1% filter of 20,000 documents) |
| `arrow_batches=true` | **Works** on environments 2-5: raw and a 19-column typed schema (timestamps, dates, decimals, binary, maps, nested structs and arrays, nulls, the corrupt-record column) give rows identical to `false`; the worker's record batches match the engine's Arrow schema. `auto` stays off on serverless |
| Streaming checkpoints on a UC volume | PASS (LatestVersion AvailableNow 20,000 rows, restart 0; capped: 8 micro-batches in one run) |
| Change feed as a batch read with `batch_checkpoint_location` on a volume | PASS for reads with changes (capped loop of 7 reads = the 295 live documents; a replay before the commit re-reads the same range; AllVersionsAndDeletes exactly 50 creates, 20 replaces, 10 deletes). Empty reads failed on 0.4.0.dev1 and pass on 1.0.0 |
| `snapshot_stream` | PASS (one snapshot per AvailableNow run, equal to the container, `snapshot_ts` strictly increasing) |
| `initial_snapshot` + AllVersionsAndDeletes with writes during the snapshot | PASS: complete after the first run; 29 keys changed after the snapshot had read them; merge by `(operation <> 'snapshot', lsn, lsn_seq)` equal to the container; no duplicate change rows |
| `partition_key`, `item_ids`, `read_many` | PASS (`partition_key` one task, 13.8 RU for 100 documents; missing ids return no row); on 0.4.0.dev1, `read_many` returned a repeated identity once per occurrence when the repeats fell into different Arrow batches; 1.0.0 removes repeated identities before reading |
| `auth_type=service_credential` | rejected with guidance before any request (at the first analysis on Spark Connect) |

## 9. Authentication

- **Account key, access token, `DefaultAzureCredential` (Azure CLI), malformed and expired credentials, least
  privilege (Data Reader on one container), secret leakage**: tested live on local Spark (0.2.0, regression on
  0.4.0.dev1). No secret appeared in any plan, log, checkpoint or error message.
- **Service principal with a client secret**: verified on serverless, classic dedicated and standard access mode
  (0.2.0 round), with least privilege.
- **Managed identity through a Unity Catalog service credential** (an access connector's system-assigned identity
  with *Cosmos DB Built-in Data Reader*; the token minted on the driver with
  `dbutils.credentials.getServiceCredentialsProvider(<name>).get_token(...)`, then `auth_type=access_token`):

  | Where | Build | Result |
  |---|---|---|
  | Serverless job (environment 4) | 0.4.0.dev1 | **PASS**: batch read 200/200, AvailableNow stream 200/200, a write with the same token refused (403) |
  | Classic DBR 17.3 LTS job | 0.4.0.dev1 | **PASS** (same checks) |
  | Serverless SDP pipeline, token minted in the pipeline source | 0.4.0.dev1, RC1, **RC2** | **PASS** on all three: `dbutils.credentials.getServiceCredentialsProvider` works in pipeline code; streaming table 200 rows, 200 distinct |

  The tokens lasted about **24 hours** (81,044 to 86,398 s at mint time). Each triggered pipeline update mints a new
  token; a continuous pipeline or a long-running stream keeps the token it started with, so use
  `service_principal` for those.
- **`auth_type=default` on Databricks** finds no customer identity: serverless and standard access mode expose no
  IMDS, and dedicated mode returns the workspace's Databricks-managed identity.

## 10. Options that Databricks adds to a read

A probe data source that returns its own option keys ran on serverless environments 2, 4 and 5, DBR 15.4, 16.4,
17.3 (dedicated and standard access mode) and in a serverless SDP pipeline (a materialized view and a streaming
table). The result was identical everywhere: `spark.read...schema(...)` adds `isuserspecifiedschema`;
`load("somepath")` adds `path`; option names reach the data source lowercased; `readStream` adds nothing, with or
without a schema. 1.0.0 accepts and ignores these keys.

## 11. Release-candidate re-checks

| Check | RC1 (`b4f987a`) | RC2 (`002b65d`) |
|---|---|---|
| Empty change-feed batch reads (idle LatestVersion from `Now`, first AllVersionsAndDeletes checkpointed read, idle read after catching up), serverless 4 / 5 | 0 rows, no error | 0 rows, no error |
| Default batch read on DBR 15.4 | works (Arrow off); `arrow_batches=true` raises with guidance | same |
| Default batch read with Arrow on, DBR 16.4 / 17.3 | rows identical to `arrow_batches=false` | same, also on 17.3 standard access mode |
| `arrow_batches=true` on serverless 4 / 5 | identical rows | identical rows (raw and typed) |
| Typed batch reads (`.schema(...)`) on serverless 4 / 5, DBR 15.4, 16.4, 17.3 | **failed**: the strict option parser rejected `isuserspecifiedschema` | **pass** (20,000 rows, the 5 expected corrupt records; pushdown variants with a schema return the right rows) |
| Default batch read, AvailableNow stream, `read_many`, `service_credential` rejection | pass | pass |
| SDP pipeline with managed identity | pass | pass |
| Benchmark full read raw / typed, 4 workers | - | 23.7 s / 27.3 s, no regression against 0.4.0.dev1 (24.3 s / 26.3 s); the same RU per read |
| Correctness (10M-document full read; AllVersionsAndDeletes capture) | - | exact; identical to the connector |

## 12. Defects found, and their status in 1.0.0

| Found in | Severity | Defect | Status in 1.0.0 |
|---|---|---|---|
| live QA, 0.2.0 | High | delete rows of AllVersionsAndDeletes rendered numeric partition keys above 2^53 differently from the other rows | fixed (every row renders the double the service hashes) |
| live QA, 0.2.0 | Low | JSON-null and undefined partition keys both rendered as NULL | fixed (`null` vs NULL) |
| live QA, 0.2.0 | High | versions of a document that share an LSN (transactional batches, multi-region-write conflicts) could not be ordered | fixed: `lsn_seq` column, ordering `struct(operation <> 'snapshot', lsn, lsn_seq)` |
| live QA, 0.2.0 | Medium | a `start_from` in the future behaved like `Now` | fixed |
| live QA, 0.2.0 | Medium | after a container was re-created, a new stream failed on reused Python workers | fixed |
| live QA, 0.2.0 | High | a partition split during a query returned rows twice; documents re-created during a read were returned twice | fixed (offline split tests; live re-create test: 0 duplicates against 100 on 0.2.0) |
| live QA, 0.2.0 | Low | `throttle_max_retries=0` was ignored | fixed (verified live under 429s) |
| live QA, 0.2.0 | Low | a replay planned before two splits once failed with `RecursionError` in the SDK | fixed |
| benchmark, 0.2.0 | High (cost) | EPK sub-splitting multiplied the RU of a scan; `auto` sub-split large partitions | fixed: `auto` = one task per physical partition, a warning states the cost of N > 1 |
| Databricks QA, 0.2.0 | High | `auth_type=service_credential` failed on every compute type | now rejected while parsing options, with the working alternatives |
| Databricks QA, 0.2.0 | Low | unsupported runtimes failed with a bare `ImportError` | fixed: the message names the supported runtimes |
| final round, 0.4.0.dev1 | High | a change-feed batch read with nothing to read failed with `AttributeError` on PySpark 4.2, DBR 18+ and serverless (the engine passes `InputPartition(None)`) | fixed |
| final round, 0.4.0.dev1 | High | the default batch read failed on DBR 15.4 LTS (its worker takes each Arrow record batch for one row) | fixed: Arrow off on DBR 15.x, `arrow_batches=true` raises with guidance |
| RC1 | Blocker | every batch read with `.schema(...)` failed: the strict option parser rejected the option Databricks adds | fixed in 1.0.0 (RC2) |
| RC2 | Low | on DBR 15.4 `filter_pushdown=true` is accepted and does nothing (correct rows, full scan) | fixed in 1.0.0: `filter_pushdown=true` fails with `CosmosConfigError` where the runtime has no pushdown API (OSS PySpark 4.0, DBR 15.4); PySpark 4.1+, DBR 17.3 and serverless keep their behaviour |
| final round | Low | a malformed `client_certificate` surfaces as a raw `ValueError` that does not name the option | fixed in 1.0.0: `CosmosAuthError` that names `client_certificate` (and `client_certificate_password` when set), with the cause chained and no secret in the message |
| final round | Low | `read_many` returns a repeated identity once per occurrence when the repeats fall into different Arrow batches | fixed in 1.0.0: repeated identities are removed before reading |
| final round | Low | `log_metrics` reports no request charge for `read_many` | fixed in 1.0.0: every task logs its identities, the documents found and their request charge |
| benchmark, final round | Medium | with the default 5,000-item pages, reads limited by RU/s are about 30% slower than with 1,000-item pages | documented in 1.0.0: use `page_size=1000` for reads limited by RU/s |
| final round | Low | the "one `load()` per filtered query" caveat of Spark 4.1-4.3 is real and silent (a reused DataFrame returned 39 of 3,000 rows) | documented in 1.0.0, as a warning in the README |

## 13. Known limitations and not tested

Not tested:
- **Partition merge** (accounts with merge enabled do not support AllVersionsAndDeletes; enabling merge is an
  account-level preview change).
- **Private endpoints through a Databricks network connectivity configuration** (needs account-level access). The
  data source needs HTTPS on port 443 only (Gateway mode).
- **Service principal with a certificate against Entra ID** (unit tests only; the malformed-value path ran live).
- **Managed identity through IMDS** (`auth_type=default`): not available to customer identities on Databricks. The
  managed-identity path through a UC service credential is tested (section 9).
- Token refresh on executors for service-principal reads longer than the token lifetime; region failover;
  AllVersionsAndDeletes beyond the continuous-backup retention; writes (the package has no writer).

Tested only in part:
- **Standard access mode**: DBR 17.3 only (the full functional matrix in the 0.2.0 round; option probe, typed,
  pushdown and Arrow reads, `read_many`, capped AvailableNow and `initial_snapshot` in the final round).
- **Live partition splits**: run against the service in the 0.2.0 round (local Spark and Databricks); for the final
  build only offline split tests and a live re-create-during-read test.

Not re-run on the final build (last run on earlier builds): the SDP pipeline rounds and the Lakehouse Plumber
project (0.3.1); Eventual consistency, multi-region writes, serverless and autoscale accounts, priority-based
execution and container-scoped RBAC (0.2.0); 30 and 50 physical partitions, job retries during the first
micro-batch, checkpoint portability across runtimes, `service_principal` on Databricks, serverless notebooks.
