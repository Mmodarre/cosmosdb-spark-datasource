# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[PEP 440](https://peps.python.org/pep-0440/) and, from 1.0.0, [Semantic Versioning](https://semver.org/)
for the public API described in the README ("Versioning").

## [Unreleased]

## [1.0.0] - 2026-10-15

First public release.

### Added

- Batch reads with `spark.read.format("cosmosdb")`: one Spark partition per physical partition
  (feed range), optional EPK sub-splitting (`partitions_per_feed_range`), and a server-side `query`
  with `query_parameters`, run per partition. Each `(pk, id)` is returned at most once per read of
  a query that returns `id`, also when a partition splits during the read.
- Typed reads: user-specified schemas with documented conversions in `PERMISSIVE`, `DROPMALFORMED`
  or `FAILFAST` mode, and schema inference from a sample (`infer_schema`, `sampling_size`,
  `sampling_query`).
- Opt-in filter pushdown for batch reads (`filter_pushdown`, PySpark 4.1 and later with
  `spark.sql.python.filterPushdown.enabled=true`): Spark's filters become parameterized `WHERE`
  predicates of the query, and Spark re-applies every filter the service cannot evaluate exactly.
- Logical-partition and point reads: `partition_key`, `item_ids`, and `read_many(identities)` for a
  DataFrame of `(id, pk)` rows; pushed `pk = 'v'` and `pk IN (...)` filters with string keys on a
  non-hierarchical partition key read only those logical partitions.
- Change-feed streams with `spark.readStream.format("cosmosdb")` in `LatestVersion` and
  `AllVersionsAndDeletes` mode: micro-batches bounded by LSN per range, so a retried or replayed
  batch reads the same changes; `lsn_seq` to order the changes of a document that share an LSN;
  `max_items_per_trigger`; native `Trigger.AvailableNow` on PySpark 4.2, Databricks Runtime 18 and
  later, and serverless compute.
- `initial_snapshot`: the first micro-batch of a stream reads the whole container, later
  micro-batches the change feed from the position the stream started at.
- `snapshot_stream`: every micro-batch is one complete snapshot of the container, stamped with
  `snapshot_ts`, with `snapshot_interval_seconds` to space the snapshots.
- The change feed as a batch read: the changes between two positions (`start_from` or a batch
  checkpoint, to now or `end_at`), capped by `max_items_per_trigger`; with
  `batch_checkpoint_location`, a read repeats its range until `commit_batch_checkpoint()` marks it
  written.
- Authentication with an account key, an Entra ID service principal (client secret or
  certificate), a pre-minted access token (for example one minted on the driver from a Unity
  Catalog service credential), or `DefaultAzureCredential`. Tokens are minted on the driver and
  shipped to the tasks; secrets never appear in logs, `repr()` or error messages.
- Throughput control: `priority`, `throughput_bucket`, and an RU budget per read
  (`target_ru_per_second`).
- Client and diagnostics options: `preferred_regions`, `consistency_level`, timeouts, `tls_verify`
  and `tls_ca_bundle`, SDK and outer retry settings, `max_integrated_cache_staleness_ms`,
  per-partition metrics (`log_metrics`) and sampled slow-request logs (`slow_request_ms`).
- Read performance: query pages fetched ahead (`prefetch_pages`), Arrow record batches
  (`arrow_batches`) and 5,000-item pages.
- Option validation: an unknown option fails at `load()` with `CosmosConfigError`, naming the
  closest known option; an option that the read would accept without using it fails at `load()` or
  when the read is planned. The options Spark and Databricks add (`path`, `paths`,
  `isUserSpecifiedSchema`) are ignored, and `unknown_options=warn` logs unknown options instead of
  failing, for a runtime that adds one more.
- Runtimes: PySpark 4.0, 4.1 and 4.2 on Python 3.10 to 3.13; Databricks Runtime 15.4 LTS and later
  (dedicated and standard access mode), serverless environment version 2 and later, and Lakeflow
  Spark Declarative Pipelines; `azure-cosmos` 4.14.0 to the latest 4.x.

### Known limitations

- Read-only: there is no writer.
- Batch reads, initial snapshots and snapshot streams are not point-in-time snapshots.
- `LatestVersion` does not deliver deletes. `AllVersionsAndDeletes` starts only at `Now`, and its
  history is bounded by the account's continuous-backup retention.
- `max_items_per_trigger` is a budget of LSNs, not of rows: bulk and transactional-batch writes
  commit many documents under one LSN.
- On PySpark 4.0 and 4.1 (Databricks Runtime 15.4 LTS to 17.3 LTS), `Trigger.AvailableNow` runs one
  micro-batch per run. The internal Spark conf `spark.sql.streaming.triggerAvailableNowWrapper.enabled`
  is not supported.
- Filter pushdown needs a fresh `load()` per filtered query on Spark 4.1 to 4.3, and projections
  are not pushed.
- Unity Catalog service credentials work only through a token minted on the driver
  (`auth_type=access_token`), which is not refreshed.
- Arrow record batches are not used on Databricks Runtime 15.4 LTS, and not by default on
  serverless compute.
- No global throughput control across reads or jobs; Gateway mode only; partition merges have not
  been tested.

[Unreleased]: https://github.com/Mmodarre/cosmosdb-spark-datasource/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Mmodarre/cosmosdb-spark-datasource/releases/tag/v1.0.0
