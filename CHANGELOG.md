# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

First public release.

### Added

- Schema inference for batch reads (`infer_schema=true`): up to `sampling_size` (1000) documents of
  `sampling_query` (default: `query`), spread over the physical partitions, give typed columns.
  Types merge to the tightest common type (else STRING); `infer_system_properties` and
  `infer_timestamp` add system properties and `_ts`.
- Filter pushdown for batch reads (`filter_pushdown=true` or `auto`, PySpark 4.1+ with
  `spark.sql.python.filterPushdown.enabled=true`): comparisons, `IN`, `IS NULL` and `LIKE`
  prefix, suffix and substring filters become parameterized `WHERE` predicates of the query. Spark
  re-applies every filter except those on system properties and plain-string `pk` equality.
- Batch reads with `spark.read.format("cosmosdb")`: one Spark partition per feed range, optional
  EPK sub-splitting (`partitions_per_feed_range`, including `auto`), and a server-side `query` with
  `query_parameters`.
- Change-feed streaming with `spark.readStream.format("cosmosdb")` in `LatestVersion` and
  `AllVersionsAndDeletes` mode: LSN-bounded, replayable micro-batches, `max_items_per_trigger`,
  and native `Trigger.AvailableNow` on PySpark 4.2 and Databricks.
- `initial_snapshot`: the first micro-batch of a stream reads the whole container, later
  micro-batches read the change feed from the position the stream started at.
- Authentication with an account key, an Entra ID service principal, a pre-minted access token,
  a Unity Catalog service credential, or `DefaultAzureCredential`.
- User-specified schemas with safe type conversion and `PERMISSIVE`, `DROPMALFORMED` and
  `FAILFAST` modes.
- `snapshot_stream`: a stream in which every micro-batch is one complete snapshot of the container,
  stamped with `snapshot_ts` in whole milliseconds, with `snapshot_interval_seconds` to space the
  snapshots. One snapshot per `Trigger.AvailableNow` run.

### Fixed

- `partitions_per_feed_range=auto` split large physical partitions into EPK sub-ranges, and every
  sub-range query costs about as much RU as a scan of the whole partition (10 sub-ranges: about 10
  times the RU, and no faster). `auto` now reads each physical partition as one task, an explicit
  value above 1 logs a WARNING with the cost, and the trade-off is documented.
- A partition split while a batch read, an initial snapshot or a snapshot stream was reading a
  partition returned rows twice: azure-cosmos 4.17.1 restarts a split range from its beginning
  (4.14.0 raises 410 and then misroutes the continuation). Every query is now scoped to one
  physical partition, a split is detected from the partition that served each page, and the
  children resume from the last page's continuation.
- A document deleted and re-created during a query read was returned twice. Each `(pk, id)` is now
  returned at most once per read; the keys are kept in bounded memory (8 bytes per document,
  spilled to a temporary file beyond about 32 MiB).
- AllVersionsAndDeletes delete rows rendered a numeric partition key above 2^53 as the double the
  service reports (`9007199254740992`, `9.223372036854776e+18`) while the document's other rows
  showed the exact integer, so deletes never matched their key. Every row now renders numbers as
  the double the service hashes.
- JSON-null and undefined partition keys both rendered as `pk = NULL`, so two documents could
  share a key. JSON null now renders as `null`; undefined stays NULL (`{}` inside a hierarchical
  key).
- With `initial_snapshot` and `Trigger.AvailableNow` (PySpark 4.2, Databricks), the first run ended
  after the snapshot, so the changes made while it was read waited for the next run. The run's
  target is now moved once after the snapshot, and the first run drains them.

[Unreleased]: https://github.com/Mmodarre/cosmosdb-spark-datasource/commits/main
