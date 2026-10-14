# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

First public release.

### Added

- Service principals with a certificate (`client_certificate`, `client_certificate_password`),
  `max_integrated_cache_staleness_ms` for queries through a dedicated gateway, and sampled
  slow-request logging (`slow_request_ms`, `slow_request_log_per_minute`) with activity ids,
  partitions, request charge, service-side duration and throttling retries.
- Change feed as a batch read: `spark.read` with change-feed options reads the changes between two
  positions (`start_from` or a checkpoint, to now or `end_at`), capped by `max_items_per_trigger`.
  With `batch_checkpoint_location`, a read replays its range until `commit_batch_checkpoint()`
  marks it written, and reports whether it caught up, so capped reads in a loop work through a
  backlog on PySpark 4.0 and 4.1.
- Throughput control: `throughput_bucket` sends every request in a throughput bucket, and
  `target_ru_per_second` caps a read (or a micro-batch) with token buckets that its tasks share
  per executor, charged with each page's request charge. There is no global, cross-job control.
- Logical-partition and point reads: `partition_key` reads one logical partition (or a prefix of
  a hierarchical key) as one task routed by partition key, `item_ids` and `read_many(identities)`
  read given documents with `read_items`, and pushed `pk = 'v'` / `pk IN (...)` filters read only
  those logical partitions.
- Faster batch reads: query pages are fetched one page ahead of the conversion (`prefetch_pages`,
  default 1), rows reach Spark as Arrow record batches (`arrow_batches`, default `auto`), and row
  conversion and the encoding of `body` take fewer calls. A client-bound read of 1M documents on
  DBR 17.3 went from 131,000 to 190,000 rows/s (raw) and from 109,000 to 170,000 rows/s (typed).
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

### Changed

- The default `page_size` is 5000 (was 1000): fewer round trips and about 7% fewer RU per document
  for a full read.

### Fixed

- After a container was deleted and re-created, a new stream (new checkpoint) failed on Python
  workers that had read the old container (`Invalid continuation: [Mismatch collection rid]`)
  until the workers restarted. A change-feed read now refreshes the client's cached container
  properties and routing map when the container's `_rid` differs from the one it was planned for.
- Change-feed reads took the continuation from the client-wide `last_response_headers`, which
  concurrent reads on the shared client of a worker overwrite. Each pager now reads it from its
  own last response, and the start probes of a new stream run in parallel.
- `start_from` with a timestamp in the future behaved like `Now`: the changes made between the
  start of the stream and that time were delivered. The stream now drops the changes with an
  earlier `_ts`; the bound is stored in the offsets.
- `throttle_max_retries=0` and `throttle_max_wait_seconds=0` were accepted and ignored: azure-cosmos
  treats 0 in its keyword arguments as "use the default" (9 retries). The settings are now passed
  in the client's connection policy, so 0 disables the SDK's 429 retries.
- `partitions_per_feed_range=auto` split large physical partitions into EPK sub-ranges, and every
  sub-range query costs about as much RU as a scan of the whole partition (10 sub-ranges: about 10
  times the RU, and no faster). `auto` now reads each physical partition as one task, an explicit
  value above 1 logs a WARNING with the cost, and the trade-off is documented.
- AllVersionsAndDeletes changes of one document that share an LSN could not be ordered with the
  documented `sequence_by` (`_ts`, `lsn`): a transactional batch that writes a document several
  times, and a multi-region-write conflict, which arrives as a delete and a create at one LSN with
  one `_ts`, so AUTO CDC could keep a stale version or drop a live document. Change-feed rows have
  a new column `lsn_seq`, the position among the changes of the same document at the same LSN, and
  the documented ordering is `struct(operation <> 'snapshot', lsn, lsn_seq)`.
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
