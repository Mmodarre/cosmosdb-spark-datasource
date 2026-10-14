# Changelog

All notable changes to this project are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[PEP 440](https://peps.python.org/pep-0440/).

## [Unreleased]

First public release.

### Added

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

[Unreleased]: https://github.com/Mmodarre/cosmosdb-spark-datasource/commits/main
