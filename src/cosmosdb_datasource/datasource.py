"""``cosmosdb`` Python data source for Azure Cosmos DB for NoSQL."""

from __future__ import annotations

from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader
from pyspark.sql.types import StructType

from cosmosdb_datasource import _compat
from cosmosdb_datasource._compat import filter_pushdown_planner, local_struct_type
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import (
    AUTO,
    STREAM_ONLY_OPTIONS,
    STREAM_SNAPSHOT_OPTIONS,
    parse_options,
    unused_options,
)
from cosmosdb_datasource.schema import batch_schema, change_feed_schema, snapshot_stream_schema


class CosmosDBDataSource(DataSource):
    """Batch (query) and streaming (change feed, or snapshot stream) reads from an Azure Cosmos DB container.

    Register once per session, then use the short name ``cosmosdb``::

        spark.dataSource.register(CosmosDBDataSource)
        df = spark.read.format("cosmosdb").option("endpoint", ...).load()
    """

    @classmethod
    def name(cls) -> str:
        return "cosmosdb"

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        # Validate eagerly so configuration errors surface at .load(); no network calls here.
        self._opts = parse_options(options)

    def schema(self) -> StructType:
        # Spark gives schema() no batch/stream signal, so snapshot_stream selects the snapshot-stream
        # schema and change-feed options select the change-feed schema (see README "Schemas").
        raw = self._opts.raw_json_column
        if self._opts.infer_schema:
            from cosmosdb_datasource.inference import infer_schema_from_container

            return infer_schema_from_container(self._opts)
        if self._opts.snapshot_stream:
            return snapshot_stream_schema(raw)
        if self._opts.stream_options_present or self._opts.batch_change_feed_options_present:
            return change_feed_schema(raw)
        return batch_schema(raw)

    def reader(self, schema: StructType) -> DataSourceReader:
        schema = local_struct_type(schema)
        stream_only = (
            *(n for n in self._opts.stream_options_present if n in STREAM_SNAPSHOT_OPTIONS),
            *(("snapshot_stream",) if self._opts.snapshot_stream else ()),
        )
        if stream_only:
            raise CosmosConfigError(
                f"Option(s) {', '.join(stream_only)} only apply to spark.readStream; remove them for batch reads."
            )
        if self._opts.stream_options_present or self._opts.batch_change_feed_options_present:
            from cosmosdb_datasource.changefeed_batch import CosmosChangeFeedBatchReader

            self._reject_unused("change_feed")
            return CosmosChangeFeedBatchReader(self._opts, schema)
        if self._opts.item_ids:
            self._reject_unused("items")
        elif self._opts.partition_key is not None:
            self._reject_unused("logical_partition")
        else:
            self._reject_unused("documents")
        pushdown = self._opts.filter_pushdown
        if pushdown == "true" and _compat.filter_pushdown_supported() is False:
            raise CosmosConfigError(
                "filter_pushdown=true: this runtime does not push filters to Python data sources; PySpark 4.1 "
                "and later and Databricks Runtime 17.3 LTS and later do. Use filter_pushdown=auto or false, and "
                "write the filter into 'query' to have the service apply it."
            )
        # A reader with pushFilters fails planning while spark.sql.python.filterPushdown.enabled is false
        # (DATA_SOURCE_PUSHDOWN_DISABLED), so "auto" returns it only to Spark's pushdown planner, which
        # runs only when the conf is true. Pushdown is opt-in because Spark 4.1 to 4.3 cache the read
        # planned with pushed filters on the loaded relation and reuse it for its other scans (README,
        # "Filter pushdown").
        if pushdown == "true" or (pushdown == AUTO and filter_pushdown_planner()):
            from cosmosdb_datasource.pushdown import CosmosPushdownBatchReader

            return CosmosPushdownBatchReader(self._opts, schema)
        from cosmosdb_datasource.batch import CosmosBatchReader

        return CosmosBatchReader(self._opts, schema)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        # On Databricks the streaming runner passes the engine's StructType; normalize it first so
        # the comparison below and the reader's type checks use this PySpark's classes.
        schema = local_struct_type(schema)
        if self._opts.batch_change_feed_options_present:
            raise CosmosConfigError(
                f"Option(s) {', '.join(self._opts.batch_change_feed_options_present)} only apply to change-feed batch "
                "reads (spark.read); a stream keeps its position in its checkpoint."
            )
        if self._opts.snapshot_stream:
            from cosmosdb_datasource.snapshot_stream import CosmosSnapshotStreamReader

            self._reject_unused("snapshot_stream")
            return CosmosSnapshotStreamReader(self._opts, schema)
        if not self._opts.stream_options_present and schema == batch_schema(self._opts.raw_json_column):
            raise CosmosConfigError(
                "spark.readStream.format('cosmosdb') needs the change-feed schema: set option "
                "change_feed_mode (LatestVersion or AllVersionsAndDeletes), or supply an explicit .schema(...); "
                "or set snapshot_stream=true for one complete snapshot per micro-batch. "
                f"(Change-feed options: {', '.join(STREAM_ONLY_OPTIONS)}.)"
            )
        from cosmosdb_datasource.stream import CosmosStreamReader

        self._reject_unused("change_feed")
        return CosmosStreamReader(self._opts, schema)

    def _reject_unused(self, read: str) -> None:
        error = unused_options(self._opts, read)
        if error is not None:
            raise error

    def __repr__(self) -> str:
        return f"CosmosDBDataSource({self._opts!r})"
