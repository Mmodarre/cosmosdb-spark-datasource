"""``cosmosdb`` Python data source for Azure Cosmos DB for NoSQL."""

from __future__ import annotations

from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader
from pyspark.sql.types import StructType

from cosmosdb_datasource._compat import local_struct_type
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.options import STREAM_ONLY_OPTIONS, parse_options
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
        if self._opts.snapshot_stream:
            return snapshot_stream_schema(raw)
        if self._opts.stream_options_present:
            return change_feed_schema(raw)
        return batch_schema(raw)

    def reader(self, schema: StructType) -> DataSourceReader:
        schema = local_struct_type(schema)
        stream_only = (
            *self._opts.stream_options_present,
            *(("snapshot_stream",) if self._opts.snapshot_stream else ()),
        )
        if stream_only:
            raise CosmosConfigError(
                f"Option(s) {', '.join(stream_only)} only apply to spark.readStream; remove them for batch reads."
            )
        from cosmosdb_datasource.batch import CosmosBatchReader

        return CosmosBatchReader(self._opts, schema)

    def streamReader(self, schema: StructType) -> DataSourceStreamReader:
        # On Databricks the streaming runner passes the engine's StructType; normalize it first so
        # the comparison below and the reader's type checks use this PySpark's classes.
        schema = local_struct_type(schema)
        if self._opts.snapshot_stream:
            from cosmosdb_datasource.snapshot_stream import CosmosSnapshotStreamReader

            return CosmosSnapshotStreamReader(self._opts, schema)
        if not self._opts.stream_options_present and schema == batch_schema(self._opts.raw_json_column):
            raise CosmosConfigError(
                "spark.readStream.format('cosmosdb') needs the change-feed schema: set option "
                "change_feed_mode (LatestVersion or AllVersionsAndDeletes), or supply an explicit .schema(...); "
                "or set snapshot_stream=true for one complete snapshot per micro-batch. "
                f"(Change-feed options: {', '.join(STREAM_ONLY_OPTIONS)}.)"
            )
        from cosmosdb_datasource.stream import CosmosStreamReader

        return CosmosStreamReader(self._opts, schema)

    def __repr__(self) -> str:
        return f"CosmosDBDataSource({self._opts!r})"
