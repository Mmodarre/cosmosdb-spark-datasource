"""Silver: current state per document (SCD Type 1) with hard deletes applied.

Ordered by ``struct(_source_rank, _ts, lsn)``. ``_source_rank`` is 0 for snapshot rows and 1 for
change rows, so any change outranks the snapshot copy of the same document whatever the
timestamps. A document changed after P0 has all of those changes in the feed; the snapshot only
matters for documents with no change after P0. For deletes the data source sets ``_ts`` to the
delete's ``crts``.

Bronze is append-only and holds the snapshot before any change, so rebuilding this table from
bronze gives the same result.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.cosmos import conf, fq, source_rank

BRONZE = fq(spark, "bronze", conf(spark, "cosmos.bronze_table", "orders_changes_avad"))
SILVER = fq(spark, "silver", conf(spark, "cosmos.silver_table", "orders_avad"))


@dp.temporary_view(name="orders_changes_avad_ranked")
def orders_changes_avad_ranked():
    return spark.readStream.table(BRONZE).withColumn("_source_rank", source_rank())


dp.create_streaming_table(
    name=SILVER,
    comment="Current state of each document (SCD1, AllVersionsAndDeletes with an initial snapshot, deletes applied)",
)

dp.create_auto_cdc_flow(
    target=SILVER,
    source="orders_changes_avad_ranked",
    keys=["pk", "id"],
    sequence_by=F.struct("_source_rank", "_ts", "lsn"),
    apply_as_deletes=F.expr("operation = 'delete'"),
    stored_as_scd_type=1,
    except_column_list=["_source_rank", "crts", "ttl_expired", "feed_range", "_ingested_at"],
)
