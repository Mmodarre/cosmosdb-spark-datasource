"""Silver: current state per document (SCD Type 1) with hard deletes applied.

Ordered by ``struct(_source_rank, lsn, lsn_seq)``. ``_source_rank`` is 0 for snapshot rows and 1 for
change rows, so any change outranks the snapshot copy of the same document. A document changed after
P0 has all of those changes in the feed, in feed order: ``lsn``, then ``lsn_seq`` for versions that
share an LSN (a transactional batch, or a multi-region-write conflict's delete and create). The
snapshot only matters for documents with no change after P0.

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
    sequence_by=F.struct("_source_rank", "lsn", "lsn_seq"),
    apply_as_deletes=F.expr("operation = 'delete'"),
    stored_as_scd_type=1,
    except_column_list=["_source_rank", "lsn_seq", "crts", "ttl_expired", "feed_range", "_ingested_at"],
)
