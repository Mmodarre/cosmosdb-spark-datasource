"""Silver: latest state per document (SCD Type 1) from the LatestVersion bronze table.

Ordered by ``struct(lsn, lsn_seq)``, the order of the change feed. Deletes never arrive in this mode,
so deleted documents stay in the table.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.cosmos import conf, fq

BRONZE = fq(spark, "bronze", conf(spark, "cosmos.bronze_table", "orders_changes_latest"))
SILVER = fq(spark, "silver", conf(spark, "cosmos.silver_table", "orders_latest"))


@dp.temporary_view(name="orders_changes_latest_stream")
def orders_changes_latest_stream():
    return spark.readStream.table(BRONZE)


dp.create_streaming_table(
    name=SILVER,
    comment="Latest version of each document (SCD1 from the LatestVersion change feed; deletes not captured)",
)

dp.create_auto_cdc_flow(
    target=SILVER,
    source="orders_changes_latest_stream",
    keys=["pk", "id"],
    sequence_by=F.struct("lsn", "lsn_seq"),
    stored_as_scd_type=1,
    except_column_list=["operation", "lsn_seq", "crts", "ttl_expired", "feed_range", "_ingested_at"],
)
