"""Bronze: the LatestVersion change feed from the beginning of the container (append-only).

The first update reads every live document through the change feed, later updates only what
changed since the checkpoint. Hard deletes are not in this feed.
"""

from pyspark import pipelines as dp

from common.cosmos import conf, cosmos_options, fq, register_cosmosdb, stream_limit_options, with_ingest_time

register_cosmosdb(spark)

BRONZE = fq(spark, "bronze", conf(spark, "cosmos.bronze_table", "orders_changes_latest"))


@dp.table(name=BRONZE, comment="Cosmos DB change feed (LatestVersion, from Beginning) with the ingest time")
def orders_changes_latest():
    changes = (
        spark.readStream.format("cosmosdb")
        .options(**cosmos_options(spark), **stream_limit_options(spark))
        .option("change_feed_mode", "LatestVersion")
        .option("start_from", "Beginning")
        .load()
    )
    return with_ingest_time(changes)
