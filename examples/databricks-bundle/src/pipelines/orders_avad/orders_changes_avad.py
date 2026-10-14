"""Bronze: one AllVersionsAndDeletes stream with an initial snapshot (append-only).

Micro-batch 0 is a snapshot of the whole container (``operation = 'snapshot'``, ``lsn = 0``), read
after the stream's start position P0 is in its offset log; every later micro-batch holds the
creates, replaces and deletes from P0 on. Snapshot and P0 belong to one checkpoint, so they are
reset together and a restart before micro-batch 0 commits reads the snapshot again from the same
P0.

AVAD history is kept only for the account's continuous-backup retention: an update after a longer
pause cannot resume, and the pipeline needs a full refresh. Never refresh this table without the
silver table (see README.md).
"""

from pyspark import pipelines as dp

from common.cosmos import conf, cosmos_options, fq, register_cosmosdb, stream_limit_options, with_ingest_time

register_cosmosdb(spark)

BRONZE = fq(spark, "bronze", conf(spark, "cosmos.bronze_table", "orders_changes_avad"))

dp.create_streaming_table(
    name=BRONZE,
    comment="Cosmos DB AllVersionsAndDeletes change feed, initial snapshot in micro-batch 0, with the ingest time",
)


@dp.append_flow(target=BRONZE, name="orders_avad_stream")
def orders_avad_stream():
    changes = (
        spark.readStream.format("cosmosdb")
        .options(**cosmos_options(spark), **stream_limit_options(spark))
        .option("change_feed_mode", "AllVersionsAndDeletes")
        .option("start_from", "Now")
        .option("initial_snapshot", "true")
        .load()
    )
    return with_ingest_time(changes)
