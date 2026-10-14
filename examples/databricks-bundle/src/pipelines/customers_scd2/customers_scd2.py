"""Batch snapshot of the customers container to SCD Type 2 (deletes close the current row).

Every pipeline update reads the whole container and ``create_auto_cdc_from_snapshot_flow`` diffs
it with the previous snapshot by ``(pk, id)``: a new key opens a current row, changed columns
close it and open a new one, a missing key closes it. The snapshot view holds document data only
(no ingest timestamps), otherwise every update would create a new version of every row.

A missing key closes the current row, so a read that returns too few documents (a wrong
container, a truncated source) looks like a mass delete. Check the container's document count
before the update if that is a risk.
"""

from pyspark import pipelines as dp

from common.cosmos import conf, cosmos_options, fq, register_cosmosdb

register_cosmosdb(spark)

TARGET = fq(spark, "silver", conf(spark, "cosmos.target_table", "customers_scd2"))


@dp.temporary_view(name="customers_snapshot", comment="Current documents of the container, one full read per update")
def customers_snapshot():
    return spark.read.format("cosmosdb").options(**cosmos_options(spark)).load()


dp.create_streaming_table(
    name=TARGET,
    comment="Cosmos DB container as SCD Type 2 (snapshot diff per pipeline update; document JSON in body)",
)

dp.create_auto_cdc_from_snapshot_flow(
    target=TARGET,
    source="customers_snapshot",
    keys=["pk", "id"],
    stored_as_scd_type=2,
)
