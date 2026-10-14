"""Reconcile Unity Catalog tables with the Cosmos DB container they are built from.

Reads ``(pk, id, _ts, _etag)`` of every document with the ``cosmosdb`` data source and
full-outer-joins it on ``(pk, id)`` with the current rows of each table:

    missing         in the container, not in the table
    extra           in the table, not in the container
    stale           in both, but ``_ts`` differs, or both have an ``_etag`` and it differs
    duplicate_keys  keys with more than one current row in the table

``_etag`` comes from an ``_etag`` column, else from ``body`` (``$._etag``); it changes on every
write, so it also catches two versions written within the same second.

Kinds: ``scd1`` (every row is current), ``scd2`` (current rows have ``__END_AT IS NULL``),
``scd1_latest`` (a LatestVersion target: hard deletes never reach it, so extra rows are only
reported). Everything else must be 0.

The container read is not a point-in-time snapshot. The job compares the container's document
count and max ``_ts`` before and after the read with what the read returned; if they differ, the
verdict is INCONCLUSIVE. Run it while nothing writes to the container and after the pipeline has
caught up. One row per table is appended to ``--results_table``; the run fails unless every table
passes.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KINDS = ("scd1", "scd2", "scd1_latest")
SAMPLE = 5

# The job runs this file directly, so src/ (the parent directory) is not on sys.path yet.
sys.path.insert(0, str(Path(sys.argv[0]).resolve().parent.parent))

from common.cosmos import ParamConf, cosmos_options, register_cosmosdb  # noqa: E402


def container_stats(spark: SparkSession, options: dict[str, str]) -> tuple[int, int | None]:
    row = (
        spark.read.format("cosmosdb")
        .schema("n BIGINT, m BIGINT")
        .options(**options)
        .option("query", "SELECT COUNT(1) AS n, MAX(c._ts) AS m FROM c")
        .load()
        .agg(F.sum("n").alias("n"), F.max("m").alias("m"))
        .first()
    )
    return int(row["n"] or 0), (int(row["m"]) if row["m"] is not None else None)


def current_rows(spark: SparkSession, table: str, kind: str) -> DataFrame:
    df = spark.table(table)
    cols = set(df.columns)
    missing = {"pk", "id", "_ts"} - cols
    if missing:
        raise ValueError(f"{table} lacks the column(s) {sorted(missing)}")
    if kind == "scd2":
        if "__END_AT" not in cols:
            raise ValueError(f"{table} has no __END_AT column; use --kind scd1")
        df = df.where(F.col("__END_AT").isNull())
    if "_etag" in cols:
        etag = F.col("_etag")
    elif "body" in cols:
        etag = F.get_json_object(F.col("body"), "$._etag")
    else:
        etag = F.lit(None).cast("string")
    return df.select("pk", "id", F.col("_ts").cast("bigint").alias("_ts"), etag.alias("_etag"))


def compare(source: DataFrame, target: DataFrame) -> dict:
    cosmos = source.select(
        "pk", "id", F.col("_ts").alias("c_ts"), F.col("_etag").alias("c_etag"), F.lit(True).alias("in_c")
    )
    duplicate_keys = target.groupBy("pk", "id").count().where("count > 1").count()
    table = target.dropDuplicates(["pk", "id"]).select(
        "pk", "id", F.col("_ts").alias("t_ts"), F.col("_etag").alias("t_etag"), F.lit(True).alias("in_t")
    )
    joined = cosmos.join(table, ["pk", "id"], "full_outer")
    in_c, in_t = F.coalesce("in_c", F.lit(False)), F.coalesce("in_t", F.lit(False))
    ts_diff = ~F.col("c_ts").eqNullSafe(F.col("t_ts"))
    etag_diff = F.col("c_etag").isNotNull() & F.col("t_etag").isNotNull() & (F.col("c_etag") != F.col("t_etag"))
    status = (
        F.when(in_c & ~in_t, "missing").when(~in_c & in_t, "extra").when(ts_diff | etag_diff, "stale").otherwise("ok")
    )
    classified = joined.select("pk", "id", "c_ts", "t_ts", status.alias("status"), in_t.alias("in_table"))
    agg = classified.agg(
        F.sum(F.col("status").isin("ok", "missing", "stale").cast("long")).alias("cosmos_rows"),
        F.sum(F.col("in_table").cast("long")).alias("table_rows"),
        *(F.sum((F.col("status") == s).cast("long")).alias(s) for s in ("missing", "extra", "stale")),
    ).first()
    counts = {k: int(v or 0) for k, v in agg.asDict().items()}
    counts["duplicate_keys"] = int(duplicate_keys)
    counts["samples"] = {
        s: [
            r.asDict()
            for r in classified.where(F.col("status") == s).select("pk", "id", "c_ts", "t_ts").limit(SAMPLE).collect()
        ]
        for s in ("missing", "extra", "stale")
        if counts[s]
    }
    return counts


def verdict(kind: str, counts: dict, quiescent: bool) -> str:
    must_be_zero = ["missing", "stale", "duplicate_keys"] + ([] if kind == "scd1_latest" else ["extra"])
    if not quiescent:
        return "INCONCLUSIVE"
    return "PASS" if all(counts[k] == 0 for k in must_be_zero) else "FAIL"


RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_at TIMESTAMP, job_run_id STRING, container STRING, target_table STRING, kind STRING, verdict STRING,
  cosmos_rows BIGINT, table_rows BIGINT, missing BIGINT, extra BIGINT, stale BIGINT, duplicate_keys BIGINT,
  quiescent BOOLEAN, samples STRING
) COMMENT 'Reconcile results: one row per table and run'
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--container", required=True)
    parser.add_argument("--table", required=True, help="one or more tables, separated by commas or spaces")
    parser.add_argument("--kind", required=True, choices=KINDS)
    parser.add_argument("--results_table", required=True)
    parser.add_argument("--job_run_id", default="")
    parser.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE", help="cosmos.* configuration")
    args = parser.parse_args()

    values = dict(kv.split("=", 1) for kv in args.conf)
    values["cosmos.container"] = args.container
    spark = SparkSession.builder.getOrCreate()
    register_cosmosdb(spark)
    options = cosmos_options(ParamConf(values))

    before = container_stats(spark, options)
    source = (
        spark.read.format("cosmosdb").schema("id STRING, pk STRING, _ts BIGINT, _etag STRING").options(**options).load()
    ).cache()
    read = (source.count(), source.agg(F.max("_ts")).first()[0])
    after = container_stats(spark, options)
    quiescent = before == after == read
    print(f"reconcile: container {args.container}: before {before}, read {read}, after {after}", flush=True)

    spark.sql(RESULTS_DDL.format(table=args.results_table))
    failed = []
    for table in (t for t in re.split(r"[,\s]+", args.table) if t):
        counts = compare(source, current_rows(spark, table, args.kind))
        outcome = verdict(args.kind, counts, quiescent)
        row = {
            "run_at": datetime.now(timezone.utc),
            "job_run_id": args.job_run_id,
            "container": args.container,
            "target_table": table,
            "kind": args.kind,
            "verdict": outcome,
            **{k: counts[k] for k in ("cosmos_rows", "table_rows", "missing", "extra", "stale", "duplicate_keys")},
            "quiescent": quiescent,
            "samples": json.dumps(counts["samples"], default=str),
        }
        results = spark.table(args.results_table)
        spark.createDataFrame([row], schema=results.schema).write.mode("append").saveAsTable(args.results_table)
        print("reconcile: " + json.dumps({k: v for k, v in row.items() if k != "run_at"}, default=str), flush=True)
        if outcome != "PASS":
            failed.append(f"{table}: {outcome}")
    if failed:
        raise RuntimeError("reconcile did not pass: " + "; ".join(failed))


if __name__ == "__main__":
    main()
