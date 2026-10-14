"""Snapshot source function of lhp_customers_scd2_history (create_auto_cdc_from_snapshot_flow).

The bronze table holds complete snapshots appended by a snapshot_stream read; every row of one
snapshot carries the same version (snapshot_ts). SDP calls next_snapshot with the version it
processed last (None on the first call) until it returns None. The next version is the smallest
snapshot_ts greater than the last processed one.

Versions are compared as epoch milliseconds computed by Spark: SDP stores a timestamp version with
full precision (__START_AT) but hands it back to this function as a naive UTC datetime truncated to
milliseconds (observed on DBR 18.3), and the data source stamps snapshot_ts in whole milliseconds,
so the comparison is exact.

min_snapshot_ratio > 0 fails the update when the next snapshot holds fewer rows than that ratio
times the previous one: a short snapshot would close every missing row as a delete. To accept an
intended shrink, run one update with the ratio set to 0.

LHP copies this file into the generated pipeline and substitutes its tokens in it, so the file
contains no brace-enclosed names and no dollar-brace sequences.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
# A key should appear once per snapshot; if it appears more often, keep the newest copy, deterministically.
_TIEBREAK_COLUMNS = ("_ts", "_etag")


def _session() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = globals().get("spark")  # LHP also assigns the pipeline's session to this global
    if spark is None:
        raise RuntimeError("next_snapshot: no active SparkSession")
    return spark


def _one_per_key(df: DataFrame, keys: Sequence[str]) -> DataFrame:
    order = [F.col(c).desc_nulls_last() for c in _TIEBREAK_COLUMNS if c in df.columns]
    if not order:
        return df.dropDuplicates(list(keys))
    window = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("__rn", F.row_number().over(window)).where(F.col("__rn") == 1).drop("__rn")


def next_snapshot(
    latest_snapshot_version: Any,
    *,
    bronze_table: str,
    version_column: str = "snapshot_ts",
    keys: Sequence[str] = ("pk", "id"),
    columns: Sequence[str] | None = None,
    min_snapshot_ratio: float | str = 0.0,
) -> tuple[DataFrame, datetime] | None:
    spark = _session()
    bronze = spark.read.table(bronze_table)
    version = F.unix_micros(F.col(version_column))
    version_ms = F.unix_millis(F.col(version_column))

    previous = None
    candidates = bronze
    if latest_snapshot_version is not None:
        latest = latest_snapshot_version
        if isinstance(latest, datetime) and latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        previous_ms = spark.range(1).select(F.unix_millis(F.lit(latest).cast("timestamp")).alias("m")).first()["m"]
        candidates = bronze.where(version_ms > F.lit(previous_ms))
        previous = bronze.where(version_ms == F.lit(previous_ms)).agg(F.max(version).alias("m")).first()["m"]

    following = candidates.agg(F.min(version).alias("m")).first()["m"]
    if following is None:
        if latest_snapshot_version is None:
            raise RuntimeError(
                "next_snapshot: " + bronze_table + " holds no snapshot yet. Run lhp_customers_snapshots_bronze "
                "at least once before this pipeline."
            )
        return None

    ratio = float(min_snapshot_ratio or 0)
    if ratio > 0 and previous is not None:  # None: the last processed snapshot was pruned from bronze
        grouped = bronze.where(version.isin(previous, following)).groupBy(version.alias("m"))
        counts = dict((r["m"], r["n"]) for r in grouped.agg(F.count(F.lit(1)).alias("n")).collect())
        before, after = counts.get(previous, 0), counts.get(following, 0)
        if after < ratio * before:
            raise RuntimeError(
                "next_snapshot: the next snapshot holds "
                + str(after)
                + " rows, fewer than min_snapshot_ratio="
                + str(ratio)
                + " x the "
                + str(before)
                + " rows of the previous one, which would close at least "
                + str(before - after)
                + " rows as deletes. If the shrink is intended, run one update with "
                "min_snapshot_ratio set to 0."
            )

    snapshot = _one_per_key(bronze.where(version == F.lit(following)), keys)
    snapshot = snapshot.select(*columns) if columns else snapshot.drop(version_column)
    return snapshot, _EPOCH + timedelta(microseconds=int(following))
