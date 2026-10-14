"""Fail with guidance on Spark runtimes that lack the Python data source API this package needs."""

from __future__ import annotations

MINIMUM = (
    "PySpark 4.0 or later; on Databricks, DBR 15.4 LTS or later (classic compute) or serverless "
    "environment version 2 or later"
)


def require_data_source_api() -> None:
    """Raise ``ImportError`` naming the supported runtimes unless ``DataSourceStreamReader`` can be imported.

    Databricks backported the batch data source API to DBR 14.3 (Spark 3.5.0) without the streaming
    reader, so checking for ``pyspark.sql.datasource`` alone is not enough.
    """
    try:
        import pyspark
    except ImportError as exc:
        raise ImportError(
            f"cosmosdb-spark-datasource needs PySpark, which the Spark runtime provides ({MINIMUM})."
        ) from exc
    try:
        from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceStreamReader  # noqa: F401
    except ImportError as exc:
        found = getattr(pyspark, "__version__", "unknown")
        raise ImportError(
            f"cosmosdb-spark-datasource needs the Python data source API with streaming readers "
            f"(pyspark.sql.datasource.DataSourceStreamReader), which this runtime's PySpark {found} does "
            f"not have. Supported: {MINIMUM}."
        ) from exc
