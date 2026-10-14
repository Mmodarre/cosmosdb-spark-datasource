"""Helpers shared by the pipelines and the reconcile job.

Pipeline configuration keys (set in ``resources/*.pipeline.yml`` from the bundle variables):

    cosmos.catalog, cosmos.bronze_schema, cosmos.silver_schema   target locations
    cosmos.database, cosmos.container                             source
    cosmos.secret_scope, cosmos.secret_key.endpoint               secret scope and the endpoint's key
    cosmos.auth_type                                              key | service_principal | service_credential | default
    cosmos.secret_key.account_key                                 key: account key
    cosmos.tenant_id, cosmos.secret_key.client_id,
    cosmos.secret_key.client_secret                               service_principal
    cosmos.service_credential                                     service_credential: a token minted from it
    cosmos.partitions_per_feed_range, cosmos.priority             optional data source options
    cosmos.max_items_per_trigger                                  optional; "0" means unlimited

The values "", "none" and "default" mean "not set". ``src/`` is the pipelines' ``root_path``, so
this module is importable from pipeline source files; it is not in the pipeline ``libraries``
globs, so it is not run as a pipeline source itself. Jobs pass the same keys as parameters and wrap
them in :class:`ParamConf`.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_UNSET = ("", "none", "default")
_READ_OPTIONS = ("partitions_per_feed_range", "priority")


class ParamConf:
    """``spark.conf``-like view of job parameters, so jobs can use the helpers below."""

    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self.conf = self

    def get(self, key: str, default: str | None = None) -> str | None:
        return self._values.get(key, default)


def conf(spark: Any, key: str, default: str | None = None) -> str:
    value = spark.conf.get(key, default)
    if value is None or value == "":
        raise ValueError(f"Configuration '{key}' is required (set it in the pipeline or job resource).")
    return value


def optional_conf(spark: Any, key: str) -> str | None:
    value = (spark.conf.get(key, "") or "").strip()
    return None if value.lower() in _UNSET else value


def fq(spark: Any, layer: str, table: str) -> str:
    """Fully qualified name of a table in the bronze or silver schema."""
    return f"{conf(spark, 'cosmos.catalog')}.{conf(spark, f'cosmos.{layer}_schema')}.{table}"


def register_cosmosdb(spark: Any) -> None:
    from cosmosdb_datasource import CosmosDBDataSource

    spark.dataSource.register(CosmosDBDataSource)


def _auth_options(spark: Any, scope: str, endpoint: str) -> dict[str, str]:
    from urllib.parse import urlsplit

    from databricks.sdk.runtime import dbutils

    auth_type = conf(spark, "cosmos.auth_type", "key").strip().lower()
    if auth_type == "key":
        return {
            "auth_type": "key",
            "account_key": dbutils.secrets.get(scope, conf(spark, "cosmos.secret_key.account_key")),
        }
    if auth_type == "service_principal":
        tenant_id = optional_conf(spark, "cosmos.tenant_id")
        if tenant_id is None:
            raise ValueError("auth_type=service_principal needs the bundle variable tenant_id.")
        return {
            "auth_type": "service_principal",
            "tenant_id": tenant_id,
            "client_id": dbutils.secrets.get(scope, conf(spark, "cosmos.secret_key.client_id")),
            "client_secret": dbutils.secrets.get(scope, conf(spark, "cosmos.secret_key.client_secret")),
        }
    if auth_type == "service_credential":
        # The data source cannot resolve a service credential where Spark plans its reads, so the
        # token is minted here, on the driver. It is not refreshed: an update must end within its
        # lifetime (about one hour).
        name = optional_conf(spark, "cosmos.service_credential")
        if name is None:
            raise ValueError("auth_type=service_credential needs the bundle variable service_credential.")
        token = dbutils.credentials.getServiceCredentialsProvider(name).get_token(
            f"https://{urlsplit(endpoint).hostname}/.default"
        )
        return {
            "auth_type": "access_token",
            "access_token": token.token,
            "access_token_expires_on": str(token.expires_on),
        }
    if auth_type == "default":
        return {"auth_type": "default"}
    raise ValueError(f"auth_type must be key, service_principal, service_credential or default, got {auth_type!r}.")


def cosmos_options(spark: Any) -> dict[str, str]:
    """Options for ``spark.read[Stream].format("cosmosdb")``: connection, credentials and sizing."""
    from databricks.sdk.runtime import dbutils

    scope = conf(spark, "cosmos.secret_scope")
    endpoint = dbutils.secrets.get(scope, conf(spark, "cosmos.secret_key.endpoint"))
    options = {
        "endpoint": endpoint,
        "database": conf(spark, "cosmos.database"),
        "container": conf(spark, "cosmos.container"),
        **_auth_options(spark, scope, endpoint),
    }
    for name in _READ_OPTIONS:
        value = optional_conf(spark, f"cosmos.{name}")
        if value is not None:
            options[name] = value
    return options


def stream_limit_options(spark: Any) -> dict[str, str]:
    cap = int(conf(spark, "cosmos.max_items_per_trigger", "0"))
    return {"max_items_per_trigger": str(cap)} if cap > 0 else {}


def with_ingest_time(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp())


def source_rank() -> Any:
    """0 for initial-snapshot rows, 1 for change rows: every change outranks every snapshot row."""
    return F.when(F.col("operation") == "snapshot", F.lit(0)).otherwise(F.lit(1))
