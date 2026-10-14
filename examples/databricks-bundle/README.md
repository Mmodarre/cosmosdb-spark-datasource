# Example: Declarative Automation Bundle

A [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) (formerly
Databricks Asset Bundles) with three triggered, serverless Lakeflow Spark Declarative Pipelines
(SDP) that read Azure Cosmos DB for NoSQL with the `cosmosdb` data source, and a job that
reconciles a target table with its container. Each pipeline implements one pattern from
[docs/MODES.md](../../docs/MODES.md).

| Pipeline | Reads | Bronze (`bronze_schema`) | Silver (`silver_schema`) |
|---|---|---|---|
| `cosmos_customers_scd2` | a full batch read of `customers_container` on every update ([mode 1](../../docs/MODES.md#1-batch-read)) | none (temporary view) | `customers_scd2`: SCD2 with `create_auto_cdc_from_snapshot_flow`; a missing document closes its row |
| `cosmos_orders_latest` | the change feed of `orders_container`, LatestVersion from `Beginning` ([mode 2](../../docs/MODES.md#2-latestversion-change-feed)) | `orders_changes_latest` | `orders_latest`: SCD1 with `create_auto_cdc_flow`; deletes are not captured |
| `cosmos_orders_avad` | the change feed of `orders_container`, AllVersionsAndDeletes with `initial_snapshot` ([mode 4](../../docs/MODES.md#4-avad--initial-snapshot)) | `orders_changes_avad` | `orders_avad`: SCD1 with `apply_as_deletes` |

The job `cosmos_reconcile` compares a container with a target table: missing, extra and stale
keys.

These patterns ran on Databricks serverless SDP (DBR 18.3) in an earlier form of this bundle; this
generic version has not been deployed as is.

## Layout

```
databricks.yml                      bundle, variables, dev target
resources/*.pipeline.yml            one pipeline per file (serverless, triggered, CURRENT channel)
resources/reconcile.job.yml         reconcile job (serverless, environment version 4)
src/common/cosmos.py                shared helpers: configuration, credentials, read options
src/pipelines/customers_scd2/       mode 1
src/pipelines/orders_latest/        mode 2: bronze and silver in separate files
src/pipelines/orders_avad/          mode 4: bronze and silver in separate files
src/ops/reconcile.py                reconcile a container with one or more tables
```

`src/` is the pipelines' `root_path`, so the pipeline source files import `common.cosmos`. Only
the files under `src/pipelines/<pipeline>/` are pipeline sources.

## Prerequisites

- The Databricks CLI with a profile for the workspace. The bundle sets no workspace host: the
  profile (`--profile` or `DATABRICKS_CONFIG_PROFILE`) selects the workspace.
- A Unity Catalog catalog with the schemas `bronze_schema`, `silver_schema` and `ops_schema`
  (defaults `cosmosdb_bronze`, `cosmosdb_silver`, `cosmosdb_ops`), and the right to create tables
  in them.
- A secret scope with the account endpoint and the credentials (see [Authentication](#authentication)).
- For `cosmos_orders_avad`: an account with continuous backup and the all versions and deletes
  change feed enabled.

## Variables

Required, without defaults:

| Variable | Meaning |
|---|---|
| `catalog` | Unity Catalog catalog of all tables |
| `cosmos_database` | Cosmos DB database |
| `secret_scope` | secret scope with the endpoint and credentials |

Optional (see `databricks.yml` for all of them): the schema names, `customers_container` and
`orders_container` (defaults `customers` and `orders`), `datasource_package` (default
`cosmosdb-spark-datasource~=0.3.1`; a wheel path in a Unity Catalog volume also works),
`auth_type` and the secret key names, `partitions_per_feed_range`, `priority` and
`max_items_per_trigger`.

Set them with `--var`, with `BUNDLE_VAR_<name>` environment variables, or in the CLI's per-target
override file, which lives under the gitignored `.databricks/` directory:

```bash
mkdir -p .databricks/bundle/dev
cat > .databricks/bundle/dev/variable-overrides.json <<'JSON'
{
  "catalog": "<catalog>",
  "cosmos_database": "<database>",
  "secret_scope": "<scope>"
}
JSON
```

## Authentication

The endpoint is always read from the secret scope (key `secret_key_endpoint`, default
`endpoint`). `auth_type` selects the credentials:

| `auth_type` | Needs |
|---|---|
| `key` (default) | the account key in the scope (key `secret_key_account_key`, default `account-key`) |
| `service_principal` | `tenant_id`, and the client id and secret in the scope (`client-id`, `client-secret`) |
| `service_credential` | `service_credential`, the name of a Unity Catalog service credential. The pipeline source mints a token from it on the driver and passes it as `access_token`; it is not refreshed, so an update must end within about one hour. |
| `default` | an identity that `DefaultAzureCredential` finds on the compute; on Databricks it finds none (see the [README](../../README.md#authentication)) |

Entra ID identities need a Cosmos DB data-plane role (for example *Cosmos DB Built-in Data
Reader*). `service_principal` and a token minted from a service credential on the driver were run
on Databricks serverless and classic compute; minting the token in a pipeline source file has not
been verified.

```bash
databricks secrets create-scope <scope>
databricks secrets put-secret <scope> endpoint --string-value "https://<account>.documents.azure.com:443/"
databricks secrets put-secret <scope> account-key   # prompts for the value
```

## Deploy and run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run cosmos_customers_scd2
databricks bundle run cosmos_orders_latest
databricks bundle run cosmos_orders_avad
```

The first update of `cosmos_orders_avad` reads the initial snapshot and then the changes made
while it was read: the AvailableNow target, captured before the stream's start position is
resolved, is moved once after the snapshot. The first update of `cosmos_orders_latest` reads every
document through the change feed; set `max_items_per_trigger` to spread a large container over
several micro-batches.

Schedule the pipelines with a job (a `pipeline_task` per pipeline) once they work. Run the AVAD
pipeline more often than the account's continuous-backup retention (7 or 30 days): a stream that
stops for longer cannot resume.

## Full refresh

- Never refresh a bronze table alone. The silver flow that streams from it fails at the next update
  with `DELTA_SOURCE_IGNORE_DELETE` (observed on DBR 18.3). Refresh the whole pipeline instead.
- Refreshing only a silver table is safe: it is rebuilt from the append-only bronze table.
- `cosmos_orders_avad`: never reset only the stream checkpoint; a full refresh of the pipeline takes
  a new start position and a new snapshot together.
- `cosmos_customers_scd2`: a refresh restarts the SCD2 history from the current state.

See [docs/MODES.md](../../docs/MODES.md#full-refresh-rules).

## Reconcile job

`cosmos_reconcile` reads `(pk, id, _ts, _etag)` of every document, compares it with the current
rows of one or more tables, and appends one row per table to
`<catalog>.<ops_schema>.reconcile_results`. The run fails unless every table passes.

```bash
databricks bundle run cosmos_reconcile \
  --params container=orders,table=<catalog>.cosmosdb_silver.orders_avad,kind=scd1
databricks bundle run cosmos_reconcile \
  --params container=orders,table=<catalog>.cosmosdb_silver.orders_latest,kind=scd1_latest
databricks bundle run cosmos_reconcile \
  --params container=customers,table=<catalog>.cosmosdb_silver.customers_scd2,kind=scd2
```

| `kind` | Current rows | Must be 0 |
|---|---|---|
| `scd1` | all rows | missing, extra, stale, duplicate keys |
| `scd2` | rows with `__END_AT IS NULL` | missing, extra, stale, duplicate keys |
| `scd1_latest` | all rows (LatestVersion target) | missing, stale, duplicate keys; extra rows are deletes the feed does not deliver, and are only reported |

The container read is not a point-in-time snapshot. Run the job while nothing writes to the
container and after the pipeline has caught up; the job compares the document count and the
highest `_ts` before and after its read, and reports INCONCLUSIVE if they changed.
