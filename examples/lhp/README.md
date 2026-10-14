# Example: Lakehouse Plumber project

A [Lakehouse Plumber](https://pypi.org/project/lakehouse-plumber/) (LHP) project that generates
three Lakeflow Spark Declarative Pipelines reading Azure Cosmos DB for NoSQL with the `cosmosdb`
data source, and a bundle that deploys them. The step-by-step guide is
[docs/LHP-GUIDE.md](../../docs/LHP-GUIDE.md).

| Pipeline | Reads | Bronze | Silver |
|---|---|---|---|
| `lhp_orders_avad` | `orders_container`, AllVersionsAndDeletes with `initial_snapshot` | `orders_changes_avad` | `orders_avad`: SCD1 with deletes |
| `lhp_orders_latest` | `orders_container`, LatestVersion from `Beginning` | `orders_changes_latest` | `orders_latest`: SCD1, deletes not captured |
| `lhp_customers_scd2` | a full batch read of `customers_container` per update | none | `customers_scd2`: SCD2 |

The project validates and generates with LHP 0.9.2; the generated pipelines have not been run on
Databricks.

```bash
cp substitutions/dev.yaml.example substitutions/dev.yaml   # replace the <...> placeholders
export DO_NOT_TRACK=1                                      # LHP telemetry
uvx --from lakehouse-plumber==0.9.2 lhp validate --env dev --pipeline-config config/pipeline_config.yaml
uvx --from lakehouse-plumber==0.9.2 lhp generate --env dev --pipeline-config config/pipeline_config.yaml
databricks bundle deploy
databricks bundle run lhp_orders_avad_pipeline
```

`generated/`, `resources/lhp/` and `substitutions/*.yaml` are gitignored: they hold the resolved
environment values.
