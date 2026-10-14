# Using the data source with Lakehouse Plumber

[Lakehouse Plumber](https://pypi.org/project/lakehouse-plumber/) (LHP) generates Lakeflow Spark
Declarative Pipelines (SDP) from YAML flowgroups. This guide builds the example project in
[`examples/lhp/`](../examples/lhp/), which reads Azure Cosmos DB with the `cosmosdb` data source
through LHP's `custom_datasource` load.

**Status.** The example validates and generates with LHP 0.9.2 (`lhp validate --strict`,
`lhp generate`). The generated pipelines have not been run on Databricks; the same three patterns,
written by hand, are in [`examples/databricks-bundle/`](../examples/databricks-bundle/). Statements
about runtime behaviour that rest on the data source's own tests or on the hand-written pipelines
say so; anything else is marked *not verified*.

| Pipeline | Pattern | Read mode ([MODES.md](MODES.md)) |
|---|---|---|
| `lhp_orders_avad` | AVAD change feed with an initial snapshot to bronze, then SCD1 with deletes | 4 |
| `lhp_orders_latest` | LatestVersion change feed from `Beginning` to bronze, then SCD1 | 2 |
| `lhp_customers_scd2` | batch snapshot view to SCD2, in one pipeline | 1 |

## 1. Prerequisites

| What | Notes |
|---|---|
| Databricks CLI | a profile for the workspace; the bundle sets no host |
| uv | runs LHP with `uvx --from lakehouse-plumber==0.9.2 lhp ...` |
| Lakehouse Plumber | 0.9.2 (`required_lhp_version` in `lhp.yaml`) |
| Cosmos DB for NoSQL account | for `lhp_orders_avad`: continuous backup with the AllVersionsAndDeletes change feed, and no partition merge |
| Unity Catalog | a catalog with the schemas `cosmosdb_lhp_bronze`, `cosmosdb_lhp_silver` and `cosmosdb_lhp_ops` (names set in the substitutions) |
| Secret scope | keys `endpoint` and `account-key` |

LHP sends telemetry by default; set `DO_NOT_TRACK=1` or run `lhp telemetry off` to disable it.

## 2. The data source package

The reader classes run in Python worker processes, which import them (and `azure-cosmos`) from
installed packages. LHP's `custom_datasource` copies only one entry file into the pipeline, so the
implementation must come from the pipeline environment: `config/pipeline_config.yaml` adds
`${datasource_package}` (default `cosmosdb-spark-datasource~=0.2.0`; a wheel path in a Unity
Catalog volume works too) and `azure-identity~=1.19` to `environment.dependencies`.

Write version ranges with `~=`: on DBR 18.3, SDP failed to install `azure-identity>=1.19,<2` while
`azure-identity~=1.19` installed. When installing a wheel from a volume, give every changed build
a new version: serverless may reuse a cached environment for an unchanged dependency list.

`packaging: source | wheel` in `pipeline_config.yaml` is a different setting: it controls how LHP
ships the generated pipeline code. The example keeps the default, `source`.

## 3. Project layout and substitutions

| File | Purpose |
|---|---|
| `lhp.yaml` | `required_lhp_version`, the `_ingested_at` operational-metadata column (opt-in per action), one event-log table per pipeline in `${ops_schema}` |
| `substitutions/dev.yaml.example` | placeholders for `dev`; copy to `substitutions/dev.yaml` |
| `config/pipeline_config.yaml` | serverless, CURRENT channel, triggered; `environment.dependencies`; default catalog and schema per pipeline |
| `presets/cosmos_connection.yaml` | entry module, endpoint, database and credentials of every cosmosdb load |
| `py_functions/cosmos_source.py` | the entry module (section 4) |
| `pipelines/<pipeline>/<flowgroup>.yaml` | one flowgroup per pipeline (section 5) |
| `databricks.yml` | bundle with `sync.include: generated/${bundle.target}/**` and the `dev` target |

```bash
cd examples/lhp
cp substitutions/dev.yaml.example substitutions/dev.yaml   # then replace the <...> placeholders
uvx --from lakehouse-plumber==0.9.2 lhp substitutions --env dev   # shows the resolved tokens
```

`substitutions/dev.yaml.example`:

```yaml
dev:
  env: dev
  catalog: <catalog>
  bronze_schema: cosmosdb_lhp_bronze
  silver_schema: cosmosdb_lhp_silver
  ops_schema: cosmosdb_lhp_ops
  datasource_package: cosmosdb-spark-datasource~=0.2.0
  cosmos_database: <database>
  customers_container: customers
  orders_container: orders
  partitions_per_feed_range: "1"

secrets:
  default_scope: <scope>
  scopes:
    cosmos: <scope>
```

The example's `.gitignore` keeps `substitutions/*.yaml` out of version control, because LHP writes
the resolved values (including the catalog) into `generated/` and `resources/lhp/`, and has no
local-override mechanism. In a project for one organisation, committing `substitutions/<env>.yaml`
is normal.

## 4. The entry module, and why `name()` must be literal

`py_functions/cosmos_source.py`:

```python
from cosmosdb_datasource import CosmosDBDataSource


class CosmosDB(CosmosDBDataSource):
    @classmethod
    def name(cls):
        return "cosmosdb"
```

For a `custom_datasource` load, LHP 0.9.2:

1. copies the file into `generated/<env>/<pipeline>/custom_python_functions/`;
2. emits `_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)`;
3. emits `try: spark.dataSource.register(CosmosDB) except Exception: pass`;
4. emits `spark.read[Stream].format("<name>")` with one `.option(...)` per option.

LHP takes `<name>` from the copied file with a regular expression for
`@classmethod def name(...): return "<literal>"`. It does not match a return annotation
(`def name(cls) -> str:`), a docstring or any statement before the `return`, or a computed value;
then it silently uses the class name as the format
([Mmodarre/Lakehouse_Plumber#314](https://github.com/Mmodarre/Lakehouse_Plumber/issues/314)).
Spark lower-cases data source names, so a class named `CosmosDB` still resolves to `cosmosdb`; a
class with another name would fail with `DATA_SOURCE_NOT_FOUND`. The package's own
`CosmosDBDataSource.name()` has a return annotation, which is why the entry module subclasses it.

LHP also substitutes tokens in copied Python files, so keep `${...}` and brace-enclosed names out
of `py_functions/`.

## 5. Flowgroups

Every flowgroup that reads Cosmos DB uses the preset `presets/cosmos_connection.yaml`, which is
merged into each `custom_datasource` source:

```yaml
name: cosmos_connection
version: "1.0"
defaults:
  load_actions:
    custom_datasource:
      module_path: py_functions/cosmos_source.py
      custom_datasource_class: CosmosDB
      options:
        endpoint: "${secret:cosmos/endpoint}"
        database: "${cosmos_database}"
        auth_type: key
        account_key: "${secret:cosmos/account-key}"
```

Quote option values: unquoted YAML booleans become Python `True`/`False`.

### `lhp_orders_avad`: AVAD with an initial snapshot to SCD1 with deletes

[`pipelines/lhp_orders_avad/orders_avad.yaml`](../examples/lhp/pipelines/lhp_orders_avad/orders_avad.yaml):

```yaml
actions:
  - name: load_orders_avad_stream
    type: load
    readMode: stream
    operational_metadata: ["_ingested_at"]
    source:
      type: custom_datasource
      options:
        container: "${orders_container}"
        change_feed_mode: AllVersionsAndDeletes
        start_from: Now
        initial_snapshot: "true"
        partitions_per_feed_range: "${partitions_per_feed_range}"
    target: v_orders_avad_stream

  - name: write_orders_changes_avad      # bronze, append-only
    type: write
    source: v_orders_avad_stream
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: orders_changes_avad

  - name: load_orders_changes_avad       # stream the bronze table
    type: load
    readMode: stream
    source:
      type: delta
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: orders_changes_avad
    target: v_orders_changes_avad

  - name: rank_orders_changes_avad
    type: transform
    transform_type: sql
    source: v_orders_changes_avad
    target: v_orders_changes_avad_ranked
    sql: |
      SELECT *, CASE WHEN operation = 'snapshot' THEN 0 ELSE 1 END AS _source_rank
      FROM stream(v_orders_changes_avad)

  - name: write_orders_avad              # silver, SCD1 with deletes
    type: write
    source: v_orders_changes_avad_ranked
    write_target:
      type: streaming_table
      mode: cdc
      catalog: "${catalog}"
      schema: "${silver_schema}"
      table: orders_avad
      cdc_config:
        keys: ["pk", "id"]
        sequence_by: ["_source_rank", "_ts", "lsn"]
        scd_type: 1
        apply_as_deletes: "operation = 'delete'"
        except_column_list: ["_source_rank", "crts", "ttl_expired", "feed_range", "_ingested_at"]
```

The generated code has `.option("initial_snapshot", "true")`, an append flow into bronze, the SQL
transform over `stream(v_orders_changes_avad)`, and
`dp.create_auto_cdc_flow(..., sequence_by=struct("_source_rank", "_ts", "lsn"), apply_as_deletes="operation = 'delete'")`.
Do not add a separate `once: true` snapshot flow instead of `initial_snapshot`: it would race the
stream ([MODES.md](MODES.md#why-not-a-separate-one-time-snapshot-flow-with-avad)).

### `lhp_orders_latest`: LatestVersion from `Beginning` to SCD1

The same shape without the snapshot and the rank: the load sets `change_feed_mode: LatestVersion`
and `start_from: Beginning`, and the CDC write uses `sequence_by: ["_ts", "lsn"]`, no
`apply_as_deletes`, and `operation` in `except_column_list`. Deletes are not in this feed. On a
large container add `max_items_per_trigger: "<LSN budget>"` to the load options, so the first
update commits in several micro-batches.

### `lhp_customers_scd2`: batch snapshot to SCD2

```yaml
actions:
  - name: load_customers_snapshot
    type: load
    readMode: batch
    source:
      type: custom_datasource
      options:
        container: "${customers_container}"
        partitions_per_feed_range: "${partitions_per_feed_range}"
    target: v_customers_snapshot

  - name: write_customers_scd2
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      catalog: "${catalog}"
      schema: "${silver_schema}"
      table: customers_scd2
      snapshot_cdc_config:
        source: v_customers_snapshot
        keys: ["pk", "id"]
        stored_as_scd_type: 2
```

The snapshot view must depend on the documents only: no `operational_metadata` here, or every
update creates a new version of every row. A read that returns too few documents closes every
missing row; pure LHP has no hook for a pre-update guard, so check the source with a job task if
that is a risk.

## 6. Secrets

- `${secret:<alias>/<key>}` in an option becomes `dbutils.secrets.get(scope="<scope>", key="<key>")`
  in the generated code. Aliases map to scopes per environment in `substitutions/<env>.yaml`
  (`secrets.scopes`).
- LHP validates scope names (letters, digits, `_`, `-`), so the `<scope>` placeholder fails
  `lhp validate` until it is replaced.
- No secret values are stored in the project or the generated code; they are read when the pipeline
  runs.
- The generated code uses the `dbutils` global that SDP provides to Python sources. The
  hand-written pipelines import it explicitly (`from databricks.sdk.runtime import dbutils`), which
  was observed to work on DBR 18.3; the global in generated code is *not verified*.
- Presets are resolved before substitutions, so the auth type cannot change per environment: use
  one auth type everywhere and vary only the values (see the comment in the preset for a service
  principal).

## 7. Generate, validate, deploy

From `examples/lhp`:

```bash
export DO_NOT_TRACK=1
uvx --from lakehouse-plumber==0.9.2 lhp validate --env dev --pipeline-config config/pipeline_config.yaml --strict
uvx --from lakehouse-plumber==0.9.2 lhp generate --env dev --pipeline-config config/pipeline_config.yaml
databricks bundle validate
databricks bundle deploy
databricks bundle run lhp_orders_avad_pipeline
```

`lhp generate` writes `generated/dev/<pipeline>/<flowgroup>.py` (plus `custom_python_functions/`)
and `resources/lhp/<pipeline>.pipeline.yml`. Both are gitignored; the bundle uploads `generated/`
because of `sync.include`. Pipeline resources are named `<pipeline>_pipeline`. `resources/lhp/`
holds the last generated environment, so regenerate before every deploy and before switching
targets.

## 8. Operations

| Pipeline | First update | Later updates |
|---|---|---|
| `lhp_orders_avad` | the snapshot only: AvailableNow captures its target before the start position is resolved, so changes made during the first update arrive with the second | AVAD changes since the checkpoint |
| `lhp_orders_latest` | every live document, through the change feed | changes since the checkpoint |
| `lhp_customers_scd2` | the initial SCD2 table (all rows current) | the diff with the previous update's snapshot |

These follow from the data source's AvailableNow behaviour and the hand-written pipelines; they
are *not verified* for the generated pipelines.

Full refresh:

- `lhp_orders_avad` and `lhp_orders_latest`: only refresh the whole pipeline
  (`databricks bundle run <pipeline>_pipeline --full-refresh-all`). After a refresh of only the
  bronze table, the silver flow fails with `DELTA_SOURCE_IGNORE_DELETE` (observed on DBR 18.3 with
  the hand-written pipeline). For `lhp_orders_avad`, never reset only the stream checkpoint either.
- Refreshing only a silver table is safe: it is rebuilt from the append-only bronze table.
- `lhp_customers_scd2`: a full refresh restarts the SCD2 history from the current state.

AVAD changes can only be read within the account's continuous-backup retention (7 or 30 days).
Schedule `lhp_orders_avad` well within it and alert on failures; a stale stream needs a full
refresh of the pipeline. How the service answers an expired continuation is *not verified*.

To compare a target table with its container, use the reconcile job of the
[bundle example](../examples/databricks-bundle/README.md#reconcile-job).

## 9. Sizing

| Knob | Where | Effect |
|---|---|---|
| `partitions_per_feed_range` | substitution, used in every load | EPK sub-splits per physical partition. `auto` targets about 1 GiB per task for batch reads and snapshots and means 1 for change-feed reads. |
| `max_items_per_trigger` | a change-feed load's options | LSN budget per micro-batch, not a row cap (bulk writes put many documents under one LSN). It does not apply to an initial snapshot. Serverless SDP runs several capped micro-batches per triggered update. |
| `page_size` | options | items per request (default 1000) |
| `priority: Low` | preset or options | with priority-based execution enabled on the account, OLTP requests win under throttling |
| `throttle_max_retries`, `throttle_max_wait_seconds` | options | SDK handling of HTTP 429 (defaults 9 and 60 s) |

Costs per read are in [MODES.md](MODES.md#costs).

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `DATA_SOURCE_NOT_FOUND: cosmosdb` (or the class name) | Registration failed and the generated `try ... except Exception: pass` hid the error ([#326](https://github.com/Mmodarre/Lakehouse_Plumber/issues/326)): register `CosmosDBDataSource` in a notebook with the same environment to see it. Or `name()` in the entry module is not a bare `return "cosmosdb"` and the class is not named `CosmosDB` (check `.format(...)` in the generated code). |
| `ModuleNotFoundError: cosmosdb_datasource` | The package is not in the pipeline environment: check `datasource_package`. |
| `ENVIRONMENT_PIP_INSTALL_ERROR` | A dependency string with `<` or `>`; use `~=`. |
| `Unresolved token(s) ...` | `substitutions/<env>.yaml` is missing (copy the `.example`), or a token is not defined in it. |
| `LHP-VAL-008 Secret validation failed: Invalid secret scope` | The scope name holds characters other than letters, digits, `_` and `-` (for example the `<scope>` placeholder). |
| Deployed pipelines point at the wrong catalog | `resources/lhp/` was generated for another environment; regenerate and deploy again. |

## 11. Known LHP limitations (0.9.2)

1. A bare `except` around the registration hides data source registration errors
   ([#326](https://github.com/Mmodarre/Lakehouse_Plumber/issues/326)).
2. The format name comes from a regular expression over `name()`, with a silent fallback to the
   class name ([#314](https://github.com/Mmodarre/Lakehouse_Plumber/issues/314)).
3. `module_path` is one local file, copied into the generated pipeline and pickled by value; a
   packaged implementation must come from the pipeline environment.
4. There are no local overrides for substitutions, and `catalog` is required on write targets and
   Delta sources, so environment values end up in `generated/` and `resources/lhp/`.
5. `resources/lhp/` reflects the last generated environment, not the bundle target.
6. Presets are resolved before substitutions, so a token cannot choose a preset.
7. Copied Python files are token-substituted (`${name}`, the deprecated bare `{name}` and
   `${secret:...}`).
8. `--pipeline-config` has no project default: pass it to every `validate` and `generate`.
9. Telemetry is on by default.
