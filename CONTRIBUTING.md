# Contributing

Bug reports, fixes and improvements are welcome. For anything larger than a small fix, open an
issue first to agree on the approach. Security issues: see [SECURITY.md](SECURITY.md).

## Development setup

The project uses [uv](https://docs.astral.sh/uv/) for environments, dependencies and builds.

```bash
git clone https://github.com/Mmodarre/cosmosdb-spark-datasource.git
cd cosmosdb-spark-datasource
uv sync                      # .venv with the package (editable) and the dev dependency group
uvx pre-commit install       # optional: ruff on every commit
```

The `dev` group contains PySpark, pyarrow, azure-identity, pytest, pytest-cov, mypy and ruff.
PySpark is not a runtime dependency of the package (Spark runtimes provide it).

`uv.lock` is committed. After changing dependencies in `pyproject.toml`, run `uv lock` and commit
the lockfile; CI fails if it is out of date.

## Checks

```bash
uv run ruff check            # lint
uv run ruff format           # format (CI runs `ruff format --check`)
uv run mypy                  # type check of src/
uv run python -m pytest      # unit tests; integration tests are deselected by default
uv build                     # sdist and wheel in dist/
```

CI ([`ci.yml`](.github/workflows/ci.yml)) runs the same checks on every pull request, the tests on
Python 3.10 to 3.13 with PySpark 4.0, 4.1 and 4.2, with the lowest and the highest azure-cosmos
version allowed by `pyproject.toml`, and enforces a coverage floor (`fail_under` in
`pyproject.toml`).

## Tests

| Location | What it covers | Needs |
|---|---|---|
| `tests/test_*.py` (default run) | options, auth, EPK arithmetic, schema conversion, offsets, the readers, initial snapshot, Databricks engine compatibility | nothing; uses `tests/fake_cosmos.py` |
| `tests/test_spark_local.py` | batch and AvailableNow stream through a local SparkSession, against the fake | Java 17 or later; skipped otherwise (CI fails if skipped) |
| `tests/test_integration.py` (`-m integration`) | the same code paths against a real endpoint | an emulator or an Azure Cosmos DB account |

`tests/harness.py` replays the call sequence of PySpark's Python data source workers without a JVM
(pickling, JSON offsets, restarts, Arrow conversion of the rows). `tests/fake_cosmos.py` is an
in-memory service with real EPK hashing, per-partition LSN logs, both change feed modes, partition
splits, replica lag, session tokens and throttling. New behaviour should come with a test at that
level; add an integration test when it depends on how the service behaves.

To run one PySpark or azure-cosmos version locally, layer it over the locked environment:

```bash
uv run --with "pyspark==4.0.*" python -m pytest
uv run --with "azure-cosmos==4.14.0" python -m pytest
```

## Integration tests

The integration tests create their own containers (`it_<random>`) in a test database, seed them,
and delete them afterwards. They are configured with environment variables:

| Variable | Meaning |
|---|---|
| `COSMOS_ENDPOINT`, `COSMOS_KEY` | endpoint and account key (required) |
| `COSMOS_DATABASE` | test database, created if missing (default `cosmosdb_datasource_it`) |
| `COSMOS_EMULATOR` | `vnext` or `legacy` for the emulators below; skips what they cannot do |
| `COSMOS_UNSUPPORTED` | comma-separated capability markers to skip on an account, e.g. `requires_avad` |
| `COSMOS_MULTI_PARTITION_THROUGHPUT` | RU/s for the multi-partition test containers (12000 or more on an account; it is billed while the tests run) |
| `COSMOS_AAD=1` | also run the `auth_type=default` test (`DefaultAzureCredential` with a data-plane role) |

Tests that need a capability that not every endpoint has carry a marker:

| Marker | Capability | vNext emulator | Legacy emulator |
|---|---|---|---|
| `requires_avad` | AllVersionsAndDeletes change feed | no | no |
| `requires_epk_ranges` | EPK sub-range filtering of queries and change feed | no | yes |
| `requires_multi_partition` | several physical partitions per container | no | yes |
| `requires_session_tokens` | enforced session tokens (Session consistency) | no | yes |
| `requires_aad` | Entra ID authentication | no | no |

### Emulators

The [Linux vNext emulator](https://learn.microsoft.com/azure/cosmos-db/emulator-linux) runs on
x64 and ARM64 (including Apple silicon). It ignores AllVersionsAndDeletes (it returns LatestVersion
items), ignores EPK sub-range filters, has one physical partition per container, does not enforce
session tokens and does not check the key:

```bash
docker run --detach --name cosmos-vnext -p 8081:8081 -p 8080:8080 \
  -e PROTOCOL=https mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest
until curl -fs http://localhost:8080/ready > /dev/null; do sleep 2; done

COSMOS_ENDPOINT=https://localhost:8081/ COSMOS_KEY="$(openssl rand -base64 64 | tr -d '\n')" \
  COSMOS_EMULATOR=vnext uv run python -m pytest -m integration
```

The legacy Linux emulator (x64 only) supports EPK sub-ranges, several partitions and session
tokens, but rejects AllVersionsAndDeletes. Start it with a key of your choice:

```bash
KEY="$(openssl rand -base64 64 | tr -d '\n')"
docker run --detach --name cosmos-legacy -p 8081:8081 -p 10250-10255:10250-10255 \
  -e AZURE_COSMOS_EMULATOR_KEY="$KEY" -e AZURE_COSMOS_EMULATOR_PARTITION_COUNT=12 \
  -e AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1 \
  mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest
until curl -kfs https://localhost:8081/_explorer/emulator.pem > /dev/null; do sleep 3; done

COSMOS_ENDPOINT=https://localhost:8081/ COSMOS_KEY="$KEY" COSMOS_EMULATOR=legacy \
  COSMOS_MULTI_PARTITION_THROUGHPUT=12000 uv run python -m pytest -m integration
```

azure-cosmos does not verify TLS certificates for `localhost` endpoints, so no certificate import
is needed.

### Azure Cosmos DB account

Use a dedicated account or at least a dedicated database: the tests create and delete containers.
The AllVersionsAndDeletes tests need continuous backup with the AllVersionsAndDeletes change feed
enabled; set `COSMOS_UNSUPPORTED=requires_avad` otherwise. The session-token test needs Session
consistency.

```bash
COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/ COSMOS_KEY=<key> \
  uv run python -m pytest -m integration
```

### In CI

[`integration.yml`](.github/workflows/integration.yml) runs both emulators (pinned images) on
pull requests, nightly and on demand, and fails if a test is skipped for any reason other than a
capability the emulator lacks. The account job runs nightly and on demand when the repository has
the secrets `COSMOS_ENDPOINT` and `COSMOS_KEY`; the repository variables `COSMOS_DATABASE`,
`COSMOS_MULTI_PARTITION_THROUGHPUT` and `COSMOS_UNSUPPORTED` configure it.

## Pull requests

- Keep a pull request to one change, with tests and documentation.
- Add an entry under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
- Comments explain why, not what; docstrings are for public API and non-obvious behaviour.
- All CI checks must pass before merging.

## Releases

See [RELEASING.md](RELEASING.md).

## License

Contributions are licensed under the [Apache License 2.0](LICENSE), the license of the project.
