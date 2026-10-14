"""PySpark Python data source for Azure Cosmos DB for NoSQL (short name ``cosmosdb``)."""

from cosmosdb_datasource._runtime import require_data_source_api
from cosmosdb_datasource._version import __version__

require_data_source_api()

from cosmosdb_datasource.batch_checkpoint import commit_batch_checkpoint  # noqa: E402 - after the runtime check
from cosmosdb_datasource.datasource import CosmosDBDataSource  # noqa: E402
from cosmosdb_datasource.errors import (  # noqa: E402
    CosmosAuthError,
    CosmosChangeFeedError,
    CosmosConfigError,
    CosmosConversionError,
    CosmosDataSourceError,
    CosmosOffsetError,
)
from cosmosdb_datasource.readmany import read_many  # noqa: E402

__all__ = [
    "CosmosAuthError",
    "CosmosChangeFeedError",
    "CosmosConfigError",
    "CosmosConversionError",
    "CosmosDBDataSource",
    "CosmosDataSourceError",
    "CosmosOffsetError",
    "__version__",
    "commit_batch_checkpoint",
    "read_many",
]
