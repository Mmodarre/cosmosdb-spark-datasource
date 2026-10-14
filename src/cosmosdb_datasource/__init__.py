"""PySpark Python data source for Azure Cosmos DB for NoSQL (short name ``cosmosdb``)."""

from cosmosdb_datasource._version import __version__
from cosmosdb_datasource.datasource import CosmosDBDataSource
from cosmosdb_datasource.errors import (
    CosmosAuthError,
    CosmosChangeFeedError,
    CosmosConfigError,
    CosmosConversionError,
    CosmosDataSourceError,
    CosmosOffsetError,
)

__all__ = [
    "CosmosAuthError",
    "CosmosChangeFeedError",
    "CosmosConfigError",
    "CosmosConversionError",
    "CosmosDBDataSource",
    "CosmosDataSourceError",
    "CosmosOffsetError",
    "__version__",
]
