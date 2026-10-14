"""Lakehouse Plumber entry module for the cosmosdb Python data source.

The implementation comes from the installed cosmosdb-spark-datasource package (see
config/pipeline_config.yaml): the reader classes run in Python worker processes, which import them
from the package, so they cannot be copied in as source.

LHP copies this file into the generated pipeline, registers the class, and takes the format name
from the string literal returned by name(). Keep name() exactly in this form: no return
annotation, no docstring and no other statement before the return; otherwise LHP 0.9.2 falls back
to the class name.
"""

from cosmosdb_datasource import CosmosDBDataSource


class CosmosDB(CosmosDBDataSource):
    @classmethod
    def name(cls):
        return "cosmosdb"
