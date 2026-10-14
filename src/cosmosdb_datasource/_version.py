"""The version of the installed distribution; ``pyproject.toml`` is its only source."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("cosmosdb-spark-datasource")
except PackageNotFoundError:  # a source tree on sys.path that was never installed
    __version__ = "0+unknown"
