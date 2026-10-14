"""Process-wide CosmosClient cache.

``CosmosClient`` construction is expensive (account metadata reads, connection pool, routing map
cache), and Spark reuses Python workers across tasks, so one client per (endpoint, principal,
client settings) is kept per process and closed at interpreter exit. Token-based clients are keyed
by principal identity, never by token value: a fresh token shipped with a later partition is
pushed into the cached credential instead of creating a new client.
"""

from __future__ import annotations

import atexit
import hashlib
import importlib
import logging
import threading
from dataclasses import dataclass
from typing import Any

from cosmosdb_datasource._version import __version__
from cosmosdb_datasource.auth import PartitionCredential, SecretToken, identity_key
from cosmosdb_datasource.backend import SdkBackend
from cosmosdb_datasource.errors import CosmosConfigError, CosmosDataSourceError, redact
from cosmosdb_datasource.options import AUTH_ACCESS_TOKEN, AUTH_KEY, CosmosOptions

logger = logging.getLogger("cosmosdb_datasource")


@dataclass
class _Entry:
    client: Any
    credential: PartitionCredential | None


_LOCK = threading.Lock()
_CLIENTS: dict[tuple[Any, ...], _Entry] = {}
_BACKENDS: dict[tuple[Any, ...], Any] = {}


def _client_key(opts: CosmosOptions) -> tuple[Any, ...]:
    auth = opts.auth
    if auth.auth_type == AUTH_KEY:
        assert auth.account_key is not None
        principal: tuple[str, ...] = (
            opts.endpoint,
            AUTH_KEY,
            hashlib.sha256(auth.account_key.encode()).hexdigest(),
        )
    elif auth.auth_type == AUTH_ACCESS_TOKEN:
        # Pre-minted tokens may belong to different principals: never share a client across them.
        assert auth.access_token is not None
        principal = (*identity_key(auth, opts.endpoint), hashlib.sha256(auth.access_token.encode()).hexdigest())
    else:
        principal = identity_key(auth, opts.endpoint)
    settings = (
        opts.preferred_regions,
        opts.consistency_level,
        opts.connection_timeout_seconds,
        opts.read_timeout_seconds,
        opts.throttle_max_retries,
        opts.throttle_max_wait_seconds,
    )
    return principal + settings


def _client_kwargs(opts: CosmosOptions) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "user_agent_suffix": f"cosmosdb-spark-datasource/{__version__}",
        "retry_throttle_total": opts.throttle_max_retries,
        "retry_throttle_backoff_max": opts.throttle_max_wait_seconds,
    }
    if opts.preferred_regions:
        kwargs["preferred_locations"] = list(opts.preferred_regions)
    if opts.consistency_level:
        kwargs["consistency_level"] = opts.consistency_level
    if opts.connection_timeout_seconds:
        kwargs["connection_timeout"] = opts.connection_timeout_seconds
    if opts.read_timeout_seconds:
        kwargs["read_timeout"] = opts.read_timeout_seconds
    return kwargs


def _create_client(opts: CosmosOptions, token: SecretToken | None) -> _Entry:
    from azure.cosmos import CosmosClient

    credential: PartitionCredential | None = None
    if opts.auth.auth_type == AUTH_KEY:
        sdk_credential: Any = opts.auth.account_key
    else:
        credential = PartitionCredential(opts.auth, opts.endpoint, token)
        sdk_credential = credential
    try:
        client = CosmosClient(opts.endpoint, credential=sdk_credential, **_client_kwargs(opts))
    except Exception as exc:  # noqa: BLE001
        raise CosmosDataSourceError(
            f"Could not create a Cosmos DB client for {opts.endpoint}: {redact(exc, opts.secrets())}"
        ) from None
    return _Entry(client=client, credential=credential)


def _load_test_backend(spec: str, opts: CosmosOptions) -> Any:
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise CosmosConfigError("Internal test backend must be given as 'module:callable'.")
    factory = getattr(importlib.import_module(module_name), attr)
    return factory(opts)


def get_backend(opts: CosmosOptions, token: SecretToken | None = None) -> Any:
    """Backend for the configured container, sharing one client per process."""
    if opts.test_backend:
        return _load_test_backend(opts.test_backend, opts)
    key = _client_key(opts)
    with _LOCK:
        entry = _CLIENTS.get(key)
        if entry is None:
            entry = _create_client(opts, token)
            _CLIENTS[key] = entry
        elif entry.credential is not None:
            entry.credential.update(token)
        backend_key = (*key, opts.database, opts.container, opts.priority)
        backend = _BACKENDS.get(backend_key)
        if backend is None:
            container = entry.client.get_database_client(opts.database).get_container_client(opts.container)
            backend = SdkBackend(container, priority=opts.priority)
            _BACKENDS[backend_key] = backend
    return backend


def _close_client(cosmos_client: Any) -> None:
    # CosmosClient.close() only exists in newer azure-cosmos releases (absent in 4.14.0);
    # the context-manager exit is available in every supported version.
    close = getattr(cosmos_client, "close", None)
    if callable(close):
        close()
    else:
        cosmos_client.__exit__(None, None, None)


def close_all() -> None:
    """Close every cached client (registered with atexit; also used by stream ``stop()``)."""
    with _LOCK:
        entries = list(_CLIENTS.values())
        _CLIENTS.clear()
        _BACKENDS.clear()
    for entry in entries:
        try:
            _close_client(entry.client)
        except Exception as exc:  # noqa: BLE001 - best effort at shutdown
            logger.debug("Ignoring error while closing a Cosmos client: %s", type(exc).__name__)


atexit.register(close_all)
