"""Process-wide CosmosClient cache.

``CosmosClient`` construction is expensive (account metadata reads, connection pool, routing map
cache), and Spark reuses Python workers across tasks, so one client per (endpoint, principal,
client settings) is kept per process and closed at interpreter exit. Token-based clients are keyed
by principal identity, never by token value: a fresh token shipped with a later partition is
pushed into the cached credential instead of creating a new client. A service principal's key also
holds a hash of its secret or certificate, so a rotated secret gets a new client whose credential
can refresh tokens. A stream reader holds a reference to its client and releases it when it stops;
the client is closed when no stream reader of the process holds it any more.
"""

from __future__ import annotations

import atexit
import hashlib
import importlib
import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from cosmosdb_datasource._version import __version__
from cosmosdb_datasource.auth import PartitionCredential, SecretToken, identity_key
from cosmosdb_datasource.backend import SdkBackend
from cosmosdb_datasource.diagnostics import SlowRequestLog
from cosmosdb_datasource.errors import CosmosConfigError, CosmosDataSourceError, redact
from cosmosdb_datasource.options import AUTH_ACCESS_TOKEN, AUTH_KEY, CosmosOptions

logger = logging.getLogger("cosmosdb_datasource")


@dataclass
class _Entry:
    client: Any
    credential: PartitionCredential | None
    # Serializes requests on ``client`` across its backends (see ``SdkBackend``).
    lock: threading.RLock = field(default_factory=threading.RLock)


_LOCK = threading.Lock()
_CLIENTS: dict[tuple[Any, ...], _Entry] = {}
_BACKENDS: dict[tuple[Any, ...], Any] = {}
_HOLDERS: dict[tuple[Any, ...], int] = {}  # client key -> stream readers that hold the client


def _digest(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _client_key(opts: CosmosOptions) -> tuple[Any, ...]:
    auth = opts.auth
    if auth.auth_type == AUTH_KEY:
        assert auth.account_key is not None
        principal: tuple[str, ...] = (opts.endpoint, AUTH_KEY, _digest(auth.account_key))
    elif auth.auth_type == AUTH_ACCESS_TOKEN:
        # Pre-minted tokens may belong to different principals: never share a client across them.
        assert auth.access_token is not None
        principal = (*identity_key(auth, opts.endpoint), _digest(auth.access_token))
    else:
        principal = identity_key(auth, opts.endpoint)
        secret = auth.client_secret or auth.client_certificate
        if secret:  # the client's credential builds its fallback from the secret it was created with
            principal = (*principal, _digest(secret))
    settings = (
        opts.preferred_regions,
        opts.consistency_level,
        opts.connection_timeout_seconds,
        opts.read_timeout_seconds,
        opts.throttle_max_retries,
        opts.throttle_max_wait_seconds,
        opts.throughput_bucket,
        opts.tls_verify,
        opts.tls_ca_bundle,
    )
    return principal + settings


def _connection_policy(opts: CosmosOptions) -> Any:
    """Return a connection policy with the timeouts and throttle retry settings of ``opts``.

    The settings are set on the policy rather than passed as keywords. The ``retry_throttle_total``
    and ``retry_throttle_backoff_max`` keywords cannot express 0: ``cosmos_client._build_connection_policy``
    keeps ``value or default``, so 0 would mean 9 retries. Before azure-cosmos 4.16 the ``read_timeout``
    keyword is not read at all. A policy passed as ``connection_policy`` keeps its ``RetryOptions``,
    whose class the public ``ConnectionPolicy.RetryOptions`` setter takes, and its timeouts.
    """
    from azure.cosmos._retry_options import RetryOptions
    from azure.cosmos.documents import ConnectionPolicy, SSLConfiguration

    policy = ConnectionPolicy()
    policy.RetryOptions = RetryOptions(
        max_retry_attempt_count=opts.throttle_max_retries,
        max_wait_time_in_seconds=opts.throttle_max_wait_seconds,  # type: ignore[arg-type]  # used as seconds * 1000
    )
    # Seconds, passed to the transport's timeouts, which take fractions (the SDK annotates them as int).
    if opts.connection_timeout_seconds:
        policy.RequestTimeout = opts.connection_timeout_seconds  # type: ignore[assignment]
    if opts.read_timeout_seconds:
        policy.ReadTimeout = opts.read_timeout_seconds
    if opts.tls_ca_bundle:
        # Passed to the transport as connection_verify=<path> (azure-cosmos _synchronized_request).
        ssl = SSLConfiguration()
        ssl.SSLCaCerts = opts.tls_ca_bundle
        policy.SSLConfiguration = ssl
    return policy


def _client_kwargs(opts: CosmosOptions) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "user_agent_suffix": f"cosmosdb-spark-datasource/{__version__}",
        "connection_policy": _connection_policy(opts),
    }
    if not opts.tls_verify:
        # The SDK resets ConnectionPolicy.DisableSSLVerification from this keyword.
        kwargs["connection_verify"] = False
    if opts.preferred_regions:
        kwargs["preferred_locations"] = list(opts.preferred_regions)
    if opts.consistency_level:
        kwargs["consistency_level"] = opts.consistency_level
    if opts.throughput_bucket:
        kwargs["throughput_bucket"] = opts.throughput_bucket  # x-ms-cosmos-throughput-bucket on every request
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
    """Return the backend for the configured container, sharing one client per process."""
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
        backend_key = (
            *key,
            opts.database,
            opts.container,
            opts.priority,
            opts.prefetch_pages,
            opts.max_integrated_cache_staleness_ms,
            opts.slow_request_ms,
            opts.slow_request_log_per_minute,
        )
        backend = _BACKENDS.get(backend_key)
        if backend is None:
            container = entry.client.get_database_client(opts.database).get_container_client(opts.container)
            slow = (
                SlowRequestLog(opts.slow_request_ms, opts.slow_request_log_per_minute) if opts.slow_request_ms else None
            )
            backend = SdkBackend(
                container,
                priority=opts.priority,
                prefetch_pages=opts.prefetch_pages,
                cache_staleness_ms=opts.max_integrated_cache_staleness_ms,
                slow_requests=slow,
                sdk_lock=entry.lock,
            )
            _BACKENDS[backend_key] = backend
    return backend


def hold(opts: CosmosOptions) -> tuple[Any, ...] | None:
    """Record that a stream reader uses the client of ``opts``; return the key to :func:`release` it with."""
    if opts.test_backend:
        return None
    key = _client_key(opts)
    with _LOCK:
        _HOLDERS[key] = _HOLDERS.get(key, 0) + 1
    return key


def release(key: tuple[Any, ...] | None) -> None:
    """Release a :func:`hold`; close the client once no stream reader of this process holds it."""
    if key is None:
        return
    with _LOCK:
        holders = _HOLDERS.get(key, 0) - 1
        if holders > 0:
            _HOLDERS[key] = holders
            return
        _HOLDERS.pop(key, None)
        entry = _CLIENTS.pop(key, None)
        for backend_key in [k for k in _BACKENDS if k[: len(key)] == key]:
            del _BACKENDS[backend_key]
    if entry is not None:
        _close_quietly(entry.client)


def _close_quietly(cosmos_client: Any) -> None:
    try:
        _close_client(cosmos_client)
    except Exception as exc:  # noqa: BLE001 - best effort
        logger.debug("Ignoring error while closing a Cosmos client: %s", type(exc).__name__)


def _close_client(cosmos_client: Any) -> None:
    # CosmosClient.close() only exists in newer azure-cosmos releases (absent in 4.14.0);
    # the context-manager exit is available in every supported version.
    close = getattr(cosmos_client, "close", None)
    if callable(close):
        close()
    else:
        cosmos_client.__exit__(None, None, None)


def close_all() -> None:
    """Close every cached client (registered with atexit)."""
    with _LOCK:
        entries = list(_CLIENTS.values())
        _CLIENTS.clear()
        _BACKENDS.clear()
        _HOLDERS.clear()
    for entry in entries:
        _close_quietly(entry.client)


atexit.register(close_all)
