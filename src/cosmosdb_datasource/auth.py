"""Credential handling.

Token-based modes mint a short-lived AAD access token on the driver while planning (batch:
``partitions()``, streaming: every micro-batch) and ship it inside each ``InputPartition``, so
executors normally make no AAD calls and use no long-lived secret. Executors fall back to building
the credential themselves only when the shipped token is about to expire (long batch scans).

Note: Spark pickles the data source object (and therefore its options) into the task closure
(``pyspark/sql/worker/plan_data_source_read.py``, ``write_read_func_and_partitions``), so secrets
passed as options are present in executor memory regardless; they are never logged or rendered
in ``repr``/error messages by this package.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlsplit

from cosmosdb_datasource.errors import CosmosAuthError, redact
from cosmosdb_datasource.options import (
    AUTH_ACCESS_TOKEN,
    AUTH_DEFAULT,
    AUTH_KEY,
    AUTH_SERVICE_CREDENTIAL,
    AUTH_SERVICE_PRINCIPAL,
    AuthOptions,
)

logger = logging.getLogger("cosmosdb_datasource")

COSMOS_FALLBACK_SCOPE = "https://cosmos.azure.com/.default"
# Switch to the executor-side fallback this long before the shipped token expires. Must exceed
# azure-core's 300 s refresh window (azure/core/pipeline/policies/_authentication.py).
REFRESH_MARGIN_SECONDS = 600
# Re-mint on the driver when the cached token has less than this left.
DRIVER_REUSE_MIN_SECONDS = 1200


class SecretToken:
    """Bearer token that never renders its value (``InputPartition.__repr__`` prints attributes)."""

    __slots__ = ("_token", "expires_on")

    def __init__(self, token: str, expires_on: int) -> None:
        self._token = token
        self.expires_on = int(expires_on)

    @property
    def value(self) -> str:
        return self._token

    def remaining(self, now: float | None = None) -> float:
        return self.expires_on - (time.time() if now is None else now)

    def __repr__(self) -> str:
        return f"SecretToken(<redacted>, expires_on={self.expires_on})"

    __str__ = __repr__

    def __getstate__(self) -> tuple[str, int]:
        return self._token, self.expires_on

    def __setstate__(self, state: tuple[str, int]) -> None:
        self._token, self.expires_on = state


def account_scope(endpoint: str) -> str:
    """Same scope the SDK requests (``azure/cosmos/_base.py`` ``create_scope_from_url``)."""
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.hostname}/.default"


def _identity_module() -> Any:
    try:
        import azure.identity as identity
    except ImportError:
        raise CosmosAuthError(
            "azure-identity is required for this auth_type. Install the package with the identity extra: "
            "pip install 'cosmosdb-spark-datasource[identity]'."
        ) from None
    return identity


def _service_credential_provider(name: str) -> Any:
    """Resolve a Unity Catalog service credential to an azure TokenCredential.

    Tries, in order: ``databricks.service_credentials`` (documented for UDF/executor contexts),
    ``pyspark.dbutils.DBUtils`` with the active session, and ``databricks.sdk.runtime.dbutils``.
    """
    attempts: list[str] = []
    try:
        import databricks.service_credentials as service_credentials  # type: ignore[import-not-found]

        return service_credentials.getServiceCredentialsProvider(name)
    except Exception as exc:  # noqa: BLE001
        attempts.append(f"databricks.service_credentials: {type(exc).__name__}")
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark).credentials.getServiceCredentialsProvider(name)
        attempts.append("pyspark.dbutils: no active SparkSession")
    except Exception as exc:  # noqa: BLE001
        attempts.append(f"pyspark.dbutils: {type(exc).__name__}")
    try:
        from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]

        return dbutils.credentials.getServiceCredentialsProvider(name)
    except Exception as exc:  # noqa: BLE001
        attempts.append(f"databricks.sdk.runtime.dbutils: {type(exc).__name__}")
    raise CosmosAuthError(
        f"Unity Catalog service credential {name!r} is not available in this Python process "
        f"({'; '.join(attempts)}). Service credentials only work on Databricks compute with access to the "
        "credential. Alternative: mint a token in the notebook with "
        "dbutils.credentials.getServiceCredentialsProvider(name).get_token(scope) and pass it with "
        "auth_type=access_token."
    )


def build_token_credential(auth: AuthOptions) -> Any:
    """Long-lived azure TokenCredential for the auth type (None for key / access_token)."""
    if auth.auth_type == AUTH_SERVICE_PRINCIPAL:
        identity = _identity_module()
        return identity.ClientSecretCredential(
            tenant_id=auth.tenant_id, client_id=auth.client_id, client_secret=auth.client_secret
        )
    if auth.auth_type == AUTH_DEFAULT:
        identity = _identity_module()
        return identity.DefaultAzureCredential(exclude_interactive_browser_credential=True)
    if auth.auth_type == AUTH_SERVICE_CREDENTIAL:
        assert auth.service_credential is not None
        return _service_credential_provider(auth.service_credential)
    return None


def _jwt_expiry(token: str) -> int | None:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        return int(exp) if exp is not None else None
    except Exception:  # noqa: BLE001 - opaque token
        return None


def get_token(credential: Any, endpoint: str, secrets: tuple[str, ...] = ()) -> SecretToken:
    """Mint a token for the account scope, retrying with the generic Cosmos scope on AADSTS500011."""
    scopes = (account_scope(endpoint), COSMOS_FALLBACK_SCOPE)
    last_exc: Exception | None = None
    for scope in scopes:
        try:
            token = credential.get_token(scope)
            return SecretToken(token.token, int(token.expires_on))
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if "AADSTS500011" not in str(exc):
                break
    raise CosmosAuthError(f"Could not obtain an access token for {endpoint}: {redact(last_exc, secrets)}") from None


class _DriverTokenCache:
    """Reuse minted tokens across micro-batches within one driver-side Python process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tokens: dict[tuple[str, ...], SecretToken] = {}
        self._credentials: dict[tuple[str, ...], Any] = {}

    def token(
        self, key: tuple[str, ...], factory: Callable[[], Any], endpoint: str, secrets: tuple[str, ...]
    ) -> SecretToken:
        with self._lock:
            cached = self._tokens.get(key)
            if cached is not None and cached.remaining() > DRIVER_REUSE_MIN_SECONDS:
                return cached
            credential = self._credentials.get(key)
            if credential is None:
                credential = factory()
                self._credentials[key] = credential
        minted = get_token(credential, endpoint, secrets)
        with self._lock:
            self._tokens[key] = minted
        return minted


_DRIVER_TOKENS = _DriverTokenCache()


def identity_key(auth: AuthOptions, endpoint: str) -> tuple[str, ...]:
    """Non-secret identity of the principal, used for caching (never contains secret values)."""
    return (
        endpoint,
        auth.auth_type,
        auth.tenant_id or "",
        auth.client_id or "",
        auth.service_credential or "",
    )


def mint_driver_token(auth: AuthOptions, endpoint: str) -> SecretToken | None:
    """Token to embed into input partitions (None for account-key auth)."""
    if auth.auth_type == AUTH_KEY:
        return None
    if auth.auth_type == AUTH_ACCESS_TOKEN:
        assert auth.access_token is not None
        expires_on = auth.access_token_expires_on or _jwt_expiry(auth.access_token)
        if expires_on is None:
            logger.warning("access_token has no expiry (set access_token_expires_on); assuming it is valid for 1 hour.")
            expires_on = int(time.time()) + 3600
        token = SecretToken(auth.access_token, expires_on)
        if token.remaining() <= 0:
            raise CosmosAuthError(
                f"The supplied access_token expired at {expires_on} (epoch seconds); mint a new token."
            )
        return token
    key = identity_key(auth, endpoint)
    if auth.client_secret:  # a rotated secret must not keep using a credential built from the old one
        key = (*key, hashlib.sha256(auth.client_secret.encode()).hexdigest())
    return _DRIVER_TOKENS.token(key, lambda: build_token_credential(auth), endpoint, auth.secrets())


class PartitionCredential:
    """azure TokenCredential used on executors.

    Serves the token minted on the driver while it is fresh; afterwards builds the configured
    credential locally (service_principal, default, service_credential). For access_token there
    is no fallback and an expired token surfaces as a clear error.
    """

    def __init__(self, auth: AuthOptions, endpoint: str, token: SecretToken | None) -> None:
        self._auth = auth
        self._endpoint = endpoint
        self._token = token
        self._fallback: Any = None
        self._lock = threading.Lock()

    def update(self, token: SecretToken | None) -> None:
        with self._lock:
            if token is not None and (self._token is None or token.expires_on > self._token.expires_on):
                self._token = token

    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        from azure.core.credentials import AccessToken

        with self._lock:
            token = self._token
        if token is not None and token.remaining() > REFRESH_MARGIN_SECONDS:
            return AccessToken(token.value, token.expires_on)
        if self._auth.auth_type != AUTH_ACCESS_TOKEN:
            with self._lock:
                if self._fallback is None:
                    logger.info(
                        "Shipped access token near expiry; building %s credential locally.", self._auth.auth_type
                    )
                    self._fallback = build_token_credential(self._auth)
                fallback = self._fallback
            try:
                fresh = fallback.get_token(*scopes, **kwargs)
            except Exception as exc:  # noqa: BLE001
                if token is not None and token.remaining() > 0:
                    logger.warning(
                        "Fallback credential failed (%s); using shipped token until it expires.", type(exc).__name__
                    )
                    return AccessToken(token.value, token.expires_on)
                raise CosmosAuthError(
                    f"Could not refresh the Cosmos DB access token on this worker: {redact(exc, self._auth.secrets())}"
                ) from None
            self.update(SecretToken(fresh.token, int(fresh.expires_on)))
            return fresh
        if token is not None and token.remaining() > 0:
            return AccessToken(token.value, token.expires_on)
        raise CosmosAuthError(
            "The access_token supplied to the cosmosdb source has expired. Long-running reads need "
            "auth_type=service_principal, service_credential or default so workers can refresh tokens."
        )

    def __repr__(self) -> str:
        return f"PartitionCredential(auth_type={self._auth.auth_type!r})"
