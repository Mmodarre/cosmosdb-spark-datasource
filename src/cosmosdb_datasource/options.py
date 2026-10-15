"""Option parsing and validation.

Spark hands options to the data source as a case-insensitive mapping (keys are lower-cased by
``pyspark.sql.datasource.CaseInsensitiveDict``). Everything here is pure Python and cheap: the
data source and reader objects are constructed in several Python worker processes, so parsing must
not perform network calls.
"""

from __future__ import annotations

import base64
import binascii
import difflib
import ipaddress
import json
import logging
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from cosmosdb_datasource.errors import REDACTED, CosmosConfigError

logger = logging.getLogger("cosmosdb_datasource")

AUTH_KEY = "key"
AUTH_SERVICE_PRINCIPAL = "service_principal"
AUTH_ACCESS_TOKEN = "access_token"
AUTH_SERVICE_CREDENTIAL = "service_credential"
AUTH_DEFAULT = "default"
AUTH_TYPES = (
    AUTH_KEY,
    AUTH_SERVICE_PRINCIPAL,
    AUTH_ACCESS_TOKEN,
    AUTH_DEFAULT,
)

MODE_LATEST_VERSION = "LatestVersion"
MODE_ALL_VERSIONS_AND_DELETES = "AllVersionsAndDeletes"
_CHANGE_FEED_MODES = {
    "latestversion": MODE_LATEST_VERSION,
    "allversionsanddeletes": MODE_ALL_VERSIONS_AND_DELETES,
}

PARSE_PERMISSIVE = "PERMISSIVE"
PARSE_DROPMALFORMED = "DROPMALFORMED"
PARSE_FAILFAST = "FAILFAST"
_PARSE_MODES = (PARSE_PERMISSIVE, PARSE_DROPMALFORMED, PARSE_FAILFAST)

_CONSISTENCY_LEVELS = {
    "strong": "Strong",
    "boundedstaleness": "BoundedStaleness",
    "session": "Session",
    "consistentprefix": "ConsistentPrefix",
    "eventual": "Eventual",
}

DEFAULT_QUERY = "SELECT * FROM c"
DEFAULT_PAGE_SIZE = 5000
MAX_PREFETCH_PAGES = 4
MAX_THROUGHPUT_BUCKET = 5  # the service's throughput buckets are numbered 1 to 5
DEFAULT_RAW_COLUMN = "body"
DEFAULT_CORRUPT_COLUMN = "_corrupt_record"
DEFAULT_SAMPLING_SIZE = 1000
INFERENCE_OPTIONS = ("sampling_size", "sampling_query", "infer_system_properties", "infer_timestamp")
AUTO = "auto"

# Options that only make sense for readStream; their presence also selects the change-feed
# default schema (DataSource.schema() receives no batch/stream signal from Spark).
STREAM_ONLY_OPTIONS = (
    "change_feed_mode",
    "start_from",
    "max_items_per_trigger",
    "initial_snapshot",
    "initial_snapshot_delay_seconds",
)
DEFAULT_INITIAL_SNAPSHOT_DELAY_SECONDS = 5.0
# Change-feed options of batch reads only; they select the change-feed schema too.
CHANGE_FEED_BATCH_OPTIONS = ("end_at", "batch_checkpoint_location")
# Change-feed options that a batch read rejects: an initial snapshot belongs to a stream.
STREAM_SNAPSHOT_OPTIONS = ("initial_snapshot", "initial_snapshot_delay_seconds")

# Change-feed options that a snapshot stream (snapshot_stream=true) rejects: it reads the whole
# container in every micro-batch.
SNAPSHOT_STREAM_EXCLUSIVE_OPTIONS = (
    "change_feed_mode",
    "start_from",
    "initial_snapshot",
    "initial_snapshot_delay_seconds",
    "max_items_per_trigger",
    "change_feed_end_wait_seconds",
)

SECRET_OPTIONS = ("account_key", "client_secret", "access_token", "client_certificate", "client_certificate_password")

# Internal, undocumented test hook: "<module>:<callable>" returning a backend. Only honoured when
# the environment variable below is set, so it cannot be triggered by accident in production.
TEST_BACKEND_OPTION = "_test_backend"
TEST_HOOKS_ENV = "COSMOSDB_DATASOURCE_TEST_HOOKS"

AUTH_OPTIONS = (
    "auth_type",
    "account_key",
    "tenant_id",
    "client_id",
    "client_secret",
    "client_certificate",
    "client_certificate_password",
    "access_token",
    "access_token_expires_on",
    "service_credential",
)
# The options of the client and its requests, which apply to every read (read_many included).
CONNECTION_OPTIONS = (
    "endpoint",
    "database",
    "container",
    *AUTH_OPTIONS,
    "preferred_regions",
    "consistency_level",
    "connection_timeout_seconds",
    "read_timeout_seconds",
    "tls_verify",
    "tls_ca_bundle",
    "throttle_max_retries",
    "throttle_max_wait_seconds",
    "max_retries",
    "retry_max_backoff_seconds",
    "priority",
    "throughput_bucket",
    "slow_request_ms",
    "slow_request_log_per_minute",
)
CONVERSION_OPTIONS = ("mode", "raw_json_column", "corrupt_record_column")
READ_OPTIONS = (
    "query",
    "query_parameters",
    "page_size",
    "partitions_per_feed_range",
    "log_metrics",
    "change_feed_mode",
    "start_from",
    "max_items_per_trigger",
    "change_feed_end_wait_seconds",
    "initial_snapshot",
    "initial_snapshot_delay_seconds",
    "snapshot_stream",
    "snapshot_interval_seconds",
    "filter_pushdown",
    "prefetch_pages",
    "arrow_batches",
    "partition_key",
    "item_ids",
    "target_ru_per_second",
    "end_at",
    "batch_checkpoint_location",
    "max_integrated_cache_staleness_ms",
    "infer_schema",
    "sampling_size",
    "sampling_query",
    "infer_system_properties",
    "infer_timestamp",
)
# Spark adds "path" for .load(path) and "paths" for .load(path1, path2); the data source ignores them.
SPARK_PATH_OPTIONS = ("path", "paths")
PUBLIC_OPTIONS = frozenset({*CONNECTION_OPTIONS, *CONVERSION_OPTIONS, *READ_OPTIONS})
KNOWN_OPTIONS = frozenset({*PUBLIC_OPTIONS, *SPARK_PATH_OPTIONS, TEST_BACKEND_OPTION})

# Options that a kind of read accepts but does not use (see unused_options).
_QUERY_PAGE_OPTIONS = ("prefetch_pages", "max_integrated_cache_staleness_ms")
_UNUSED_BY = {
    "documents": ("change_feed_end_wait_seconds",),
    "logical_partition": ("change_feed_end_wait_seconds", "partitions_per_feed_range"),
    "items": (
        "change_feed_end_wait_seconds",
        "partitions_per_feed_range",
        "page_size",
        "filter_pushdown",
        *_QUERY_PAGE_OPTIONS,
    ),
    "change_feed": ("filter_pushdown", "arrow_batches"),
    "snapshot_stream": ("filter_pushdown", "arrow_batches"),
}
_WHY_UNUSED = {
    "documents": "only change-feed reads wait for a lagging replica",
    "logical_partition": "a logical partition is read as one task",
    "items": "item_ids are read with point reads, not with a query",
    "change_feed": "change-feed reads run no query (except an initial snapshot), and get no filters or Arrow batches",
    "snapshot_stream": "streams get no filters from Spark and no Arrow batches",
}


def _suggestion(name: str) -> str | None:
    def plain(text: str) -> str:
        return text.replace("_", "").replace("-", "")

    same = [known for known in sorted(PUBLIC_OPTIONS) if plain(known) == plain(name)]
    if same:
        return same[0]
    close = difflib.get_close_matches(name, sorted(PUBLIC_OPTIONS), n=1, cutoff=0.75)
    return close[0] if close else None


def _unknown(names: list[str]) -> CosmosConfigError:
    described = []
    for name in names:
        suggestion = _suggestion(name)
        described.append(f"{name!r} (did you mean {suggestion!r}?)" if suggestion else repr(name))
    return CosmosConfigError(
        f"Unknown cosmosdb option(s): {', '.join(described)}. Option names are case-insensitive; the README lists them."
    )


_ENDPOINT_HOST_RE = re.compile(r"^[A-Za-z0-9.\-]+$")


def _is_local_host(host: str) -> bool:
    """Loopback addresses, ``localhost`` and single-label names (an emulator container on a local network)."""
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return "." not in host


def _parse_endpoint(raw: str) -> str:
    parts = urlsplit(raw)
    host = parts.hostname or ""
    valid_host = bool(_ENDPOINT_HOST_RE.match(host)) or host == "::1"
    try:
        port_ok = parts.port is None or parts.port > 0
    except ValueError:
        port_ok = False
    if (
        parts.scheme not in ("https", "http")
        or not valid_host
        or not port_ok
        or parts.path not in ("", "/")
        or parts.query
        or parts.fragment
        or parts.username
    ):
        raise CosmosConfigError(
            f"Option 'endpoint' must look like https://<account>.documents.azure.com:443/, got {raw!r}."
        )
    if parts.scheme == "http" and not _is_local_host(host):
        raise CosmosConfigError(
            "Option 'endpoint' must use https://. http:// is accepted only for an emulator on this machine or a "
            f"local network (localhost, a loopback address or a host name without a domain), got {raw!r}."
        )
    return raw


def _normalize(options: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in (options or {}).items():
        if value is None:
            continue
        out[str(key).strip().lower()] = str(value)
    return out


def _get(opts: dict[str, str], name: str, default: str | None = None) -> str | None:
    value = opts.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value != "" else default


def _int(opts: dict[str, str], name: str, default: int | None, *, minimum: int | None = None) -> int | None:
    raw = _get(opts, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise CosmosConfigError(f"Option '{name}' must be an integer, got {raw!r}.") from None
    if minimum is not None and value < minimum:
        raise CosmosConfigError(f"Option '{name}' must be >= {minimum}, got {value}.")
    return value


def _float(opts: dict[str, str], name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = _get(opts, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise CosmosConfigError(f"Option '{name}' must be a number, got {raw!r}.") from None
    if value < minimum:
        raise CosmosConfigError(f"Option '{name}' must be >= {minimum}, got {value}.")
    return value


def _bool(opts: dict[str, str], name: str, default: bool) -> bool:
    raw = _get(opts, name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in ("true", "1", "yes"):
        return True
    if lowered in ("false", "0", "no"):
        return False
    raise CosmosConfigError(f"Option '{name}' must be true or false, got {raw!r}.")


def parse_timestamp_option(name: str, raw: str) -> datetime:
    """Parse an ISO-8601 timestamp option into an aware UTC datetime (naive input means UTC)."""
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        raise CosmosConfigError(
            f"Option '{name}' must be 'Beginning', 'Now' or an ISO-8601 timestamp "
            f"(e.g. 2026-01-31T00:00:00Z), got {raw!r}."
        ) from None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_expires_on(raw: str | None) -> int | None:
    if raw is None:
        return None
    text = raw.strip()
    try:
        return int(float(text))
    except ValueError:
        pass
    return int(parse_timestamp_option("access_token_expires_on", text).timestamp())


@dataclass(frozen=True)
class AuthOptions:
    auth_type: str
    tenant_id: str | None = None
    client_id: str | None = None
    access_token_expires_on: int | None = None
    # Secrets: excluded from repr/eq so they never end up in logs or error messages.
    account_key: str | None = field(default=None, repr=False, compare=False)
    client_secret: str | None = field(default=None, repr=False, compare=False)
    client_certificate: str | None = field(default=None, repr=False, compare=False)  # base64 PEM or PKCS#12
    client_certificate_password: str | None = field(default=None, repr=False, compare=False)
    access_token: str | None = field(default=None, repr=False, compare=False)

    def secrets(self) -> tuple[str, ...]:
        values = (
            self.account_key,
            self.client_secret,
            self.access_token,
            self.client_certificate,
            self.client_certificate_password,
        )
        return tuple(s for s in values if s)


@dataclass(frozen=True)
class CosmosOptions:
    endpoint: str
    database: str
    container: str
    auth: AuthOptions
    preferred_regions: tuple[str, ...] = ()
    consistency_level: str | None = None
    connection_timeout_seconds: float | None = None
    read_timeout_seconds: float | None = None
    tls_verify: bool = True
    tls_ca_bundle: str | None = None
    throttle_max_retries: int = 9
    throttle_max_wait_seconds: float = 60.0
    max_retries: int = 8
    retry_max_backoff_seconds: float = 60.0
    priority: str | None = None
    query: str = DEFAULT_QUERY
    query_parameters: tuple[tuple[str, Any], ...] = ()
    page_size: int = DEFAULT_PAGE_SIZE
    partitions_per_feed_range: int | str = 1
    parse_mode: str = PARSE_PERMISSIVE
    raw_json_column: str = DEFAULT_RAW_COLUMN
    corrupt_record_column: str = DEFAULT_CORRUPT_COLUMN
    log_metrics: bool = False
    change_feed_mode: str = MODE_LATEST_VERSION
    change_feed_mode_explicit: bool = False
    start_from: str = "Beginning"  # "Beginning" | "Now" | ISO-8601 (normalized, UTC)
    max_items_per_trigger: int | None = None
    change_feed_end_wait_seconds: float = 120.0
    initial_snapshot: bool = False
    initial_snapshot_delay_seconds: float = DEFAULT_INITIAL_SNAPSHOT_DELAY_SECONDS
    snapshot_stream: bool = False
    snapshot_interval_seconds: float = 0.0
    filter_pushdown: str = "false"  # "false" | "true" | "auto"
    prefetch_pages: int = 1
    throughput_bucket: int | None = None
    target_ru_per_second: float | None = None
    partition_key: str | None = None  # JSON of one logical partition key (or a hierarchical prefix)
    item_ids: tuple[tuple[str, str], ...] = ()  # (id, partition key as JSON)
    arrow_batches: str = AUTO  # "auto" | "true" | "false"
    infer_schema: bool = False
    sampling_size: int = DEFAULT_SAMPLING_SIZE
    sampling_query: str | None = None
    infer_system_properties: bool = False
    infer_timestamp: bool = False
    stream_options_present: tuple[str, ...] = ()
    batch_change_feed_options_present: tuple[str, ...] = ()
    end_at: str | None = None  # ISO-8601, UTC
    max_integrated_cache_staleness_ms: int | None = None
    slow_request_ms: float | None = None
    slow_request_log_per_minute: int = 10
    batch_checkpoint_location: str | None = None
    query_explicit: bool = False
    test_backend: str | None = None
    given: frozenset[str] = frozenset()  # the options set explicitly (lower case)

    @property
    def start_from_datetime(self) -> datetime | None:
        if self.start_from in ("Beginning", "Now"):
            return None
        return parse_timestamp_option("start_from", self.start_from)

    def parameters_for_sdk(self) -> list[dict[str, Any]] | None:
        if not self.query_parameters:
            return None
        return [{"name": name, "value": value} for name, value in self.query_parameters]

    def secrets(self) -> tuple[str, ...]:
        return self.auth.secrets()

    def __repr__(self) -> str:  # explicit, so secrets can never leak through a dataclass repr
        return (
            f"CosmosOptions(endpoint={self.endpoint!r}, database={self.database!r}, "
            f"container={self.container!r}, auth_type={self.auth.auth_type!r}, "
            f"partitions_per_feed_range={self.partitions_per_feed_range!r}, "
            f"page_size={self.page_size}, change_feed_mode={self.change_feed_mode!r}, "
            f"initial_snapshot={self.initial_snapshot}, snapshot_stream={self.snapshot_stream})"
        )


def _infer_auth_type(opts: dict[str, str]) -> str:
    candidates = []
    if _get(opts, "account_key"):
        candidates.append(AUTH_KEY)
    if _get(opts, "client_secret") or _get(opts, "client_certificate"):
        candidates.append(AUTH_SERVICE_PRINCIPAL)
    if _get(opts, "access_token"):
        candidates.append(AUTH_ACCESS_TOKEN)
    if _get(opts, "service_credential"):
        candidates.append(AUTH_SERVICE_CREDENTIAL)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates:
        raise CosmosConfigError(
            "No credentials configured. Set option 'auth_type' to one of "
            f"{', '.join(AUTH_TYPES)} and provide the matching options "
            "(e.g. auth_type=key with account_key)."
        )
    raise CosmosConfigError(
        f"Credentials for several auth types were supplied ({', '.join(candidates)}); "
        "set option 'auth_type' explicitly."
    )


# Spark plans Python data source reads (partitions(), latestOffset(), initialOffset()) in worker processes,
# and every lookup of a Unity Catalog service credential fails there: databricks.service_credentials
# only works in UDF and task processes, dbutils needs a SparkSession or notebook auth (verified on
# Databricks serverless, classic dedicated and standard access mode). Planning also needs the token.
SERVICE_CREDENTIAL_UNSUPPORTED = (
    "auth_type=service_credential is not supported: Spark plans Python data source reads in worker "
    "processes that cannot use Unity Catalog service credentials. Use auth_type=service_principal with "
    "the client secret from a secret scope, which workers can refresh. For runs shorter than the token "
    "lifetime (about one hour), mint a token on the driver with "
    "dbutils.credentials.getServiceCredentialsProvider('<name>').get_token("
    "'https://<account>.documents.azure.com/.default') and pass auth_type=access_token, access_token and "
    "access_token_expires_on."
)


_AUTH_TYPE_OPTIONS = {
    AUTH_KEY: ("account_key",),
    AUTH_SERVICE_PRINCIPAL: (
        "tenant_id",
        "client_id",
        "client_secret",
        "client_certificate",
        "client_certificate_password",
    ),
    AUTH_ACCESS_TOKEN: ("access_token", "access_token_expires_on"),
    AUTH_DEFAULT: (),
}


def _parse_auth(opts: dict[str, str]) -> AuthOptions:
    raw_type = _get(opts, "auth_type")
    auth_type = raw_type.lower() if raw_type else _infer_auth_type(opts)
    if auth_type == AUTH_SERVICE_CREDENTIAL:
        raise CosmosConfigError(SERVICE_CREDENTIAL_UNSUPPORTED)
    if auth_type not in AUTH_TYPES:
        raise CosmosConfigError(f"Option 'auth_type' must be one of {', '.join(AUTH_TYPES)}, got {raw_type!r}.")
    auth = AuthOptions(
        auth_type=auth_type,
        tenant_id=_get(opts, "tenant_id"),
        client_id=_get(opts, "client_id"),
        access_token_expires_on=_parse_expires_on(_get(opts, "access_token_expires_on")),
        account_key=_get(opts, "account_key"),
        client_secret=_get(opts, "client_secret"),
        access_token=_get(opts, "access_token"),
        client_certificate=_certificate(_get(opts, "client_certificate")),
        client_certificate_password=_get(opts, "client_certificate_password"),
    )
    required = {
        AUTH_KEY: ("account_key",),
        AUTH_SERVICE_PRINCIPAL: ("tenant_id", "client_id"),
        AUTH_ACCESS_TOKEN: ("access_token",),
        AUTH_DEFAULT: (),
    }[auth_type]
    missing = [name for name in required if not _get(opts, name)]
    if missing:
        raise CosmosConfigError(f"auth_type={auth_type} requires option(s): {', '.join(missing)}.")
    extra = [n for n in AUTH_OPTIONS if n != "auth_type" and n not in _AUTH_TYPE_OPTIONS[auth_type] and _get(opts, n)]
    if extra:
        raise CosmosConfigError(f"Option(s) {', '.join(extra)} do not apply to auth_type={auth_type}.")
    if auth.client_certificate_password and not auth.client_certificate:
        raise CosmosConfigError("Option 'client_certificate_password' only applies with 'client_certificate'.")
    if auth_type == AUTH_SERVICE_PRINCIPAL and bool(auth.client_secret) == bool(auth.client_certificate):
        raise CosmosConfigError(
            "auth_type=service_principal requires exactly one of the options client_secret and client_certificate."
        )
    if auth.client_certificate:
        try:
            base64.b64decode(auth.client_certificate, validate=True)
        except (ValueError, binascii.Error):
            raise CosmosConfigError(
                "Option 'client_certificate' must be the base64 encoding of a PEM or PKCS#12 certificate with its "
                "private key (line breaks are ignored)."
            ) from None
    return auth


def _certificate(raw: str | None) -> str | None:
    """Return the base64 text without whitespace: ``base64`` and ``openssl base64`` wrap their output."""
    return "".join(raw.split()) if raw is not None else None


def _parse_query_parameters(raw: str | None) -> tuple[tuple[str, Any], ...]:
    if raw is None:
        return ()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CosmosConfigError(
            'Option \'query_parameters\' must be a JSON list like [{"name": "@x", "value": 1}]: ' + str(exc)
        ) from None
    if not isinstance(parsed, list):
        raise CosmosConfigError("Option 'query_parameters' must be a JSON list of {name, value} objects.")
    out = []
    for i, item in enumerate(parsed):
        if not isinstance(item, dict) or "name" not in item or "value" not in item:
            raise CosmosConfigError(f"query_parameters[{i}] must be an object with 'name' and 'value'.")
        name = item["name"]
        if not isinstance(name, str) or not name.startswith("@"):
            raise CosmosConfigError(f"query_parameters[{i}].name must be a string starting with '@', got {name!r}.")
        out.append((name, item["value"]))
    return tuple(out)


def _parse_start_from(raw: str | None) -> str:
    if raw is None:
        return "Beginning"
    lowered = raw.strip().lower()
    if lowered == "beginning":
        return "Beginning"
    if lowered == "now":
        return "Now"
    return parse_timestamp_option("start_from", raw).isoformat()


def _parse_partitions_per_feed_range(raw: str | None) -> int | str:
    if raw is None:
        return 1
    if raw.strip().lower() == AUTO:
        return AUTO
    try:
        value = int(raw)
    except ValueError:
        raise CosmosConfigError(
            f"Option 'partitions_per_feed_range' must be a positive integer or 'auto', got {raw!r}."
        ) from None
    if value < 1 or value > 1024:
        raise CosmosConfigError(f"Option 'partitions_per_feed_range' must be between 1 and 1024, got {value}.")
    return value


def _parse_partition_key(raw: str | None) -> str | None:
    """Return the key as JSON: the option's JSON value, or the option text itself as a string key."""
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        value = raw
    if isinstance(value, dict) or (isinstance(value, list) and not all(not isinstance(v, (dict, list)) for v in value)):
        raise CosmosConfigError(
            "Option 'partition_key' must be a partition key value: a string, number, boolean, null, or a JSON "
            f"array of those for a hierarchical key, got {raw!r}."
        )
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _parse_item_ids(raw: str | None) -> tuple[tuple[str, str], ...]:
    if raw is None:
        return ()
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise CosmosConfigError(f'Option \'item_ids\' must be a JSON list like [["id1", "pk1"]]: {exc}') from None
    if not isinstance(parsed, list) or not parsed:
        raise CosmosConfigError(
            'Option \'item_ids\' must be a non-empty JSON list of ["id", pk] pairs or {"id", "pk"} objects.'
        )
    out = []
    for i, item in enumerate(parsed):
        if isinstance(item, dict) and set(item) == {"id", "pk"}:
            item_id, key = item["id"], item["pk"]
        elif isinstance(item, list) and len(item) == 2:
            item_id, key = item
        else:
            raise CosmosConfigError(f'item_ids[{i}] must be ["id", pk] or {{"id": ..., "pk": ...}}, got {item!r}.')
        if not isinstance(item_id, str) or not item_id:
            raise CosmosConfigError(f"item_ids[{i}]: the id must be a non-empty string, got {item_id!r}.")
        out.append((item_id, json.dumps(key, ensure_ascii=False, separators=(",", ":"))))
    return tuple(out)


def _parse_end_at(raw: str | None, mode: str) -> str | None:
    if raw is None:
        return None
    if mode != MODE_LATEST_VERSION:
        raise CosmosConfigError(
            "Option 'end_at' needs change_feed_mode=LatestVersion: the service resolves a point in time only "
            "in the LatestVersion change feed."
        )
    return parse_timestamp_option("end_at", raw).isoformat()


def _throughput_bucket(opts: dict[str, str]) -> int | None:
    value = _int(opts, "throughput_bucket", None, minimum=1)
    if value is not None and value > MAX_THROUGHPUT_BUCKET:
        raise CosmosConfigError(
            f"Option 'throughput_bucket' must be between 1 and {MAX_THROUGHPUT_BUCKET}, got {value}."
        )
    return value


def _target_ru(opts: dict[str, str]) -> float | None:
    if _get(opts, "target_ru_per_second") is None:
        return None
    value = _float(opts, "target_ru_per_second", 0.0, minimum=0.0)
    if value <= 0:
        raise CosmosConfigError(f"Option 'target_ru_per_second' must be greater than 0, got {value}.")
    return value


def _prefetch_pages(opts: dict[str, str]) -> int:
    value = _int(opts, "prefetch_pages", 1, minimum=0)
    assert value is not None
    if value > MAX_PREFETCH_PAGES:
        raise CosmosConfigError(f"Option 'prefetch_pages' must be at most {MAX_PREFETCH_PAGES}, got {value}.")
    return value


def _timeout(opts: dict[str, str], name: str) -> float | None:
    if _get(opts, name) is None:
        return None
    value = _float(opts, name, 0.0, minimum=-math.inf)
    if value <= 0:
        raise CosmosConfigError(f"Option '{name}' must be greater than 0, got {value}.")
    return value


def _parse_tristate(opts: dict[str, str], name: str, default: str) -> str:
    raw = _get(opts, name)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered == AUTO:
        return AUTO
    if lowered in ("true", "1", "yes"):
        return "true"
    if lowered in ("false", "0", "no"):
        return "false"
    raise CosmosConfigError(f"Option '{name}' must be auto, true or false, got {raw!r}.")


def _parse_filter_pushdown(raw: str | None) -> str:
    if raw is None:
        return "false"
    lowered = raw.strip().lower()
    if lowered == AUTO:
        return AUTO
    if lowered in ("true", "1", "yes"):
        return "true"
    if lowered in ("false", "0", "no"):
        return "false"
    raise CosmosConfigError(f"Option 'filter_pushdown' must be true, false or auto, got {raw!r}.")


def parse_options(options: Mapping[str, Any] | None) -> CosmosOptions:
    """Validate raw Spark options and return a typed, immutable configuration."""
    opts = _normalize(options)

    unknown = sorted(k for k in opts if k not in KNOWN_OPTIONS)
    if unknown:
        raise _unknown(unknown)
    given = frozenset(k for k in opts if _get(opts, k) is not None)

    endpoint = _get(opts, "endpoint")
    database = _get(opts, "database")
    container = _get(opts, "container")
    missing = [n for n, v in (("endpoint", endpoint), ("database", database), ("container", container)) if not v]
    if missing:
        raise CosmosConfigError(f"Missing required option(s): {', '.join(missing)}.")
    assert endpoint is not None and database is not None and container is not None
    _parse_endpoint(endpoint)

    consistency_raw = _get(opts, "consistency_level")
    consistency = None
    if consistency_raw is not None:
        consistency = _CONSISTENCY_LEVELS.get(consistency_raw.lower())
        if consistency is None:
            raise CosmosConfigError(
                f"Option 'consistency_level' must be one of {', '.join(_CONSISTENCY_LEVELS.values())}, "
                f"got {consistency_raw!r}."
            )

    priority_raw = _get(opts, "priority")
    priority = None
    if priority_raw is not None:
        priority = priority_raw.capitalize()
        if priority not in ("High", "Low"):
            raise CosmosConfigError(f"Option 'priority' must be High or Low, got {priority_raw!r}.")

    parse_mode = (_get(opts, "mode") or PARSE_PERMISSIVE).upper()
    if parse_mode not in _PARSE_MODES:
        raise CosmosConfigError(f"Option 'mode' must be one of {', '.join(_PARSE_MODES)}, got {parse_mode!r}.")

    cf_mode_raw = _get(opts, "change_feed_mode")
    cf_mode = MODE_LATEST_VERSION
    if cf_mode_raw is not None:
        cf_mode = _CHANGE_FEED_MODES.get(cf_mode_raw.replace("_", "").lower())  # type: ignore[assignment]
        if cf_mode is None:
            raise CosmosConfigError(
                f"Option 'change_feed_mode' must be LatestVersion or AllVersionsAndDeletes, got {cf_mode_raw!r}."
            )

    start_from_raw = _get(opts, "start_from")
    if cf_mode == MODE_ALL_VERSIONS_AND_DELETES:
        if start_from_raw is None:
            start_from = "Now"
        elif start_from_raw.strip().lower() != "now":
            raise CosmosConfigError(
                "change_feed_mode=AllVersionsAndDeletes only supports start_from=Now "
                f"(the service cannot replay deletes from an earlier point), got {start_from_raw!r}."
            )
        else:
            start_from = "Now"
    else:
        start_from = _parse_start_from(start_from_raw)
    future_start = start_from not in ("Beginning", "Now") and parse_timestamp_option(
        "start_from", start_from
    ) > datetime.now(timezone.utc)
    if future_start and _bool(opts, "initial_snapshot", False):
        raise CosmosConfigError(
            "Option 'start_from' cannot be in the future with initial_snapshot=true: the snapshot reads the "
            "container when the stream starts, so the changes made before start_from would be missing from "
            "the result. Use start_from=Now, or a time that has passed."
        )

    snapshot_stream = _bool(opts, "snapshot_stream", False)
    if snapshot_stream:
        conflicting = [n for n in SNAPSHOT_STREAM_EXCLUSIVE_OPTIONS if _get(opts, n) is not None]
        if conflicting:
            raise CosmosConfigError(
                f"Option(s) {', '.join(conflicting)} cannot be combined with snapshot_stream=true: a snapshot "
                "stream reads the whole container in every micro-batch, not the change feed."
            )
    elif _get(opts, "snapshot_interval_seconds") is not None:
        raise CosmosConfigError("Option 'snapshot_interval_seconds' only applies with snapshot_stream=true.")

    infer = _bool(opts, "infer_schema", False)
    if infer:
        streaming = [
            *(n for n in (*STREAM_ONLY_OPTIONS, *CHANGE_FEED_BATCH_OPTIONS) if _get(opts, n) is not None),
            *(["snapshot_stream"] if snapshot_stream else []),
        ]
        if streaming:
            raise CosmosConfigError(
                f"Option 'infer_schema' applies to batch reads, not with {', '.join(streaming)}; supply a schema "
                "with .schema(...) for streams."
            )
    else:
        stray = [n for n in INFERENCE_OPTIONS if _get(opts, n) is not None]
        if stray:
            raise CosmosConfigError(f"Option(s) {', '.join(stray)} only apply with infer_schema=true.")

    partition_key = _parse_partition_key(_get(opts, "partition_key"))
    item_ids = _parse_item_ids(_get(opts, "item_ids"))
    if partition_key is not None or item_ids:
        scoped = [n for n in ("partition_key", "item_ids") if _get(opts, n) is not None]
        streaming = [
            *(n for n in (*STREAM_ONLY_OPTIONS, *CHANGE_FEED_BATCH_OPTIONS) if _get(opts, n) is not None),
            *(["snapshot_stream"] if snapshot_stream else []),
        ]
        if len(scoped) > 1:
            raise CosmosConfigError("Options 'partition_key' and 'item_ids' cannot be combined.")
        if streaming:
            raise CosmosConfigError(f"Option '{scoped[0]}' applies to batch reads, not with {', '.join(streaming)}.")
        if item_ids and _get(opts, "query") is not None:
            raise CosmosConfigError("Option 'item_ids' reads documents by id; remove 'query'.")

    query = _get(opts, "query")
    if "query_parameters" in given and query is None and _get(opts, "sampling_query") is None:
        raise CosmosConfigError("Option 'query_parameters' only applies with 'query' or 'sampling_query'.")
    if "slow_request_log_per_minute" in given and not _float(opts, "slow_request_ms", 0.0, minimum=0.0):
        raise CosmosConfigError("Option 'slow_request_log_per_minute' only applies with 'slow_request_ms' above 0.")
    raw_column = _get(opts, "raw_json_column", DEFAULT_RAW_COLUMN) or DEFAULT_RAW_COLUMN
    corrupt_column = _get(opts, "corrupt_record_column", DEFAULT_CORRUPT_COLUMN) or DEFAULT_CORRUPT_COLUMN
    if raw_column == corrupt_column:
        raise CosmosConfigError("Options 'raw_json_column' and 'corrupt_record_column' must differ.")

    test_backend = _get(opts, TEST_BACKEND_OPTION)
    if test_backend is not None and os.environ.get(TEST_HOOKS_ENV) != "1":
        raise CosmosConfigError(f"Unknown option '{TEST_BACKEND_OPTION}'.")

    tls_verify = _bool(opts, "tls_verify", True)
    tls_ca_bundle = _get(opts, "tls_ca_bundle")
    if tls_ca_bundle is not None and not tls_verify:
        raise CosmosConfigError("Options 'tls_ca_bundle' and 'tls_verify=false' cannot be combined.")
    if not tls_verify:
        logger.warning("tls_verify=false: the TLS certificate of %s is not verified.", endpoint)

    regions_raw = _get(opts, "preferred_regions")
    regions = tuple(r.strip() for r in regions_raw.split(",") if r.strip()) if regions_raw else ()

    return CosmosOptions(
        endpoint=endpoint,
        database=database,
        container=container,
        auth=_parse_auth(opts),
        preferred_regions=regions,
        consistency_level=consistency,
        connection_timeout_seconds=_timeout(opts, "connection_timeout_seconds"),
        read_timeout_seconds=_timeout(opts, "read_timeout_seconds"),
        tls_verify=tls_verify,
        tls_ca_bundle=tls_ca_bundle,
        throttle_max_retries=_int(opts, "throttle_max_retries", 9, minimum=0),  # type: ignore[arg-type]
        throttle_max_wait_seconds=_float(opts, "throttle_max_wait_seconds", 60.0, minimum=0.0),
        max_retries=_int(opts, "max_retries", 8, minimum=0),  # type: ignore[arg-type]
        retry_max_backoff_seconds=_float(opts, "retry_max_backoff_seconds", 60.0, minimum=0.1),
        priority=priority,
        query=query or DEFAULT_QUERY,
        query_parameters=_parse_query_parameters(_get(opts, "query_parameters")),
        page_size=_int(opts, "page_size", DEFAULT_PAGE_SIZE, minimum=1),  # type: ignore[arg-type]
        partitions_per_feed_range=_parse_partitions_per_feed_range(_get(opts, "partitions_per_feed_range")),
        parse_mode=parse_mode,
        raw_json_column=raw_column,
        corrupt_record_column=corrupt_column,
        log_metrics=_bool(opts, "log_metrics", False),
        change_feed_mode=cf_mode,
        change_feed_mode_explicit=cf_mode_raw is not None,
        start_from=start_from,
        max_items_per_trigger=_int(opts, "max_items_per_trigger", None, minimum=1),
        change_feed_end_wait_seconds=_float(opts, "change_feed_end_wait_seconds", 120.0, minimum=0.0),
        initial_snapshot=_bool(opts, "initial_snapshot", False),
        initial_snapshot_delay_seconds=_float(
            opts, "initial_snapshot_delay_seconds", DEFAULT_INITIAL_SNAPSHOT_DELAY_SECONDS, minimum=0.0
        ),
        snapshot_stream=snapshot_stream,
        snapshot_interval_seconds=_float(opts, "snapshot_interval_seconds", 0.0, minimum=0.0),
        filter_pushdown=_parse_filter_pushdown(_get(opts, "filter_pushdown")),
        prefetch_pages=_prefetch_pages(opts),
        partition_key=partition_key,
        item_ids=item_ids,
        throughput_bucket=_throughput_bucket(opts),
        target_ru_per_second=_target_ru(opts),
        arrow_batches=_parse_tristate(opts, "arrow_batches", AUTO),
        infer_schema=infer,
        sampling_size=_int(opts, "sampling_size", DEFAULT_SAMPLING_SIZE, minimum=1),  # type: ignore[arg-type]
        sampling_query=_get(opts, "sampling_query"),
        infer_system_properties=_bool(opts, "infer_system_properties", False),
        infer_timestamp=_bool(opts, "infer_timestamp", False),
        stream_options_present=tuple(n for n in STREAM_ONLY_OPTIONS if _get(opts, n) is not None),
        batch_change_feed_options_present=tuple(n for n in CHANGE_FEED_BATCH_OPTIONS if _get(opts, n) is not None),
        end_at=_parse_end_at(_get(opts, "end_at"), cf_mode),
        batch_checkpoint_location=_get(opts, "batch_checkpoint_location"),
        max_integrated_cache_staleness_ms=_int(opts, "max_integrated_cache_staleness_ms", None, minimum=0),
        slow_request_ms=_float(opts, "slow_request_ms", 0.0, minimum=0.0) or None,
        slow_request_log_per_minute=_int(opts, "slow_request_log_per_minute", 10, minimum=1),  # type: ignore[arg-type]
        query_explicit=query is not None,
        test_backend=test_backend,
        given=given,
    )


def redacted_options(options: Mapping[str, Any] | None) -> dict[str, str]:
    """Return a copy of the raw options that is safe to log."""
    return {k: (REDACTED if k in SECRET_OPTIONS else v) for k, v in _normalize(options).items()}


def unused_options(opts: CosmosOptions, read: str) -> CosmosConfigError | None:
    """Return the error for options that ``read`` (a key of ``_UNUSED_BY``) would accept without using them."""
    unused = [n for n in _UNUSED_BY[read] if n in opts.given]
    if read == "change_feed" and not opts.initial_snapshot:
        unused += [n for n in _QUERY_PAGE_OPTIONS if n in opts.given]  # only the initial snapshot runs queries
    if not unused:
        return None
    return CosmosConfigError(f"Option(s) {', '.join(unused)} have no effect here: {_WHY_UNUSED[read]}.")
