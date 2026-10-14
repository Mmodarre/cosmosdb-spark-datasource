"""Option parsing and validation.

Spark hands options to the data source as a case-insensitive mapping (keys are lower-cased by
``pyspark.sql.datasource.CaseInsensitiveDict``). Everything here is pure Python and cheap: the
data source and reader objects are constructed in several Python worker processes, so parsing must
not perform network calls.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

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
    AUTH_SERVICE_CREDENTIAL,
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
DEFAULT_PAGE_SIZE = 1000
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

SECRET_OPTIONS = ("account_key", "client_secret", "access_token")

# Internal, undocumented test hook: "<module>:<callable>" returning a backend. Only honoured when
# the environment variable below is set, so it cannot be triggered by accident in production.
TEST_BACKEND_OPTION = "_test_backend"
TEST_HOOKS_ENV = "COSMOSDB_DATASOURCE_TEST_HOOKS"

KNOWN_OPTIONS = frozenset(
    {
        "endpoint",
        "database",
        "container",
        "auth_type",
        "account_key",
        "tenant_id",
        "client_id",
        "client_secret",
        "access_token",
        "access_token_expires_on",
        "service_credential",
        "preferred_regions",
        "consistency_level",
        "connection_timeout_seconds",
        "read_timeout_seconds",
        "throttle_max_retries",
        "throttle_max_wait_seconds",
        "max_retries",
        "retry_max_backoff_seconds",
        "priority",
        "query",
        "query_parameters",
        "page_size",
        "partitions_per_feed_range",
        "mode",
        "raw_json_column",
        "corrupt_record_column",
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
        "infer_schema",
        "sampling_size",
        "sampling_query",
        "infer_system_properties",
        "infer_timestamp",
        TEST_BACKEND_OPTION,
        # Spark may add "path" when .load(path) is used; we ignore it.
        "path",
    }
)

_ENDPOINT_RE = re.compile(r"^https://[A-Za-z0-9.\-]+(:\d+)?/?$")


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
    service_credential: str | None = None
    access_token_expires_on: int | None = None
    # Secrets: excluded from repr/eq so they never end up in logs or error messages.
    account_key: str | None = field(default=None, repr=False, compare=False)
    client_secret: str | None = field(default=None, repr=False, compare=False)
    access_token: str | None = field(default=None, repr=False, compare=False)

    def secrets(self) -> tuple[str, ...]:
        return tuple(s for s in (self.account_key, self.client_secret, self.access_token) if s)


@dataclass(frozen=True)
class CosmosOptions:
    endpoint: str
    database: str
    container: str
    auth: AuthOptions
    preferred_regions: tuple[str, ...] = ()
    consistency_level: str | None = None
    connection_timeout_seconds: int | None = None
    read_timeout_seconds: int | None = None
    throttle_max_retries: int = 9
    throttle_max_wait_seconds: int = 60
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
    infer_schema: bool = False
    sampling_size: int = DEFAULT_SAMPLING_SIZE
    sampling_query: str | None = None
    infer_system_properties: bool = False
    infer_timestamp: bool = False
    stream_options_present: tuple[str, ...] = ()
    query_explicit: bool = False
    test_backend: str | None = None

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
    if _get(opts, "client_secret"):
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


def _parse_auth(opts: dict[str, str]) -> AuthOptions:
    raw_type = _get(opts, "auth_type")
    auth_type = raw_type.lower() if raw_type else _infer_auth_type(opts)
    if auth_type not in AUTH_TYPES:
        raise CosmosConfigError(f"Option 'auth_type' must be one of {', '.join(AUTH_TYPES)}, got {raw_type!r}.")
    auth = AuthOptions(
        auth_type=auth_type,
        tenant_id=_get(opts, "tenant_id"),
        client_id=_get(opts, "client_id"),
        service_credential=_get(opts, "service_credential"),
        access_token_expires_on=_parse_expires_on(_get(opts, "access_token_expires_on")),
        account_key=_get(opts, "account_key"),
        client_secret=_get(opts, "client_secret"),
        access_token=_get(opts, "access_token"),
    )
    required = {
        AUTH_KEY: ("account_key",),
        AUTH_SERVICE_PRINCIPAL: ("tenant_id", "client_id", "client_secret"),
        AUTH_ACCESS_TOKEN: ("access_token",),
        AUTH_SERVICE_CREDENTIAL: ("service_credential",),
        AUTH_DEFAULT: (),
    }[auth_type]
    missing = [name for name in required if not _get(opts, name)]
    if missing:
        raise CosmosConfigError(f"auth_type={auth_type} requires option(s): {', '.join(missing)}.")
    return auth


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
        logger.warning("Ignoring unknown cosmosdb option(s): %s", ", ".join(unknown))

    endpoint = _get(opts, "endpoint")
    database = _get(opts, "database")
    container = _get(opts, "container")
    missing = [n for n, v in (("endpoint", endpoint), ("database", database), ("container", container)) if not v]
    if missing:
        raise CosmosConfigError(f"Missing required option(s): {', '.join(missing)}.")
    assert endpoint is not None and database is not None and container is not None
    if not _ENDPOINT_RE.match(endpoint):
        raise CosmosConfigError(
            f"Option 'endpoint' must look like https://<account>.documents.azure.com:443/, got {endpoint!r}."
        )

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
            *(n for n in STREAM_ONLY_OPTIONS if _get(opts, n) is not None),
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

    query = _get(opts, "query")
    raw_column = _get(opts, "raw_json_column", DEFAULT_RAW_COLUMN) or DEFAULT_RAW_COLUMN
    corrupt_column = _get(opts, "corrupt_record_column", DEFAULT_CORRUPT_COLUMN) or DEFAULT_CORRUPT_COLUMN
    if raw_column == corrupt_column:
        raise CosmosConfigError("Options 'raw_json_column' and 'corrupt_record_column' must differ.")

    test_backend = _get(opts, TEST_BACKEND_OPTION)
    if test_backend is not None and os.environ.get(TEST_HOOKS_ENV) != "1":
        raise CosmosConfigError(f"Unknown option '{TEST_BACKEND_OPTION}'.")

    regions_raw = _get(opts, "preferred_regions")
    regions = tuple(r.strip() for r in regions_raw.split(",") if r.strip()) if regions_raw else ()

    return CosmosOptions(
        endpoint=endpoint,
        database=database,
        container=container,
        auth=_parse_auth(opts),
        preferred_regions=regions,
        consistency_level=consistency,
        connection_timeout_seconds=_int(opts, "connection_timeout_seconds", None, minimum=1),
        read_timeout_seconds=_int(opts, "read_timeout_seconds", None, minimum=1),
        throttle_max_retries=_int(opts, "throttle_max_retries", 9, minimum=0),  # type: ignore[arg-type]
        throttle_max_wait_seconds=_int(opts, "throttle_max_wait_seconds", 60, minimum=0),  # type: ignore[arg-type]
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
        infer_schema=infer,
        sampling_size=_int(opts, "sampling_size", DEFAULT_SAMPLING_SIZE, minimum=1),  # type: ignore[arg-type]
        sampling_query=_get(opts, "sampling_query"),
        infer_system_properties=_bool(opts, "infer_system_properties", False),
        infer_timestamp=_bool(opts, "infer_timestamp", False),
        stream_options_present=tuple(n for n in STREAM_ONLY_OPTIONS if _get(opts, n) is not None),
        query_explicit=query is not None,
        test_backend=test_backend,
    )


def redacted_options(options: Mapping[str, Any] | None) -> dict[str, str]:
    """Return a copy of the raw options that is safe to log."""
    return {k: (REDACTED if k in SECRET_OPTIONS else v) for k, v in _normalize(options).items()}
