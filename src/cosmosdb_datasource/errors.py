"""Exception types and secret redaction helpers."""

from __future__ import annotations

import re
from collections.abc import Iterable

REDACTED = "***REDACTED***"


class CosmosDataSourceError(Exception):
    """Base class for all errors raised by the Cosmos DB data source."""


class CosmosConfigError(CosmosDataSourceError, ValueError):
    """Invalid or inconsistent data source options."""


class CosmosAuthError(CosmosDataSourceError):
    """Credential could not be built or a token could not be obtained."""


class CosmosConversionError(CosmosDataSourceError):
    """A document value could not be converted to the requested Spark type (FAILFAST mode)."""


class CosmosChangeFeedError(CosmosDataSourceError):
    """Change feed state is inconsistent or the change feed could not be read deterministically."""


class CosmosOffsetError(CosmosChangeFeedError):
    """A checkpointed streaming offset cannot be used with the current container or options."""


# Credential-like substrings, masked even when the literal secret is unknown. The bracketed first
# letters keep secret scanners from matching these sources ("[s]" matches "s").
_SECRET_PATTERNS = [
    # Signature parameter of the Cosmos master-key and AAD authorization headers ("type=master&ver=1.0&...").
    re.compile(r"([s]ig=)[^&\s'\"]+", re.IGNORECASE),
    # Bearer tokens / JWTs.
    re.compile(r"(Bearer\s+)[A-Za-z0-9\-_.=]+", re.IGNORECASE),
    re.compile(r"eyJ[A-Za-z0-9\-_]{10,}\.[A-Za-z0-9\-_]{10,}\.[A-Za-z0-9\-_]{10,}"),
    # The account-key parameter of connection strings.
    re.compile(r"(Account[K]ey=)[^;\s'\"]+", re.IGNORECASE),
]


def redact(text: object, secrets: Iterable[str | None] = ()) -> str:
    """Return ``str(text)`` with known secret values and credential-like substrings masked."""
    out = str(text)
    for secret in secrets:
        if secret and len(secret) >= 4:
            out = out.replace(secret, REDACTED)
    for pattern in _SECRET_PATTERNS:
        out = pattern.sub(lambda m: m.group(1) + REDACTED, out) if pattern.groups else pattern.sub(REDACTED, out)
    return out
