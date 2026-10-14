"""Outer retry policy layered on top of the azure-cosmos SDK retries.

The SDK already retries 429 (``ResourceThrottleRetryPolicy``: default 9 attempts / 30 s
cumulative wait, honouring ``x-ms-retry-after-ms``), 503, 408/5xx and connection errors
(``azure/cosmos/_retry_utility.py``). Long scans of large containers can still exhaust those
budgets under sustained throttling, so page iteration is wrapped in a second, slower layer that
resumes from the last fully consumed page's continuation token. Driver-side metadata calls
(container properties, feed ranges, change-feed position probes) use the same policy through
:func:`call_with_retry`, so throttling caused by concurrent reads does not fail query planning.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

logger = logging.getLogger("cosmosdb_datasource")

TRANSIENT_STATUS_CODES = frozenset({408, 410, 429, 449, 500, 502, 503, 504})
# 404 with sub-status 1002 (READ_SESSION_NOTAVAILABLE): no replica has caught up with the session
# token sent with the request yet. The SDK retries it once, then raises; the replica catches up.
SESSION_NOT_AVAILABLE = (404, 1002)

_T = TypeVar("_T")


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 8
    base_delay_seconds: float = 1.0
    max_backoff_seconds: float = 60.0

    def delay(self, attempt: int, exc: BaseException | None = None) -> float:
        """Seconds to sleep before retry ``attempt`` (1-based): retry-after if given, else backoff."""
        hinted = retry_after_seconds(exc) if exc is not None else None
        if hinted is not None:
            return min(self.max_backoff_seconds, hinted) + random.uniform(0, 0.25)
        exp = min(self.max_backoff_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        # "Equal jitter": keep half of the exponential delay, randomize the other half.
        return exp / 2 + random.uniform(0, exp / 2)


def status_code(exc: BaseException) -> int | None:
    code = getattr(exc, "status_code", None)
    return code if isinstance(code, int) else None


def sub_status(exc: BaseException) -> int | None:
    code = getattr(exc, "sub_status", None)
    return code if isinstance(code, int) else None


def retry_after_seconds(exc: BaseException) -> float | None:
    headers = getattr(exc, "headers", None)
    if not headers:
        response = getattr(exc, "response", None)
        headers = getattr(response, "headers", None)
    if not headers:
        return None
    try:
        for name in ("x-ms-retry-after-ms", "retry-after-ms"):
            value = headers.get(name)
            if value is not None:
                return max(0.0, float(value) / 1000.0)
        value = headers.get("Retry-After") or headers.get("retry-after")
        if value is not None:
            return max(0.0, float(value))
    except (TypeError, ValueError):
        return None
    return None


def is_transient(exc: BaseException) -> bool:
    """Errors worth retrying after the SDK gave up: throttling, timeouts, transient 5xx, network."""
    code = status_code(exc)
    if code is not None:
        return code in TRANSIENT_STATUS_CODES or (code, sub_status(exc)) == SESSION_NOT_AVAILABLE
    try:
        from azure.core.exceptions import ServiceRequestError, ServiceResponseError
        from azure.cosmos.exceptions import CosmosClientTimeoutError
    except ImportError:  # pragma: no cover - azure-core always present with azure-cosmos
        return False
    return isinstance(exc, (ServiceRequestError, ServiceResponseError, CosmosClientTimeoutError, ConnectionError))


def sleep(seconds: float) -> None:
    """Indirection so tests can replace sleeping."""
    time.sleep(seconds)


def call_with_retry(fn: Callable[[], _T], policy: RetryPolicy, what: str) -> _T:
    """``fn()``, retried with ``policy`` on transient errors only (bounded by ``max_retries``)."""
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if not is_transient(exc):
                raise
            attempt += 1
            if attempt > policy.max_retries:
                raise
            delay = policy.delay(attempt, exc)
            log_retry(what, attempt, policy.max_retries, delay, exc)
            sleep(delay)


def log_retry(what: str, attempt: int, max_retries: int, delay: float, exc: BaseException) -> None:
    logger.warning(
        "Transient Cosmos DB error while %s (attempt %d/%d, status=%s); retrying in %.1fs: %s",
        what,
        attempt,
        max_retries,
        status_code(exc),
        delay,
        type(exc).__name__,
    )
