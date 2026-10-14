"""Sampled diagnostics of slow requests (``slow_request_ms``).

The fetch of every page (one or more HTTP responses, including the SDK's own retries) is timed where it
happens, on the read-ahead thread when there is one. A fetch slower than the threshold is logged at
WARNING with what identifies it on the service side: the activity ids, the physical partitions, the
request charge, the service-side duration and the SDK's throttling retries. At most
``slow_request_log_per_minute`` lines are written per Python worker per minute; the number left out is
reported with the next line, so a throttled read does not flood the executor log.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger("cosmosdb_datasource")

_HEADERS = (
    ("activity", "x-ms-activity-id"),
    ("partition", "x-ms-documentdb-partitionkeyrangeid"),
    ("service_ms", "x-ms-request-duration-ms"),
    ("throttle_retries", "x-ms-throttle-retry-count"),
    ("throttle_wait_ms", "x-ms-throttle-retry-wait-time-ms"),
)


class SlowRequestLog:
    def __init__(self, threshold_ms: float, per_minute: int) -> None:
        self._threshold = threshold_ms / 1000.0
        self._per_minute = per_minute
        self._lock = threading.Lock()
        self._window = 0.0
        self._logged = 0
        self._suppressed = 0

    def observe(self, operation: str, elapsed: float, responses: list[Mapping[str, Any]], **details: Any) -> None:
        """Log the fetch if it took longer than the threshold and this minute's quota is not used up."""
        if elapsed < self._threshold:
            return
        with self._lock:
            now = time.monotonic()
            if now - self._window >= 60.0:
                self._window, self._logged = now, 0
            if self._logged >= self._per_minute:
                self._suppressed += 1
                return
            self._logged += 1
            suppressed, self._suppressed = self._suppressed, 0
        fields = {name: _collect(responses, header) for name, header in _HEADERS}
        charge = sum(_float(r.get("x-ms-request-charge")) for r in responses)
        text = " ".join(f"{k}={v}" for k, v in {**details, **fields}.items() if v not in (None, "", []))
        logger.warning(
            "cosmosdb slow %s: %.0f ms (threshold %.0f ms), %d response(s), request_charge=%.2f %s%s",
            operation,
            elapsed * 1000,
            self._threshold * 1000,
            len(responses),
            charge,
            text,
            f" ({suppressed} slow request(s) not logged in the last minute)" if suppressed else "",
        )


def _collect(responses: list[Mapping[str, Any]], header: str) -> str:
    values: list[str] = []
    for response in responses:
        value = response.get(header)
        if value is not None and str(value) not in values:
            values.append(str(value))
    return ",".join(values)


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
