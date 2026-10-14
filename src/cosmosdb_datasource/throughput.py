"""Local RU budget of one read (``target_ru_per_second``).

Planning splits the budget evenly over the read's input partitions; a task's share joins a token bucket
that the read's tasks running on the same machine share, kept in a small file under the temp directory
(removed when the read's last task leaves) and updated under ``fcntl.flock``. The bucket refills at the
sum of the shares of its current members and is charged with every page's ``x-ms-request-charge`` after
the page arrived; a task whose charge leaves the bucket in debt sleeps until the debt is repaid. A read
therefore stays at or below its budget however its tasks are spread over executors, and tasks on one
executor use each other's unused share. A task leaves the bucket when its page iterator ends; a member
not seen for ``STALE_SECONDS`` after its last sleep ended (a crashed worker) is dropped.

The budget covers one read, or one micro-batch of a stream. Nothing coordinates reads, jobs or other
clients of the container: this is not the connector's global throughput control.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import os
import tempfile
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - not on Spark executors (Linux)
    fcntl = None  # type: ignore[assignment]

STALE_SECONDS = 30.0
BURST_SECONDS = 1.0  # bucket capacity, in seconds of the current rate
_DIRECTORY = "cosmosdb-ru-budget"
_LOCAL_LOCK = threading.Lock()
_members = itertools.count()


def sleep(seconds: float) -> None:
    """Sleep ``seconds``; an indirection so tests can replace sleeping."""
    time.sleep(seconds)


@dataclass(frozen=True)
class RuBudget:
    """The share of one input partition in a read's RU budget."""

    read_id: str
    ru_per_second: float


class SharedBucket:
    def __init__(self, budget: RuBudget, directory: str | None = None) -> None:
        root = directory or os.path.join(tempfile.gettempdir(), _DIRECTORY)
        os.makedirs(root, exist_ok=True)
        self._path = os.path.join(root, f"{budget.read_id}.json")
        # One lock file for every read: per-read lock files could not be removed safely (a process waiting
        # on a removed file's lock would no longer exclude one that opens a new file).
        self._lock_path = os.path.join(root, ".lock")
        self._share = budget.ru_per_second
        self._member = f"{os.getpid()}-{next(_members)}"
        self.waited = 0.0

    @contextlib.contextmanager
    def _state(self) -> Iterator[dict[str, Any]]:
        with _LOCAL_LOCK, open(self._lock_path, "a+") as lock:
            if fcntl is not None:
                fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                try:
                    with open(self._path) as fh:
                        state = json.load(fh)
                except (FileNotFoundError, ValueError):
                    state = {}
                yield state
                if state.get("members") or "tokens" not in state:
                    tmp = f"{self._path}.{self._member}.tmp"
                    with open(tmp, "w") as fh:
                        json.dump(state, fh)
                    os.replace(tmp, self._path)
                else:  # the read's last member left
                    with contextlib.suppress(FileNotFoundError):
                        os.remove(self._path)
            finally:
                if fcntl is not None:
                    fcntl.flock(lock, fcntl.LOCK_UN)

    def consume(self, request_charge: float) -> None:
        """Charge ``request_charge`` RU and sleep until the bucket is out of debt."""
        with self._state() as state:
            now = time.monotonic()
            members = {k: v for k, v in state.get("members", {}).items() if now - v[1] < STALE_SECONDS}
            members[self._member] = [self._share, now]
            rate = sum(share for share, _seen in members.values())
            tokens = state.get("tokens", rate * BURST_SECONDS) + rate * max(0.0, now - state.get("updated", now))
            tokens = min(tokens, rate * BURST_SECONDS) - request_charge
            wait = max(0.0, -tokens / rate)
            # A member counts until STALE_SECONDS after the end of its sleep, so long waits keep its share.
            members[self._member] = [self._share, now + wait]
            state.update(tokens=tokens, updated=now, members=members)
        if wait > 0:
            self.waited += wait
            sleep(wait)

    def leave(self) -> None:
        with self._state() as state:
            now = time.monotonic()
            members = {k: v for k, v in state.get("members", {}).items() if now - v[1] < STALE_SECONDS}
            members.pop(self._member, None)
            state["members"] = members


class ThrottledBackend:
    """A backend whose page iterators and item reads are charged to a :class:`SharedBucket`."""

    def __init__(self, backend: Any, budget: RuBudget) -> None:
        self._backend = backend
        self._budget = budget

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)

    def _metered(self, pages: Iterator[Any]) -> Iterator[Any]:
        bucket = SharedBucket(self._budget)
        try:
            for page in pages:
                bucket.consume(page.request_charge)
                yield page
        finally:
            bucket.leave()

    def query_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.query_pages(*args, **kwargs))

    def query_partition_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.query_partition_pages(*args, **kwargs))

    def change_feed_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.change_feed_pages(*args, **kwargs))

    def read_items(self, items: list[tuple[str, Any]]) -> tuple[list[dict[str, Any]], float]:
        bucket = SharedBucket(self._budget)
        try:
            docs, charge = self._backend.read_items(items)
            bucket.consume(charge)
            return docs, charge
        finally:
            bucket.leave()
