"""Local RU budget of one read (``target_ru_per_second``).

Planning splits the budget evenly over the read's input partitions. A task's share joins a token bucket
that the read's tasks running on the same machine share: a small file in a directory under the temp
directory that only the user running Spark can open (``cosmosdb-ru-budget-<uid>``, mode 0700), updated
under ``fcntl.flock``. The bucket refills at the sum of the shares of its current members and is charged
with every page's and every ``read_items`` call's ``x-ms-request-charge`` after it arrived; a task whose
charge leaves the bucket in debt sleeps until the debt is repaid. A task charges one bucket for its whole
read (every page iterator, resume and point-read chunk), so a new iterator never starts with a fresh
burst. A read therefore stays at or below its budget however its tasks are spread over executors, and
tasks on one executor use each other's unused share.

A task leaves the bucket when its read ends; a member not seen for ``STALE_SECONDS`` after its last sleep
ended (a crashed worker) is dropped. The file outlives its members, so a task that joins later continues
from the bucket's state; the files of reads without a live member are removed once they have not been
written for ``STALE_SECONDS``, by the next task that opens a bucket on the machine.

Without ``fcntl``, where the directory is not private to the user, or where files cannot be locked
(``ENOLCK`` on some network file systems), the bucket is shared only by the tasks of one Python worker, and
a WARNING says so once per worker.

The budget covers one read, or one micro-batch of a stream. Nothing coordinates reads, jobs or other
clients of the container: this is not the connector's global throughput control.
"""

from __future__ import annotations

import contextlib
import itertools
import json
import logging
import math
import os
import stat
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

logger = logging.getLogger("cosmosdb_datasource")

STALE_SECONDS = 30.0
BURST_SECONDS = 1.0  # bucket capacity, in seconds of the current rate
# A state file older than this is removed whatever it holds: its monotonic times may predate a reboot.
ABANDONED_SECONDS = 86_400.0
_DIRECTORY = "cosmosdb-ru-budget"
_LOCAL_LOCK = threading.Lock()
_LOCAL_STATES: dict[str, dict[str, Any]] = {}  # buckets shared by the tasks of this process only
_members = itertools.count()
_warned_local = False


def sleep(seconds: float) -> None:
    """Sleep ``seconds``; an indirection so tests can replace sleeping."""
    time.sleep(seconds)


@dataclass(frozen=True)
class RuBudget:
    """The share of one input partition in a read's RU budget."""

    read_id: str
    ru_per_second: float


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _valid_state(raw: Any) -> dict[str, Any]:
    """Return the bucket state in ``raw``, or an empty state if it is not one (a damaged file)."""
    if not isinstance(raw, dict):
        return {}
    tokens, updated, members = raw.get("tokens"), raw.get("updated"), raw.get("members")
    if not (isinstance(tokens, float | int) and isinstance(updated, float | int) and isinstance(members, dict)):
        return {}
    if not (_number(tokens) and _number(updated)):
        return {}
    clean = {
        name: [float(entry[0]), float(entry[1])]
        for name, entry in members.items()
        if isinstance(name, str) and isinstance(entry, list) and len(entry) == 2 and all(map(_number, entry))
    }
    return {"tokens": float(tokens), "updated": float(updated), "members": clean}


def _live(members: dict[str, list[float]], now: float) -> dict[str, list[float]]:
    return {name: entry for name, entry in members.items() if now - entry[1] < STALE_SECONDS}


def _private_directory() -> str | None:
    """Return the per-user directory of the bucket files, or None if it cannot be used safely."""
    if fcntl is None or not hasattr(os, "getuid"):
        return None
    root = os.path.join(tempfile.gettempdir(), f"{_DIRECTORY}-{os.getuid()}")
    try:
        os.mkdir(root, 0o700)
    except FileExistsError:
        pass
    except OSError:
        return None
    try:
        info = os.lstat(root)
    except OSError:
        return None
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        return None
    return root


def _warn_local(reason: str) -> None:
    global _warned_local
    if not _warned_local:
        _warned_local = True
        logger.warning(
            "cosmosdb target_ru_per_second: %s, so a read's RU budget is shared only by its tasks in this "
            "Python worker; tasks in other workers on this machine charge their own buckets.",
            reason,
        )


class SharedBucket:
    def __init__(self, budget: RuBudget, directory: str | None = None) -> None:
        """Join the bucket of ``budget``'s read; ``directory`` replaces the per-user directory (tests)."""
        self._read_id = budget.read_id
        self._share = budget.ru_per_second
        self._member = f"{os.getpid()}-{next(_members)}"
        self.waited = 0.0
        self._root = directory if directory is not None else _private_directory()
        if self._root is None:
            _warn_local("there is no private directory for the shared bucket under the temp directory")
        else:
            self._sweep()

    @contextlib.contextmanager
    def _locked(self) -> Iterator[bool]:
        """Hold the lock of the bucket files; yield False to use the process-local state instead."""
        with _LOCAL_LOCK:
            if self._root is None or fcntl is None:
                yield False
                return
            # One lock file for every read: per-read lock files could not be removed safely (a process waiting
            # on a removed file's lock would no longer exclude one that opens a new file).
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(os.path.join(self._root, ".lock"), flags, 0o600)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                except OSError as exc:  # for example ENOLCK
                    _warn_local(f"the bucket files cannot be locked ({exc.strerror or exc})")
                    self._root = None
                    yield False
                    return
                try:
                    yield True
                finally:
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    @contextlib.contextmanager
    def _state(self) -> Iterator[dict[str, Any]]:
        with self._locked() as shared:
            if not shared or self._root is None:
                yield _LOCAL_STATES.setdefault(self._read_id, {})
                return
            path = os.path.join(self._root, f"{self._read_id}.json")
            try:
                with open(path) as fh:
                    state = _valid_state(json.load(fh))
            except (FileNotFoundError, ValueError):
                state = {}
            yield state
            fd, tmp = tempfile.mkstemp(dir=self._root, prefix=f".{self._read_id}.", suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as fh:
                    json.dump(state, fh)
                os.replace(tmp, path)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.remove(tmp)
                raise

    def _sweep(self) -> None:
        """Remove the files of reads without a live member that were not written for ``STALE_SECONDS``."""
        with self._locked() as shared:
            if not shared or self._root is None:
                return
            now, wall = time.monotonic(), time.time()
            for name in os.listdir(self._root):
                if name in (".lock", f"{self._read_id}.json"):
                    continue
                path = os.path.join(self._root, name)
                try:
                    age = wall - os.lstat(path).st_mtime
                    if age < STALE_SECONDS:
                        continue
                    if name.endswith(".json") and age < ABANDONED_SECONDS:
                        with open(path) as fh:
                            if _live(_valid_state(json.load(fh)).get("members", {}), now):
                                continue
                    os.remove(path)
                except (OSError, ValueError):
                    continue

    def consume(self, request_charge: float) -> None:
        """Charge ``request_charge`` RU and sleep until the bucket is out of debt."""
        with self._state() as state:
            now = time.monotonic()
            members = _live(state.get("members", {}), now)
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
            members = _live(state.get("members", {}), time.monotonic())
            members.pop(self._member, None)
            state["members"] = members


class ThrottledBackend:
    """A backend whose page iterators and item reads are charged to one :class:`SharedBucket` per task."""

    def __init__(self, backend: Any, budget: RuBudget) -> None:
        self._backend = backend
        self._budget = budget
        self._bucket: SharedBucket | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)

    def _charge(self, request_charge: float) -> None:
        if self._bucket is None:
            self._bucket = SharedBucket(self._budget)
        self._bucket.consume(request_charge)

    def leave(self) -> None:
        """End the task's membership of the bucket: its read is done."""
        if self._bucket is not None:
            self._bucket.leave()
            self._bucket = None

    def _metered(self, pages: Iterator[Any]) -> Iterator[Any]:
        for page in pages:
            self._charge(page.request_charge)
            yield page

    def query_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.query_pages(*args, **kwargs))

    def query_partition_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.query_partition_pages(*args, **kwargs))

    def change_feed_pages(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self._metered(self._backend.change_feed_pages(*args, **kwargs))

    def read_items(self, items: list[tuple[str, Any]]) -> tuple[list[dict[str, Any]], float]:
        docs, charge = self._backend.read_items(items)
        self._charge(charge)
        return docs, charge


def leave(backend: Any) -> None:
    """End a task's membership of its read's bucket, if its backend is throttled."""
    if isinstance(backend, ThrottledBackend):
        backend.leave()
