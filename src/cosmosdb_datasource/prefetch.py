"""Read ahead of a page iterator on a background thread.

A task with one physical partition to read spends a large share of its time waiting for the next page.
Fetching page N+1 while page N is converted overlaps that wait: the request
releases the GIL while it waits on the socket. The fetch thread owns the source iterator, so the SDK pager
is still driven from one thread at a time, and pages (or the error that ended the source) reach the
consumer in source order, so a caller resuming from the continuation of the last page it consumed sees
the same sequence as without read-ahead.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from typing import Any, TypeVar

_T = TypeVar("_T")
_ITEM, _END, _ERROR = 0, 1, 2
_POLL_SECONDS = 0.5


def prefetched(source: Iterator[_T], depth: int) -> Iterator[_T]:
    """Yield the items of ``source`` while a thread fetches at most ``depth`` items ahead (0: no thread)."""
    if depth <= 0:
        yield from source
        return
    ready: queue.Queue[tuple[int, Any]] = queue.Queue()
    # One permit per item the fetcher may hold that the consumer has not taken yet.
    permits = threading.Semaphore(depth)
    stop = threading.Event()

    def fetch() -> None:
        try:
            while True:
                permits.acquire()
                if stop.is_set():
                    return
                try:
                    item = next(source)
                except StopIteration:
                    ready.put((_END, None))
                    return
                ready.put((_ITEM, item))
        except BaseException as exc:  # noqa: BLE001 - handed to the consumer, which re-raises it in order
            ready.put((_ERROR, exc))
        finally:
            close = getattr(source, "close", None)
            if callable(close):
                close()

    thread = threading.Thread(target=fetch, name="cosmosdb-prefetch", daemon=True)
    thread.start()
    try:
        while True:
            try:
                kind, value = ready.get(timeout=_POLL_SECONDS)
            except queue.Empty:
                if not thread.is_alive() and ready.empty():
                    raise RuntimeError("the cosmosdb read-ahead thread ended without a result") from None
                continue
            if kind == _END:
                return
            if kind == _ERROR:
                raise value
            permits.release()
            yield value
    finally:
        # The consumer stopped (done, failed or closed early): let a waiting fetcher exit. A fetch in
        # flight completes and is discarded.
        stop.set()
        permits.release()
