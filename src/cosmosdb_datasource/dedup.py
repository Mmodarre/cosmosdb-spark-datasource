"""Per-task record of the documents a query read has emitted, with bounded memory.

A document is identified by its partition key and id. Two documents with the same key can only both
be returned by one scan if the first was deleted and the key re-created while the scan ran: the new
document gets a new ``_rid`` and the service returns documents in ``_rid`` order, so the scan reaches
it again. Deciding whether such a key was already emitted needs a record of every emitted key, so
:class:`SeenKeys` stores 64-bit fingerprints in sorted runs and moves the large runs to an anonymous
temporary file. Memory stays below about 32 MiB whatever the partition size (merging two runs briefly
holds them as a list); the file grows by 8 bytes per emitted document.
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from array import array
from bisect import bisect_left
from collections.abc import Mapping, Sequence
from typing import IO, Any, Protocol

from cosmosdb_datasource.documents import PartitionKeyInfo, parse_path

_UNDEFINED = {"undefined": True}  # partition key values are scalars, so an object cannot collide with one
_PENDING_LIMIT = 1 << 16  # fingerprints kept unsorted in a set before they become a run
_SPILL_ENTRIES = 1 << 19  # runs of this size (4 MiB) go to the temporary file
_WIDTH = 8


def _component(doc: Mapping[str, Any], segments: Sequence[str]) -> Any:
    cur: Any = doc
    for seg in segments:
        if not isinstance(cur, Mapping) or seg not in cur:
            return _UNDEFINED
        cur = cur[seg]
    return cur


def fingerprint(doc: Mapping[str, Any], pk: PartitionKeyInfo) -> int | None:
    """Return a 64-bit fingerprint of ``(partition key, id)``, or None for rows without an id.

    A missing partition key value differs from JSON null, as on the service, where they are two
    logical partitions.
    """
    doc_id = doc.get("id")
    if not isinstance(doc_id, str):
        return None
    key = [_component(doc, parse_path(p)) for p in pk.paths]
    text = json.dumps([key, doc_id], separators=(",", ":"), ensure_ascii=False, sort_keys=True, default=str)
    return int.from_bytes(hashlib.blake2b(text.encode("utf-8"), digest_size=_WIDTH).digest(), "big")


class _Run(Protocol):
    def __len__(self) -> int: ...

    def __getitem__(self, index: int) -> int: ...


class _DiskRun:
    """A sorted run of fingerprints in the temporary file, indexable like a sequence (for ``bisect``)."""

    def __init__(self, file: IO[bytes], offset: int, length: int) -> None:
        self._file = file
        self._offset = offset
        self._length = length

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, index: int) -> int:
        self._file.seek(self._offset + index * _WIDTH)
        return int.from_bytes(self._file.read(_WIDTH), "little")


class SeenKeys:
    """Exact set of 64-bit fingerprints; membership tests are only needed for a few documents per scan."""

    def __init__(self, spill_entries: int = _SPILL_ENTRIES, pending_limit: int = _PENDING_LIMIT) -> None:
        self._spill_entries = spill_entries
        self._pending_limit = pending_limit
        self._pending: set[int] = set()
        self._runs: list[array] = []  # sorted, sizes non-increasing (merged like a binary counter)
        self._spilled: list[_DiskRun] = []
        self._file: IO[bytes] | None = None
        self._file_size = 0

    @property
    def spilled_runs(self) -> int:
        return len(self._spilled)

    def add(self, fp: int) -> None:
        self._pending.add(fp)
        if len(self._pending) >= self._pending_limit:
            self._seal()

    def __contains__(self, fp: object) -> bool:
        if not isinstance(fp, int):
            return False
        if fp in self._pending:
            return True
        runs: list[_Run] = [*self._runs, *self._spilled]
        for run in runs:
            i = bisect_left(run, fp)
            if i < len(run) and run[i] == fp:
                return True
        return False

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        self._spilled = []

    def _seal(self) -> None:
        self._runs.append(array("Q", sorted(self._pending)))
        self._pending = set()
        while len(self._runs) > 1 and len(self._runs[-2]) <= len(self._runs[-1]):
            newer, older = self._runs.pop(), self._runs.pop()
            # sorted() finds the two ascending runs and merges them in linear time
            self._runs.append(array("Q", sorted(older + newer)))
        if self._runs and len(self._runs[0]) >= self._spill_entries:
            self._spill(self._runs.pop(0))

    def _spill(self, run: array) -> None:
        if self._file is None:
            # Lives as long as the read (closed by close()), so no context manager.
            self._file = tempfile.TemporaryFile(prefix="cosmosdb-seen-")  # noqa: SIM115
        if sys.byteorder != "little":
            run = array("Q", run)
            run.byteswap()
        data = run.tobytes()
        self._file.seek(self._file_size)
        self._file.write(data)
        self._spilled.append(_DiskRun(self._file, self._file_size, len(run)))
        self._file_size += len(data)
