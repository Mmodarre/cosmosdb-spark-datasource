"""The checkpoint of change-feed batch reads (``batch_checkpoint_location``)."""

from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Any

from cosmosdb_datasource.errors import CosmosConfigError, CosmosOffsetError
from cosmosdb_datasource.offsets import Offset

_FILE = re.compile(r"^\d+$")


@dataclass(frozen=True)
class PlannedRange:
    index: int
    start: Offset
    end: Offset
    caught_up: bool

    def to_dict(self) -> dict[str, Any]:
        return {"v": 1, "start": self.start.to_dict(), "end": self.end.to_dict(), "caught_up": self.caught_up}

    @classmethod
    def from_dict(cls, index: int, data: Any) -> PlannedRange:
        if not isinstance(data, dict) or data.get("v") != 1:
            raise CosmosOffsetError(f"Batch checkpoint offsets/{index} is not a cosmosdb change-feed range.")
        return cls(index, Offset.from_dict(data["start"]), Offset.from_dict(data["end"]), bool(data.get("caught_up")))


def _local_path(location: str) -> str:
    if location.startswith("dbfs:/"):
        return "/dbfs/" + location[len("dbfs:/") :].lstrip("/")
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", location):
        raise CosmosConfigError(
            f"batch_checkpoint_location must be a path the driver can open as a file (for example /Volumes/... "
            f"or /dbfs/... on Databricks), got {location!r}."
        )
    return location


class BatchCheckpoint:
    """``offsets/<n>``: the range planned for read n; ``commits/<n>``: read n's output was written."""

    def __init__(self, location: str) -> None:
        root = _local_path(location)
        self._offsets = os.path.join(root, "offsets")
        self._commits = os.path.join(root, "commits")

    def _indexes(self, directory: str) -> list[int]:
        try:
            return sorted(int(n) for n in os.listdir(directory) if _FILE.match(n))
        except FileNotFoundError:
            return []

    def latest(self) -> PlannedRange | None:
        indexes = self._indexes(self._offsets)
        if not indexes:
            return None
        n = indexes[-1]
        with open(os.path.join(self._offsets, str(n))) as fh:
            return PlannedRange.from_dict(n, json.load(fh))

    def committed(self, index: int) -> bool:
        return os.path.exists(os.path.join(self._commits, str(index)))

    def plan(self, planned: PlannedRange) -> None:
        """Create ``offsets/<n>``; fail if it exists, so two jobs cannot both plan read n."""
        os.makedirs(self._offsets, exist_ok=True)
        path = os.path.join(self._offsets, str(planned.index))
        text = json.dumps(planned.to_dict())
        fd, tmp = tempfile.mkstemp(dir=self._offsets, prefix=f".{planned.index}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(text)
            try:
                os.link(tmp, path)  # atomic, and never replaces an existing file
            except FileExistsError:
                raise self._taken(planned.index) from None
            except OSError:  # a file system without hard links, such as some FUSE mounts
                try:
                    with open(path, "x") as fh:
                        fh.write(text)
                except FileExistsError:
                    raise self._taken(planned.index) from None
        finally:
            with contextlib.suppress(OSError):
                os.remove(tmp)

    @staticmethod
    def _taken(index: int) -> CosmosOffsetError:
        return CosmosOffsetError(f"Batch checkpoint offsets/{index} exists already: another job writes it.")

    def commit(self) -> bool:
        latest = self.latest()
        if latest is None:
            raise CosmosOffsetError("The batch checkpoint has no planned read to commit.")
        os.makedirs(self._commits, exist_ok=True)
        with open(os.path.join(self._commits, str(latest.index)), "w") as fh:
            fh.write("{}")
        return latest.caught_up


def commit_batch_checkpoint(location: str) -> bool:
    """Mark the latest change-feed batch read of ``location`` as written; return True if it was caught up.

    Call it after the read's output was written. The next read with the checkpoint starts where this one
    ended. Caught up means the read reached the position it was planned to reach (``end_at``, or the current
    position when it was planned) rather than stopping at ``max_items_per_trigger``.
    """
    return BatchCheckpoint(location).commit()
