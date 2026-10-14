"""Partition-key extraction and rendering, and change-feed item normalization.

The ``pk`` column renders a partition key as text, identically for documents and for the
partition key of AllVersionsAndDeletes delete metadata, so the rows of one document share one key:

* a string is itself; JSON null is ``null``; a boolean is ``true`` or ``false``;
* a number is the IEEE double the service hashes, an integral value without a fraction: the
  service treats 9007199254740993 and 9007199254740992 as one logical partition and reports the
  double in delete metadata;
* a hierarchical key is a compact JSON array of its components;
* an undefined key (the document lacks the property) is NULL; an undefined component of a
  hierarchical key is ``{}``, Cosmos DB's own encoding of an undefined key. JSON null and undefined
  are different logical partitions on the service.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cosmosdb_datasource.errors import CosmosChangeFeedError
from cosmosdb_datasource.options import MODE_ALL_VERSIONS_AND_DELETES

OP_UPSERT = "upsert"
OP_CREATE = "create"
OP_REPLACE = "replace"
OP_DELETE = "delete"
OP_SNAPSHOT = "snapshot"  # rows of the initial snapshot (initial_snapshot=true)
SNAPSHOT_LSN = 0  # snapshot rows carry no change LSN; 0 sorts below every change


def parse_path(path: str) -> tuple[str, ...]:
    """Split a partition key path like ``/a/b`` or ``/"odd key"/c`` into property names."""
    if not path.startswith("/"):
        raise ValueError(f"Partition key path must start with '/': {path!r}")
    segments: list[str] = []
    buf: list[str] = []
    in_quotes = False
    for ch in path[1:]:
        if ch == '"':
            in_quotes = not in_quotes
        elif ch == "/" and not in_quotes:
            segments.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    segments.append("".join(buf))
    return tuple(segments)


def _walk(doc: Any, segments: tuple[str, ...]) -> Any:
    cur = doc
    for seg in segments:
        if not isinstance(cur, Mapping) or seg not in cur:
            return None
        cur = cur[seg]
    return cur


class _Undefined:
    def __repr__(self) -> str:
        return "UNDEFINED"


UNDEFINED: Any = _Undefined()  # a partition key value the document does not have


def _walk_defined(doc: Any, segments: tuple[str, ...]) -> Any:
    cur = doc
    for seg in segments:
        if not isinstance(cur, Mapping) or seg not in cur:
            return UNDEFINED
        cur = cur[seg]
    return cur


def _canonical(value: Any) -> Any:
    if value is UNDEFINED:
        return {}
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value
    try:
        number = float(value)
    except OverflowError:  # beyond the double range: not a value the service can hash
        return value
    return int(number) if number.is_integer() else number


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def render_partition_key(components: list[Any], hierarchical: bool) -> str | None:
    """Render partition key components (``UNDEFINED`` for missing ones) as the ``pk`` column text."""
    if not components:
        return None
    if hierarchical:
        return _json([_canonical(c) for c in components])
    value = components[0]
    if value is UNDEFINED:
        return None
    if isinstance(value, str):
        return value
    return _json(_canonical(value))


@dataclass(frozen=True)
class PartitionKeyInfo:
    """Partition key definition of a container (``properties["partitionKey"]``)."""

    paths: tuple[str, ...] = ()
    kind: str = "Hash"
    version: int = 1
    system_key: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "_segments", tuple(parse_path(p) for p in self.paths))

    @classmethod
    def from_container_properties(cls, properties: Mapping[str, Any]) -> PartitionKeyInfo:
        pk = properties.get("partitionKey") or {}
        paths = tuple(pk.get("paths") or ())
        for p in paths:
            parse_path(p)  # validate early, on the driver
        return cls(
            paths=paths,
            kind=str(pk.get("kind") or "Hash"),
            version=int(pk.get("version") or 1),
            system_key=bool(pk.get("systemKey", False)),
        )

    @property
    def hierarchical(self) -> bool:
        return len(self.paths) > 1

    def extract(self, doc: Mapping[str, Any] | None) -> Any:
        """Return the native partition key value of ``doc``: a scalar, or a list for hierarchical keys."""
        if doc is None or not self.paths:
            return None
        values = [_walk(doc, segs) for segs in self._segments]  # type: ignore[attr-defined]
        return values if self.hierarchical else values[0]

    def render(self, doc: Mapping[str, Any] | None) -> str | None:
        """Return the ``pk`` column text of ``doc`` (see the module docstring)."""
        if doc is None or not self.paths:
            return None
        return render_partition_key(
            [_walk_defined(doc, segs) for segs in self._segments],  # type: ignore[attr-defined]
            self.hierarchical,
        )

    def render_metadata(self, value: Any) -> str | None:
        """Return the ``pk`` column text of the partition key reported in AllVersionsAndDeletes delete metadata.

        The service reports an object keyed by property name (``{"deviceId": "d1"}``), and null for an
        undefined key; lists and scalars are accepted too.
        """
        if not self.paths:
            return None
        if value is None:
            components = [UNDEFINED] * len(self.paths)
        elif isinstance(value, Mapping):
            components = []
            for p, segs in zip(self.paths, self._segments, strict=True):  # type: ignore[attr-defined]
                joined = "/".join(segs)
                if p in value:
                    components.append(value[p])
                elif joined in value:
                    components.append(value[joined])
                elif segs[-1] in value:
                    components.append(value[segs[-1]])
                else:
                    components.append(_walk_defined(value, segs))
        elif isinstance(value, list):
            components = value if self.hierarchical or len(value) != 1 else value[:1]
        else:
            components = [value]
        return render_partition_key(list(components), self.hierarchical)

    def to_dict(self) -> dict[str, Any]:
        return {"paths": list(self.paths), "kind": self.kind, "version": self.version}


@dataclass
class ChangeRecord:
    """Mode-independent view of one change-feed item."""

    id: str | None
    pk: str | None  # rendered, see render_partition_key
    operation: str
    ts: int | None
    lsn: int
    crts: int | None
    ttl_expired: bool
    doc: dict[str, Any] | None


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def item_lsn(item: Mapping[str, Any], mode: str) -> int | None:
    if mode == MODE_ALL_VERSIONS_AND_DELETES:
        meta = item.get("metadata")
        return _as_int(meta.get("lsn")) if isinstance(meta, Mapping) else None
    return _as_int(item.get("_lsn"))


def to_snapshot_record(doc: Mapping[str, Any], pk_info: PartitionKeyInfo) -> ChangeRecord:
    """Convert a document read by the initial snapshot to change-feed form (``operation='snapshot'``)."""
    return ChangeRecord(
        id=doc.get("id"),
        pk=pk_info.render(doc),
        operation=OP_SNAPSHOT,
        ts=_as_int(doc.get("_ts")),
        lsn=SNAPSHOT_LSN,
        crts=None,
        ttl_expired=False,
        doc=doc if isinstance(doc, dict) else dict(doc),
    )


def to_change_record(item: Mapping[str, Any], mode: str, pk_info: PartitionKeyInfo) -> ChangeRecord:
    lsn = item_lsn(item, mode)
    if lsn is None:
        field = "metadata.lsn" if mode == MODE_ALL_VERSIONS_AND_DELETES else "_lsn"
        raise CosmosChangeFeedError(
            f"Change feed item without '{field}' (id={item.get('id')!r}); cannot bound the read deterministically."
        )
    if mode != MODE_ALL_VERSIONS_AND_DELETES:
        latest = dict(item)
        return ChangeRecord(
            id=latest.get("id"),
            pk=pk_info.render(latest),
            operation=OP_UPSERT,
            ts=_as_int(latest.get("_ts")),
            lsn=lsn,
            crts=None,
            ttl_expired=False,
            doc=latest,
        )
    meta = item.get("metadata") or {}
    op = str(meta.get("operationType") or "").lower() or OP_UPSERT
    crts = _as_int(meta.get("crts"))
    current = item.get("current")
    previous = item.get("previous")
    if op == OP_DELETE:
        prev = previous if isinstance(previous, Mapping) else {}
        if "partitionKey" in meta or not prev:
            pk = pk_info.render_metadata(meta.get("partitionKey"))
        else:
            pk = pk_info.render(prev)
        return ChangeRecord(
            id=meta.get("id") or prev.get("id"),
            pk=pk,
            operation=OP_DELETE,
            ts=crts if crts is not None else _as_int(prev.get("_ts")),
            lsn=lsn,
            crts=crts,
            ttl_expired=bool(meta.get("timeToLiveExpired", False)),
            doc=None,
        )
    doc = dict(current) if isinstance(current, Mapping) else None
    return ChangeRecord(
        id=(doc or {}).get("id") or meta.get("id"),
        pk=pk_info.render(doc) if doc is not None else pk_info.render_metadata(meta.get("partitionKey")),
        operation=op,
        ts=_as_int((doc or {}).get("_ts")) if doc and doc.get("_ts") is not None else crts,
        lsn=lsn,
        crts=crts,
        ttl_expired=bool(meta.get("timeToLiveExpired", False)),
        doc=doc,
    )
