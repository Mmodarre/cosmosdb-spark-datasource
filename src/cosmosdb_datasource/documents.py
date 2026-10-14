"""Partition-key extraction and change-feed item normalization."""

from __future__ import annotations

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
        """Native partition key value of ``doc``: a scalar, or a list for hierarchical keys."""
        if doc is None or not self.paths:
            return None
        values = [_walk(doc, segs) for segs in self._segments]  # type: ignore[attr-defined]
        return values if self.hierarchical else values[0]

    def extract_from_metadata(self, value: Any) -> Any:
        """Partition key as reported in AllVersionsAndDeletes delete metadata.

        The service reports it as an object keyed by property name (e.g. ``{"deviceId": "d1"}``);
        lists and scalars are accepted too.
        """
        if value is None:
            return None
        if isinstance(value, Mapping):
            out = []
            for p in self.paths:
                segs = parse_path(p)
                if p in value:
                    out.append(value[p])
                elif "/".join(segs) in value:
                    out.append(value["/".join(segs)])
                elif segs[-1] in value:
                    out.append(value[segs[-1]])
                else:
                    out.append(_walk(value, segs))
            if not out:
                return None
            return out if self.hierarchical else out[0]
        if isinstance(value, list):
            if self.hierarchical:
                return value
            return value[0] if len(value) == 1 else value
        return value

    def to_dict(self) -> dict[str, Any]:
        return {"paths": list(self.paths), "kind": self.kind, "version": self.version}


@dataclass
class ChangeRecord:
    """Mode-independent view of one change-feed item."""

    id: str | None
    pk: Any
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
    """A document read by the initial snapshot, in change-feed form (``operation='snapshot'``)."""
    return ChangeRecord(
        id=doc.get("id"),
        pk=pk_info.extract(doc),
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
        doc = dict(item)
        return ChangeRecord(
            id=doc.get("id"),
            pk=pk_info.extract(doc),
            operation=OP_UPSERT,
            ts=_as_int(doc.get("_ts")),
            lsn=lsn,
            crts=None,
            ttl_expired=False,
            doc=doc,
        )
    meta = item.get("metadata") or {}
    op = str(meta.get("operationType") or "").lower() or OP_UPSERT
    crts = _as_int(meta.get("crts"))
    current = item.get("current")
    previous = item.get("previous")
    if op == OP_DELETE:
        prev = previous if isinstance(previous, Mapping) else {}
        pk = pk_info.extract_from_metadata(meta.get("partitionKey"))
        if pk is None and prev:
            pk = pk_info.extract(prev)
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
        pk=pk_info.extract(doc) if doc is not None else pk_info.extract_from_metadata(meta.get("partitionKey")),
        operation=op,
        ts=_as_int((doc or {}).get("_ts")) if doc and doc.get("_ts") is not None else crts,
        lsn=lsn,
        crts=crts,
        ttl_expired=bool(meta.get("timeToLiveExpired", False)),
        doc=doc,
    )
