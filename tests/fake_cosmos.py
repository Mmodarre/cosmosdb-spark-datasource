"""In-memory / file-backed fake of the Cosmos DB behaviours the data source relies on.

Implements the backend interface of ``cosmosdb_datasource.backend.SdkBackend`` with service-like
semantics:

* documents are routed by their real EPK (computed with the SDK's own hashing),
* every physical partition keeps an LSN-ordered change log; LatestVersion shows only the newest
  version of each live document, AllVersionsAndDeletes shows every version incl. deletes,
* change-feed reads resume after an LSN, report per-child positions in an SDK-format composite
  continuation token, round-robin over children after a split, and return a drained page (HTTP
  304) carrying the partition's current LSN,
* queries apply their WHERE clause with the service's semantics (``tests/fake_query.py``), return
  documents in ``_rid`` order and resume after the last ``_rid`` (as the service does), so a
  document that is not modified during a query is returned exactly once, a replaced document keeps
  its place, and a deleted and re-created document comes again at the end,
* a split while a query runs is handled like azure-cosmos 4.17.1 (the range restarts from its
  beginning if it now spans several partitions, or continues on the single child) or, with
  ``query_split="raise"``, like 4.14.0 (HTTP 410/1002); every page names the partitions that served it,
* session tokens (``"<partition id>:-1#<lsn>"``, the format observed on the service) on start
  probes; a query carrying one is served only by a replica at or beyond that LSN (else 404/1002,
  as verified against the service), with the token of a split parent applying to its children,
* fault injection: throttling (429 with retry-after, globally or per method), change-feed replica
  lag (stale reads), query replica lag, splits; a hook runs before every query page (to interleave
  writes with a read), and ``call_log`` records probes and query pages in order.

State can be shared across processes through a JSON file (``FAKE_COSMOS_DIR``) so the same fake
can back a real local SparkSession.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import threading
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from azure.cosmos.partition_key import PartitionKey

from cosmosdb_datasource import epk
from cosmosdb_datasource.backend import QueryPage
from cosmosdb_datasource.changefeed import ChangeFeedPage, decode_positions
from cosmosdb_datasource.documents import PartitionKeyInfo
from cosmosdb_datasource.options import MODE_ALL_VERSIONS_AND_DELETES
from tests.fake_query import compile_query

_REGISTRY: dict[str, FakeCosmosStore] = {}
_LOCK = threading.RLock()


class FakeHttpError(Exception):
    """Looks like azure.cosmos.exceptions.CosmosHttpResponseError to the retry classifier."""

    def __init__(self, status_code: int, headers: dict[str, str] | None = None, sub_status: int | None = None) -> None:
        super().__init__(f"fake HTTP {status_code}" + (f"/{sub_status}" if sub_status is not None else ""))
        self.status_code = status_code
        self.sub_status = sub_status
        self.headers = headers or {}


@dataclass
class Version:
    lsn: int
    op: str  # create | replace | delete
    id: str
    pk: Any
    epk: str
    ts: int
    doc: dict[str, Any] | None
    ttl_expired: bool = False
    rid: int = 0  # a document incarnation's _rid: kept by replaces, new for a create or re-create


@dataclass
class Partition:
    id: str
    lo: str
    hi: str
    lsn: int
    log: list[Version] = field(default_factory=list)
    parents: list[str] = field(default_factory=list)  # ids of split ancestors (session token resolution)


class FakeCosmosStore:
    def __init__(
        self,
        name: str = "fake",
        *,
        pk_paths: tuple[str, ...] = ("/pk",),
        kind: str = "Hash",
        version: int = 2,
        partitions: int = 1,
        start_lsn: int = 0,
        clock: int = 1_700_000_000,
    ) -> None:
        self.name = name
        self.rid = base64.b64encode(uuid.uuid4().bytes[:6]).decode()
        self.pk_def = {"paths": list(pk_paths), "kind": kind, "version": version}
        self._pk = PartitionKey(path=list(pk_paths) if kind == "MultiHash" else pk_paths[0], kind=kind, version=version)
        self._pk_info = PartitionKeyInfo(paths=tuple(pk_paths), kind=kind, version=version)
        bounds = epk.split_range(epk.MIN_EPK, epk.MAX_EPK, partitions, kind=kind, version=version)
        self.partitions = [Partition(str(i), lo, hi, start_lsn) for i, (lo, hi) in enumerate(bounds)]
        self.clock = clock
        self.clock_step = 1  # 0 makes consecutive writes share a _ts second, as on the service
        self.next_partition_id = len(self.partitions)
        self.next_rid = 1
        self.query_split = "restart"  # "restart" (azure-cosmos 4.17.1) or "raise" (4.14.0)
        # Fault injection
        self.throttle_next = 0
        self.throttle_calls: dict[str, int] = {}  # method name -> number of 429s to raise
        self.lag_requests = 0
        self.lag_delta = 0
        self.query_lag_requests = 0  # query pages served by a replica lagging query_lag_delta LSNs
        # change-feed requests for a range that spans a split fail with RecursionError, as azure-cosmos
        # 4.17.1 did when its unbounded 410 handling kept meeting a gone range
        self.gone_recursion_errors = 0
        self.query_lag_delta = 0
        self.request_count = 0
        self.call_log: list[list[Any]] = []
        self.path: str | None = None
        self._before_query_page: Callable[[], None] | None = None  # in-memory only

    def register(self) -> FakeCosmosStore:
        with _LOCK:
            _REGISTRY[self.name] = self
        directory = os.environ.get("FAKE_COSMOS_DIR")
        if directory:
            self.path = os.path.join(directory, f"{self.name}.json")
            self.save()
        return self

    def save(self) -> None:
        if not self.path:
            return
        data = copy.deepcopy({k: v for k, v in self.__dict__.items() if not k.startswith("_")})
        data["partitions"] = [{**p.__dict__, "log": [v.__dict__ for v in p.log]} for p in self.partitions]
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(data, fh)
        os.replace(tmp, self.path)

    @classmethod
    def load(cls, path: str) -> FakeCosmosStore:
        with open(path) as fh:
            data = json.load(fh)
        store = cls(
            data["name"],
            pk_paths=tuple(data["pk_def"]["paths"]),
            kind=data["pk_def"]["kind"],
            version=data["pk_def"]["version"],
        )
        for key, value in data.items():
            if key != "partitions":
                setattr(store, key, value)
        store.partitions = [
            Partition(p["id"], p["lo"], p["hi"], p["lsn"], [Version(**v) for v in p["log"]], p.get("parents", []))
            for p in data["partitions"]
        ]
        return store

    def _epk(self, pk_value: Any) -> str:
        return self._pk._get_epk_range_for_partition_key(pk_value).min

    def _owner(self, key: str) -> Partition:
        for p in self.partitions:
            if p.lo <= key < p.hi:
                return p
        raise AssertionError(f"no partition owns {key}")

    def _latest(self, part: Partition, pk: Any, doc_id: str) -> Version | None:
        for v in reversed(part.log):
            if v.id == doc_id and v.pk == pk:
                return v
        return None

    def upsert(self, doc: dict[str, Any], *, same_lsn_as_previous: bool = False) -> Version:
        with _LOCK:
            pk = self._pk_info.extract(doc)
            key = self._epk(pk)
            part = self._owner(key)
            prev = self._latest(part, pk, doc["id"])
            self.clock += self.clock_step
            if not same_lsn_as_previous:
                part.lsn += 1
            body = dict(doc)
            body.update(
                {
                    "_self": "s",
                    "_etag": f'"{uuid.uuid4()}"',
                    "_attachments": "a/",
                    "_ts": self.clock,
                }
            )
            op = "create" if prev is None or prev.op == "delete" else "replace"
            if op == "create":
                rid, self.next_rid = self.next_rid, self.next_rid + 1
            else:
                rid = prev.rid  # type: ignore[union-attr]
            body["_rid"] = f"r{rid}"
            version = Version(part.lsn, op, doc["id"], pk, key, self.clock, body, rid=rid)
            part.log.append(version)
            self.save()
            return version

    def delete(self, pk: Any, doc_id: str, *, ttl_expired: bool = False, same_lsn_as_previous: bool = False) -> Version:
        with _LOCK:
            key = self._epk(pk)
            part = self._owner(key)
            self.clock += self.clock_step
            if not same_lsn_as_previous:
                part.lsn += 1
            version = Version(part.lsn, "delete", doc_id, pk, key, self.clock, None, ttl_expired)
            part.log.append(version)
            self.save()
            return version

    def bump_lsn(self, n: int = 1) -> None:
        """Advance every partition's LSN without visible changes (e.g. internal writes)."""
        with _LOCK:
            for p in self.partitions:
                p.lsn += n
            self.save()

    def split(self, partition_id: str) -> list[Partition]:
        """Split a physical partition at its EPK midpoint; children inherit LSN and history."""
        with _LOCK:
            parent = next(p for p in self.partitions if p.id == partition_id)
            (a_lo, a_hi), (b_lo, b_hi) = epk.split_range(
                parent.lo, parent.hi, 2, kind=self.pk_def["kind"], version=self.pk_def["version"]
            )
            children = []
            for lo, hi in ((a_lo, a_hi), (b_lo, b_hi)):
                child = Partition(
                    str(self.next_partition_id),
                    lo,
                    hi,
                    parent.lsn,
                    [v for v in parent.log if lo <= v.epk < hi],
                    [parent.id, *parent.parents],
                )
                self.next_partition_id += 1
                children.append(child)
            idx = self.partitions.index(parent)
            self.partitions[idx : idx + 1] = children
            self.save()
            return children

    def write_child_lsn_bump(self, partition_id: str, n: int) -> None:
        with _LOCK:
            next(p for p in self.partitions if p.id == partition_id).lsn += n
            self.save()

    def log_call(self, *entry: Any) -> None:
        with _LOCK:
            self.call_log.append(list(entry))

    def live_documents(self) -> list[dict[str, Any]]:
        out = []
        for p in self.partitions:
            latest: dict[tuple[str, str], Version] = {}
            for v in p.log:
                latest[(json.dumps(v.pk), v.id)] = v
            out.extend(v.doc for v in latest.values() if v.op != "delete" and v.doc is not None)
        return out

    def overlapping(self, lo: str, hi: str) -> list[Partition]:
        return [p for p in self.partitions if p.lo < hi and lo < p.hi]


def session_token(part: Partition, lsn: int) -> str:
    return f"{part.id}:-1#{lsn}"


def _required_lsn(token: str | None, part: Partition) -> int | None:
    """Return the LSN a (compound) session token demands for ``part`` (its own or a split parent's entry)."""
    if not token:
        return None
    by_id = {}
    for entry in token.split(","):
        pid, _, vector = entry.partition(":")
        by_id[pid] = int(vector.split("#")[1])
    for pid in (part.id, *part.parents):
        if pid in by_id:
            return by_id[pid]
    return None


def _encode(rid: str, mode: str, children: list[list[Any]]) -> str:
    def rng(lo: str, hi: str) -> dict[str, Any]:
        return {"min": lo, "max": hi, "isMinInclusive": True, "isMaxInclusive": False}

    state = {
        "v": "v2",
        "containerRid": rid,
        "mode": mode,
        "startFrom": {"Type": "Beginning"},
        "continuation": {
            "v": "v2",
            "rid": rid,
            "continuation": [
                {"token": None if lsn is None else f'"{lsn}"', "range": rng(lo, hi)} for lo, hi, lsn in children
            ],
            "Range": rng(children[0][0], children[-1][1]),
        },
    }
    return base64.b64encode(json.dumps(state).encode()).decode()


class FakeBackend:
    """Implements the reader-facing backend interface on top of a FakeCosmosStore."""

    def __init__(self, store: FakeCosmosStore) -> None:
        self._store = store

    @property
    def store(self) -> FakeCosmosStore:
        if self._store.path:  # cross-process: always see the latest state
            self._store = FakeCosmosStore.load(self._store.path)
            self._store.path = os.path.join(os.environ.get("FAKE_COSMOS_DIR", ""), f"{self._store.name}.json")
        return self._store

    def _request(self, name: str = "") -> FakeCosmosStore:
        store = self.store
        with _LOCK:
            store.request_count += 1
            if store.throttle_next > 0:
                store.throttle_next -= 1
                store.save()
                raise FakeHttpError(429, {"x-ms-retry-after-ms": "5"})
            if store.throttle_calls.get(name, 0) > 0:
                store.throttle_calls[name] -= 1
                store.save()
                raise FakeHttpError(429, {"x-ms-retry-after-ms": "5"})
        return store

    def _visible_lsn(self, store: FakeCosmosStore, part: Partition) -> int:
        if store.lag_requests > 0:
            return max(0, part.lsn - store.lag_delta)
        return part.lsn

    def _consume_lag(self, store: FakeCosmosStore) -> None:
        if store.lag_requests > 0:
            store.lag_requests -= 1
            store.save()

    def properties(self, refresh: bool = False) -> dict[str, Any]:
        store = self._request("properties") if self.store.throttle_calls.get("properties") else self.store
        return {"id": store.name, "_rid": store.rid, "partitionKey": dict(store.pk_def)}

    @property
    def rid(self) -> str:
        return self.store.rid

    def feed_ranges(self, force_refresh: bool = False) -> list[tuple[str, str]]:
        store = self._request("feed_ranges") if self.store.throttle_calls.get("feed_ranges") else self.store
        return [(p.lo, p.hi) for p in store.partitions]

    def query_pages(
        self, query, parameters, rng, page_size, continuation=None, session_token=None
    ) -> Iterator[QueryPage]:
        """Serve ``SELECT *`` pages in ``_rid`` order; the continuation holds the last ``_rid`` returned.

        A range that spans several partitions is read partition by partition, as the SDK does. If the
        partition serving a single-partition range splits between two pages, the read continues on
        the single child that now holds the range, or restarts from the beginning when the range spans
        several children (azure-cosmos 4.17.1); with ``query_split="raise"`` it fails with 410/1002.
        """
        lo, hi = rng
        matches = compile_query(query, parameters)
        after = json.loads(continuation)["rid"] if continuation else 0
        serving: str | None = None  # the partition of a single-partition read
        queue: list[list[Any]] | None = None  # [lo, hi, after] per partition of a multi-partition read
        while True:
            store = self._request("query_pages")
            if store._before_query_page is not None:
                store._before_query_page()
            with _LOCK:
                parts = store.overlapping(lo, hi)
                gone = queue is None and serving is not None and serving not in {p.id for p in parts}
                if gone and store.query_split == "raise":
                    raise FakeHttpError(410, sub_status=1002)
                if queue is None and len(parts) > 1:  # a single-partition continuation cannot be carried over
                    queue = [[max(p.lo, lo), min(p.hi, hi), 0] for p in parts]
                if queue is not None:
                    q_lo, q_hi, q_after = queue[0]
                    part = store.overlapping(q_lo, q_hi)[0]
                else:
                    q_lo, q_hi, q_after = lo, hi, after
                    part = parts[0]
                    serving = part.id
                lagging = store.query_lag_requests > 0
                if lagging:
                    store.query_lag_requests -= 1
                visible = part.lsn - store.query_lag_delta if lagging else part.lsn
                required = _required_lsn(session_token, part)
                if required is not None and visible < required:
                    raise FakeHttpError(404, sub_status=1002)  # READ_SESSION_NOTAVAILABLE
                latest: dict[tuple[str, str], Version] = {}
                for v in part.log:
                    if q_lo <= v.epk < q_hi and v.lsn <= visible:
                        latest[(json.dumps(v.pk), v.id)] = v
                docs = sorted(
                    (v for v in latest.values() if v.op != "delete" and v.rid > q_after and matches(v.doc or {})),
                    key=lambda v: v.rid,
                )
                page = docs[:page_size]
                more = len(docs) > page_size
                store.call_log.append(["query_page", q_lo, q_hi, session_token, len(page)])
                clock = store.clock
            last = page[-1].rid if page else q_after
            if queue is not None:
                if more:
                    queue[0][2] = last
                else:
                    queue.pop(0)
                token = json.dumps({"queue": queue}) if queue else None
                done = not queue
            else:
                after = last
                token = json.dumps({"rid": after}) if more else None
                done = not more
            yield QueryPage(
                [dict(v.doc) for v in page], token, request_charge=2.5, partitions=(part.id,), server_time=float(clock)
            )
            if done:
                return

    def query_partition_pages(
        self, query, parameters, partition_key, page_size, continuation=None
    ) -> Iterator[QueryPage]:
        """Serve the documents of one logical partition (or of a prefix of a hierarchical key), by ``_rid``."""
        matches = compile_query(query, parameters)
        after = json.loads(continuation)["rid"] if continuation else 0
        prefix = isinstance(partition_key, list) and len(partition_key) < len(self.store.pk_def["paths"])
        while True:
            store = self._request("query_partition_pages")
            with _LOCK:
                docs = []
                for part in store.partitions:
                    latest: dict[tuple[str, str], Version] = {}
                    for v in part.log:
                        latest[(json.dumps(v.pk), v.id)] = v
                    for v in latest.values():
                        key_matches = v.pk[: len(partition_key)] == partition_key if prefix else v.pk == partition_key
                        if v.op != "delete" and key_matches and v.rid > after and matches(v.doc or {}):
                            docs.append(v)
                docs.sort(key=lambda v: v.rid)
                page, more = docs[:page_size], len(docs) > page_size
                store.call_log.append(["partition_query_page", json.dumps(partition_key), len(page)])
            if page:
                after = page[-1].rid
            token = json.dumps({"rid": after}) if more else None
            yield QueryPage([dict(v.doc) for v in page], token, request_charge=1.0, partitions=("0",))
            if not more:
                return

    def read_items(self, items) -> tuple[list[dict[str, Any]], float]:
        store = self._request("read_items")
        wanted = {(item_id, json.dumps(key)) for item_id, key in items}
        found = []
        for doc in store.live_documents():
            if (doc["id"], json.dumps(store._pk_info.extract(doc))) in wanted:
                found.append(dict(doc))
        store.log_call("read_items", len(items), len(found))
        return found, float(len(items))

    def key_epk(self, partition_key) -> str:
        return self.store._epk(partition_key)

    def key_range(self, partition_key) -> tuple[str, str]:
        rng = self.store._pk._get_epk_range_for_partition_key(partition_key)
        return rng.min.upper(), rng.max.upper()

    def _entries(
        self, store: FakeCosmosStore, part: Partition, lo: str, hi: str, after: int, mode: str, visible: int
    ) -> list[Version]:
        in_range = [v for v in part.log if lo <= v.epk < hi and v.lsn <= visible]
        if mode == MODE_ALL_VERSIONS_AND_DELETES:
            return [v for v in in_range if v.lsn > after]
        latest: dict[tuple[str, str], Version] = {}
        for v in in_range:
            latest[(json.dumps(v.pk), v.id)] = v
        return sorted((v for v in latest.values() if v.op != "delete" and v.lsn > after), key=lambda v: v.lsn)

    def _item(self, v: Version, mode: str, store: FakeCosmosStore) -> dict[str, Any]:
        if mode != MODE_ALL_VERSIONS_AND_DELETES:
            item = dict(v.doc or {})
            item["_lsn"] = v.lsn
            return item
        meta = {
            "operationType": v.op,
            "lsn": v.lsn,
            "crts": v.ts,
            "previousImageLSN": v.lsn - 1,
            "timeToLiveExpired": v.ttl_expired,
        }
        if v.op == "delete":
            paths = store.pk_def["paths"]
            meta["id"] = v.id
            values = v.pk if len(paths) > 1 else [v.pk]
            meta["partitionKey"] = {p.lstrip("/"): val for p, val in zip(paths, values, strict=True)}
            return {"current": {}, "metadata": meta}  # the service sends an empty "current" for deletes
        return {"current": dict(v.doc or {}), "metadata": meta}

    def change_feed_pages(
        self, rng, mode, start_lsn, page_size, resume_token=None, container_rid=None
    ) -> Iterator[ChangeFeedPage]:
        if resume_token:
            children = [[p.lo, p.hi, p.lsn] for p in decode_positions(resume_token)]
        else:
            children = [[rng[0], rng[1], start_lsn]]
        not_modified_in_a_row = 0
        while True:
            store = self._request()
            lo, hi, after = children[0]
            parts = store.overlapping(lo, hi)
            if len(parts) > 1 and store.gone_recursion_errors > 0:
                store.gone_recursion_errors -= 1
                store.save()
                raise RecursionError("maximum recursion depth exceeded")
            if len(parts) > 1:
                # Split: like azure-cosmos (handle_feed_range_gone) the child tokens get the FULL
                # child partition ranges, not intersected with the token's own range.
                children.pop(0)
                for p in parts:
                    children.append([p.lo, p.hi, after])
                continue
            part = parts[0]
            visible = self._visible_lsn(store, part)
            self._consume_lag(store)
            entries = self._entries(store, part, lo, hi, after or 0, mode, visible)[:page_size]
            if entries:
                children[0][2] = entries[-1].lsn
                children.append(children.pop(0))
                not_modified_in_a_row = 0
                yield ChangeFeedPage(
                    [self._item(v, mode, store) for v in entries],
                    decode_positions(_encode(store.rid, mode, children)),
                    _encode(store.rid, mode, children),
                    request_charge=1.0 * len(entries),
                )
                continue
            children[0][2] = max(after or 0, visible)
            children.append(children.pop(0))
            not_modified_in_a_row += 1
            if not_modified_in_a_row >= len(children):
                tok = _encode(store.rid, mode, children)
                yield ChangeFeedPage([], decode_positions(tok), tok, drained=True, request_charge=1.0)
                return

    def _now(self, rng, name: str) -> tuple[int, str]:
        store = self._request(name)
        parts = store.overlapping(*rng)
        values = [self._visible_lsn(store, p) for p in parts]
        self._consume_lag(store)
        store.log_call(name, rng[0], rng[1], min(values))
        return min(values), ",".join(session_token(p, v) for p, v in zip(parts, values, strict=True))

    def current_lsn(self, rng, mode) -> int:
        return self._now(rng, "current_lsn")[0]

    def resolve_start(self, rng, mode, start) -> tuple[int, str]:
        if start == "Now":
            return self._now(rng, "resolve_start")
        store = self._request("resolve_start")
        lo, hi = rng
        best: Version | None = None
        for p in store.overlapping(lo, hi):
            for v in self._entries(store, p, lo, hi, -1, "LatestVersion", p.lsn):
                if isinstance(start, datetime) and v.ts < start.timestamp():
                    continue
                if best is None or v.lsn < best.lsn:
                    best = v
        parts = store.overlapping(lo, hi)
        tokens = ",".join(session_token(p, p.lsn) for p in parts)
        lsn = best.lsn - 1 if best is not None else min(p.lsn for p in parts)
        store.log_call("resolve_start", lo, hi, lsn)
        return lsn, tokens

    def close(self) -> None:
        return None


def backend_factory(opts: Any) -> FakeBackend:
    """Target of the internal ``_test_backend`` option: ``tests.fake_cosmos:backend_factory``."""
    name = opts.container
    with _LOCK:
        store = _REGISTRY.get(name)
    if store is None:
        directory = os.environ.get("FAKE_COSMOS_DIR")
        if not directory:
            raise KeyError(f"fake container {name!r} is not registered")
        store = FakeCosmosStore.load(os.path.join(directory, f"{name}.json"))
        store.path = os.path.join(directory, f"{name}.json")
    return FakeBackend(store)
