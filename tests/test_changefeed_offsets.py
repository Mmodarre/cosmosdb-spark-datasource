"""Continuation-token codec, the LSN-bounded read loop and the offset model."""

from __future__ import annotations

import base64
import json

import pytest
from azure.cosmos._change_feed.change_feed_state import ChangeFeedState, ChangeFeedStateV2

from cosmosdb_datasource import epk
from cosmosdb_datasource.changefeed import (
    ChangeFeedPage,
    Position,
    ReadStats,
    build_continuation,
    decode_positions,
    parse_etag_lsn,
    read_bounded,
)
from cosmosdb_datasource.errors import CosmosChangeFeedError, CosmosOffsetError
from cosmosdb_datasource.offsets import Offset, RangeState, SnapshotState, min_overlapping_lsn, rekey
from cosmosdb_datasource.retry import RetryPolicy, call_with_retry, is_transient, retry_after_seconds
from cosmosdb_datasource.stream import allocate_budget
from tests.fake_cosmos import FakeHttpError

# -- token codec ---------------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["LatestVersion", "AllVersionsAndDeletes"])
def test_built_token_is_accepted_by_the_sdk(mode: str) -> None:
    token = build_continuation("rid==", mode, "05C1", "FF", 1234)
    state = ChangeFeedState.from_json("dbs/d/colls/c", "rid==", {"continuationFeedRange": token})
    assert isinstance(state, ChangeFeedStateV2)
    assert state._mode == mode
    headers: dict = {}
    state.set_start_from_request_headers(headers)
    state.set_mode_request_headers(headers)
    assert headers["If-None-Match"] == '"1234"'
    # round trip through the SDK's own serialization and our decoder
    sdk_token = base64.b64encode(json.dumps(state.to_dict()).encode()).decode()
    assert decode_positions(sdk_token) == [Position("05C1", "FF", 1234)]


def test_sdk_rejects_token_for_another_container() -> None:
    token = build_continuation("rid==", "LatestVersion", "", "FF", 1)
    with pytest.raises(ValueError, match="Mismatch collection rid"):
        ChangeFeedState.from_json("dbs/d/colls/c", "other==", {"continuationFeedRange": token})


def test_decode_positions_multi_child_and_errors() -> None:
    state = {
        "continuation": {
            "continuation": [
                {"token": '"10"', "range": {"min": "", "max": "80"}},
                {"token": None, "range": {"min": "80", "max": "ff"}},
            ]
        }
    }
    token = base64.b64encode(json.dumps(state).encode()).decode()
    assert decode_positions(token) == [Position("", "80", 10), Position("80", "FF", None)]
    assert decode_positions(None) == []
    with pytest.raises(CosmosChangeFeedError, match="decode"):
        decode_positions("bm90IGpzb24=")
    assert parse_etag_lsn('"42"') == 42 and parse_etag_lsn("42") == 42
    with pytest.raises(CosmosChangeFeedError):
        parse_etag_lsn('"abc"')


# -- bounded read loop ---------------------------------------------------------------------------


def lv(lsn: int, doc_id: str | None = None) -> dict:
    return {"id": doc_id or f"d{lsn}", "_lsn": lsn}


class Script:
    """open_pages() that serves scripted page sequences per call; records resume tokens."""

    def __init__(self, *runs) -> None:
        self.runs = list(runs)
        self.resumes: list[str | None] = []

    def __call__(self, resume):
        self.resumes.append(resume)
        run = self.runs.pop(0)

        def gen():
            for step in run:
                if isinstance(step, Exception):
                    raise step
                yield step

        return gen()


def page(items, *lsns, drained=False, token=None):
    positions = [Position(f"{i:02X}", f"{i + 1:02X}", lsn) for i, lsn in enumerate(lsns)]
    return ChangeFeedPage(items, positions, token or f"tok{lsns}", drained=drained, request_charge=1.0)


def run(script, start=0, end=10, wait=10.0, retries=3):
    stats = ReadStats()
    items = list(
        read_bounded(
            script,
            start_lsn=start,
            end_lsn=end,
            mode="LatestVersion",
            policy=RetryPolicy(max_retries=retries),
            end_wait_seconds=wait,
            stats=stats,
            describe="range test",
        )
    )
    return [i["_lsn"] for i in items], stats


def test_stops_at_end_bound_on_single_range(no_sleep) -> None:
    script = Script([page([lv(1), lv(4)], 4), page([lv(9), lv(11), lv(12)], 12), page([lv(20)], 20)])
    lsns, stats = run(script, end=10)
    assert lsns == [1, 4, 9]
    assert stats.skipped_beyond_end == 2 and stats.pages == 2  # third page never requested


def test_split_mid_read_is_judged_per_child(no_sleep) -> None:
    # child A races past the end bound while child B still has items <= end
    script = Script(
        [
            page([lv(3)], 3),
            page([lv(11), lv(12)], 12, 5),  # A beyond end, B at 5
            page([lv(6), lv(8)], 12, 8),  # B items still in range
            page([lv(13)], 13, 8),  # A again (skipped)
            page([], 13, 15, drained=True),  # B 304 at its current LSN
        ]
    )
    lsns, stats = run(script, end=10)
    assert lsns == [3, 6, 8]
    assert stats.skipped_beyond_end == 3


def test_items_at_or_below_start_are_never_emitted(no_sleep) -> None:
    lsns, _ = run(Script([page([lv(5), lv(6), lv(7)], 7), page([], 12, drained=True)]), start=6, end=10)
    assert lsns == [7]


def test_waits_for_lagging_replica_then_completes(no_sleep) -> None:
    script = Script(
        [page([lv(2)], 2), page([], 6, drained=True)],  # replica only at 6 < end
        [page([lv(9)], 9), page([], 10, drained=True)],  # resumed from last token, caught up
    )
    lsns, stats = run(script, end=10)
    assert lsns == [2, 9]
    assert stats.lag_waits == 1
    assert script.resumes == [None, "tok(6,)"]


def test_lagging_replica_times_out(no_sleep) -> None:
    script = Script(*[[page([], 3, drained=True)] for _ in range(50)])
    with pytest.raises(CosmosChangeFeedError, match="did not reach LSN 10"):
        run(script, end=10, wait=2.0)


def test_transient_errors_resume_from_last_page(no_sleep) -> None:
    throttled = FakeHttpError(429, {"x-ms-retry-after-ms": "250"})
    script = Script(
        [page([lv(1)], 1, token="t1"), throttled],
        [page([lv(2)], 2, token="t2"), FakeHttpError(503)],
        [page([lv(3)], 10, token="t3")],
    )
    lsns, stats = run(script, end=10)
    assert lsns == [1, 2, 3]
    assert script.resumes == [None, "t1", "t2"]
    assert stats.retries == 2
    assert 0.25 <= no_sleep[0] <= 0.5  # honoured retry-after


def test_non_transient_errors_and_retry_budget(no_sleep) -> None:
    with pytest.raises(FakeHttpError):
        run(Script([FakeHttpError(403)]))
    with pytest.raises(FakeHttpError):
        run(Script(*[[FakeHttpError(429)] for _ in range(5)]), retries=2)


def test_item_without_lsn_is_an_error(no_sleep) -> None:
    with pytest.raises(CosmosChangeFeedError, match="_lsn"):
        run(Script([page([{"id": "x"}], 5)]))


def test_retry_classification() -> None:
    assert is_transient(FakeHttpError(429)) and is_transient(FakeHttpError(449)) and is_transient(FakeHttpError(408))
    assert not is_transient(FakeHttpError(404)) and not is_transient(FakeHttpError(401))
    assert is_transient(ConnectionError())
    assert is_transient(FakeHttpError(404, sub_status=1002))  # read session not available (yet)
    assert not is_transient(FakeHttpError(404, sub_status=0)) and not is_transient(FakeHttpError(404))
    assert retry_after_seconds(FakeHttpError(429, {"x-ms-retry-after-ms": "1500"})) == 1.5
    assert retry_after_seconds(FakeHttpError(429)) is None
    policy = RetryPolicy(max_retries=5, base_delay_seconds=1, max_backoff_seconds=8)
    for attempt in range(1, 8):
        assert 0 < policy.delay(attempt) <= 8


# -- offsets -------------------------------------------------------------------------------------


def make_offset(*ranges) -> Offset:
    return Offset("rid", "LatestVersion", tuple(RangeState(lo, hi, lsn) for lo, hi, lsn in ranges))


def test_offset_json_round_trip_is_byte_stable() -> None:
    off = make_offset(("40", "FF", 7), ("", "40", 3))
    text = json.dumps(off.to_dict())
    assert text == '{"v": 1, "rid": "rid", "mode": "LatestVersion", "ranges": {"-40": 3, "40-FF": 7}}'
    again = Offset.from_dict(json.loads(text))
    assert json.dumps(again.to_dict()) == text and again == off


@pytest.mark.parametrize(
    "bad",
    [
        {"v": 2, "rid": "r", "mode": "m", "ranges": {"-FF": 1}},
        {"v": 1, "rid": "r", "mode": "m", "ranges": {}},
        {"v": 1, "rid": "r", "mode": "m", "ranges": {"-80": 1}},
        {"v": 1, "rid": "r", "mode": "m", "ranges": {"-FF": "1"}},
        {"v": 1, "rid": "r", "mode": "m", "ranges": {"bad": 1}},
        {"v": 1, "mode": "m", "ranges": {"-FF": 1}},
        [1, 2],
    ],
)
def test_invalid_offsets(bad) -> None:
    with pytest.raises(CosmosOffsetError):
        Offset.from_dict(bad)


def test_offset_validation_against_container() -> None:
    off = make_offset(("", "FF", 1))
    off.validate_for("rid", "LatestVersion")
    with pytest.raises(CosmosOffsetError, match="re-created"):
        off.validate_for("other", "LatestVersion")
    with pytest.raises(CosmosOffsetError, match="switch modes"):
        off.validate_for("rid", "AllVersionsAndDeletes")


def test_rekey_splits_inherit_lsn_and_never_merge() -> None:
    off = make_offset(("", "40", 3), ("40", "FF", 7))
    split = rekey(off, [("", "20"), ("20", "40"), ("40", "FF")])
    assert split.lsn_bounds() == [("", "20", 3), ("20", "40", 3), ("40", "FF", 7)]
    merged = rekey(off, [("", "FF")])
    assert merged.lsn_bounds() == off.lsn_bounds()
    assert min_overlapping_lsn(split, "10", "30") == 3
    assert min_overlapping_lsn(split, "30", "50") == 3


def test_allocate_budget() -> None:
    assert allocate_budget([0, 0], [10, 30], None) == [10, 30]
    assert allocate_budget([0, 0], [10, 30], 100) == [10, 30]
    assert allocate_budget([0, 0], [10, 30], 20) == [5, 15]
    assert allocate_budget([5, 0], [5, 1000], 10) == [5, 10]
    ends = allocate_budget([0, 0, 0], [1, 1, 1000], 3)
    assert ends[0] == 1 and ends[1] == 1 and ends[2] >= 1  # progress for every lagging range
    assert allocate_budget([9], [3], 10) == [9]  # never move backwards


def test_overlay_maps_split_end_to_start() -> None:
    start = make_offset(("", "40", 3), ("40", "FF", 7))
    end = make_offset(("", "20", 9), ("20", "40", 4), ("40", "FF", 7))
    pieces = epk.overlay([(r.lo, r.hi) for r in start.ranges], [(r.lo, r.hi) for r in end.ranges])
    pairs = [(start.ranges[i].lsn, end.ranges[j].lsn) for _, _, i, j in pieces]
    assert pairs == [(3, 9), (3, 4), (7, 7)]


def test_clip_positions_after_sdk_widened_split() -> None:
    from cosmosdb_datasource.changefeed import clip_positions

    previous = [Position("10", "30", 7)]
    widened = [Position("", "20", 9), Position("20", "40", 7)]  # full child partitions
    assert clip_positions(widened, previous, "10", "30") == [Position("10", "20", 7), Position("20", "30", 7)]
    multi = [Position("10", "18", 4), Position("18", "30", 6)]
    assert clip_positions([Position("", "25", 9), Position("25", "40", 9)], multi, "10", "30") == [
        Position("10", "18", 4),
        Position("18", "25", 6),
        Position("25", "30", 6),
    ]
    with pytest.raises(CosmosChangeFeedError):
        clip_positions([Position("40", "50", 1)], previous, "10", "30")


def test_read_bounded_rescopes_widened_token(no_sleep) -> None:
    rebuilt: list[list[Position]] = []

    def rebuild(children):
        rebuilt.append(children)
        return "clipped"

    widened = ChangeFeedPage([lv(3), lv(4)], [Position("", "20", 4), Position("20", "40", 2)], "wide")
    script = Script(
        [ChangeFeedPage([lv(2)], [Position("10", "30", 2)], "t1"), widened],
        [
            ChangeFeedPage([lv(5)], [Position("20", "30", 2), Position("10", "20", 10)], "t2"),
            ChangeFeedPage([], [Position("10", "20", 10), Position("20", "30", 12)], "t3", drained=True),
        ],
    )
    stats = ReadStats()
    items = list(
        read_bounded(
            script,
            start_lsn=0,
            end_lsn=10,
            mode="LatestVersion",
            policy=RetryPolicy(),
            end_wait_seconds=5,
            stats=stats,
            describe="r",
            lo="10",
            hi="30",
            rebuild_token=rebuild,
        )
    )
    assert [i["_lsn"] for i in items] == [2, 5]  # items of the widened page were discarded
    assert script.resumes == [None, "clipped"]
    assert rebuilt == [[Position("10", "20", 2), Position("20", "30", 2)]]


def test_call_with_retry_is_bounded_and_transient_only(no_sleep) -> None:
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise FakeHttpError(429, {"x-ms-retry-after-ms": "100"})
        return "ok"

    assert call_with_retry(flaky, RetryPolicy(max_retries=5), "probing") == "ok"
    assert len(calls) == 3 and len(no_sleep) == 2 and all(0.1 <= d <= 0.35 for d in no_sleep)
    with pytest.raises(FakeHttpError):
        call_with_retry(lambda: (_ for _ in ()).throw(FakeHttpError(503)), RetryPolicy(max_retries=2), "x")
    assert len(no_sleep) == 4
    with pytest.raises(FakeHttpError):
        call_with_retry(lambda: (_ for _ in ()).throw(FakeHttpError(401)), RetryPolicy(max_retries=5), "x")
    assert len(no_sleep) == 4  # not retried


def snapshot_offset(**kw) -> Offset:
    snap = SnapshotState((("40-FF", "1:-1#7"), ("-40", "0:-1#3")))
    return Offset(
        "rid", "AllVersionsAndDeletes", (RangeState("", "40", 3), RangeState("40", "FF", 7)), snapshot=snap, **kw
    )


def test_snapshot_field_round_trip_is_byte_stable() -> None:
    initial = snapshot_offset(initial=True)
    text = json.dumps(initial.to_dict())
    assert text == (
        '{"v": 1, "rid": "rid", "mode": "AllVersionsAndDeletes", "ranges": {"-40": 3, "40-FF": 7}, '
        '"initial": true, "snapshot": {"session": {"-40": "0:-1#3", "40-FF": "1:-1#7"}}}'
    )
    again = Offset.from_dict(json.loads(text))
    assert again == initial and json.dumps(again.to_dict()) == text
    end = snapshot_offset(init=initial.ranges)
    assert json.loads(json.dumps(end.to_dict()))["snapshot"] == {"session": {"-40": "0:-1#3", "40-FF": "1:-1#7"}}
    assert Offset.from_dict(end.to_dict()) == end
    # an empty token map (no session token reported) still marks the snapshot
    bare = Offset.from_dict({"v": 1, "rid": "r", "mode": "m", "ranges": {"-FF": 1}, "snapshot": {"session": {}}})
    assert bare.snapshot == SnapshotState() and bare.snapshot.session_token_for("", "FF") is None
    # rekey keeps the marker
    assert rekey(initial, [("", "20"), ("20", "40"), ("40", "FF")]).snapshot == initial.snapshot


@pytest.mark.parametrize(
    "bad",
    [
        "pending",
        {"session": "0:-1#3"},
        {"session": {"-FF": 3}},
        {"session": {"bad": "0:-1#3"}},
        {},
    ],
)
def test_invalid_snapshot_field(bad) -> None:
    with pytest.raises(CosmosOffsetError):
        Offset.from_dict({"v": 1, "rid": "r", "mode": "m", "ranges": {"-FF": 1}, "snapshot": bad})


def test_snapshot_session_token_for_overlapping_ranges() -> None:
    snap = SnapshotState((("-40", "0:-1#3"), ("40-80", "1:-1#7"), ("80-FF", "2:-1#9,3:-1#9")))
    assert snap.session_token_for("", "40") == "0:-1#3"
    assert snap.session_token_for("10", "20") == "0:-1#3"  # a child of a split parent gets the parent's token
    assert snap.session_token_for("30", "90") == "0:-1#3,1:-1#7,2:-1#9,3:-1#9"  # merged ranges: compound
    assert snap.session_token_for("40", "80") == "1:-1#7"
