"""Local RU budget (target_ru_per_second) and throughput buckets."""

from __future__ import annotations

import errno
import json
import multiprocessing
import os
import stat
import threading
import time
import types

import pytest

from cosmosdb_datasource import client, throughput
from cosmosdb_datasource.errors import CosmosConfigError
from cosmosdb_datasource.throughput import RuBudget, SharedBucket, ThrottledBackend
from tests.conftest import fake_options
from tests.harness import make_source, run_batch


def test_bucket_paces_to_the_rate(tmp_path) -> None:
    bucket = SharedBucket(RuBudget("r1", 200.0), str(tmp_path))
    started = time.monotonic()
    for _ in range(6):
        bucket.consume(100.0)  # 600 RU at 200 RU/s, 200 RU of burst
    elapsed = time.monotonic() - started
    assert elapsed > 1.8  # no upper bound: a loaded machine may take longer
    bucket.leave()


def test_tasks_share_one_bucket(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    waits: list[float] = []
    lock = threading.Lock()

    def record(seconds: float) -> None:
        with lock:
            waits.append(seconds)
        time.sleep(seconds)

    monkeypatch.setattr(throughput, "sleep", record)
    budget = RuBudget("r2", 100.0)  # two tasks of 100 RU/s: 200 RU/s together

    def task() -> None:
        bucket = SharedBucket(budget, str(tmp_path))
        for _ in range(4):
            bucket.consume(100.0)
        bucket.leave()

    started = time.monotonic()
    threads = [threading.Thread(target=task) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started
    assert elapsed > 2.6  # 800 RU at 200 RU/s, less 200 RU of burst: never faster than the rate
    assert waits


def _process_task(directory: str, out) -> None:
    bucket = SharedBucket(RuBudget("r3", 100.0), directory)
    for _ in range(3):
        bucket.consume(100.0)
    bucket.leave()
    out.put(bucket.waited)


def test_processes_share_one_bucket(tmp_path) -> None:
    ctx = multiprocessing.get_context("spawn")
    out = ctx.Queue()
    started = time.monotonic()
    procs = [ctx.Process(target=_process_task, args=(str(tmp_path), out)) for _ in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(30)
    waited = [out.get(timeout=5) for _ in procs]
    elapsed = time.monotonic() - started
    assert all(p.exitcode == 0 for p in procs)
    assert sum(waited) > 1.0 and elapsed > 1.8  # 600 RU at 200 RU/s together, less the burst


def test_stale_members_are_dropped(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    gone = SharedBucket(RuBudget("r4", 1000.0), str(tmp_path))
    gone.consume(0.0)  # a task that died without leaving
    clock = [time.monotonic()]
    monkeypatch.setattr(throughput.time, "monotonic", lambda: clock[0])
    clock[0] += throughput.STALE_SECONDS + 1
    waits: list[float] = []
    monkeypatch.setattr(throughput, "sleep", waits.append)
    alive = SharedBucket(RuBudget("r4", 10.0), str(tmp_path))
    alive.consume(30.0)
    assert waits and waits[0] == pytest.approx(2.0)  # (30 - 10 of burst) / 10 RU/s: the dead share is gone


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch, tmp_path) -> list[float]:
    """Sleeping advances a fake monotonic clock; returns the list of sleeps."""
    monkeypatch.setattr(throughput.tempfile, "gettempdir", lambda: str(tmp_path))
    now = [1000.0]
    waits: list[float] = []

    def sleep(seconds: float) -> None:
        waits.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(throughput.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(throughput, "sleep", sleep)
    return waits


def test_reads_are_paced_by_target_ru(make_store, fake_clock: list[float]) -> None:
    waits = fake_clock
    store = make_store(partitions=2)
    for i in range(400):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 13}"})
    rows, parts = run_batch(fake_options(store, page_size="10", target_ru_per_second="20"))
    assert len(rows) == 400
    assert {p.ru_budget.ru_per_second for p in parts} == {10.0}
    pages = sum(1 for e in store.call_log if e[0] == "query_page")
    charged = pages * 2.5  # the fake charges 2.5 RU per query page
    assert sum(waits) == pytest.approx((charged - 2 * 10.0) / 10.0, rel=0.25)  # each task: its share, less a burst
    _rows, parts = run_batch(fake_options(store))
    assert not hasattr(parts[0], "ru_budget")


def test_change_feed_reads_are_paced(make_store, fake_clock: list[float]) -> None:
    from tests.harness import StreamHarness

    waits = fake_clock
    store = make_store(partitions=1)
    for i in range(50):
        store.upsert({"id": f"d{i}", "pk": f"p{i % 5}"})
    h = StreamHarness(fake_options(store, change_feed_mode="LatestVersion", page_size="5", target_ru_per_second="2"))
    h.start()
    assert len(h.run_until_idle()) == 50
    assert sum(waits) > 10


def test_options_and_client_settings(make_store) -> None:
    store = make_store()
    opts = make_source(fake_options(store, throughput_bucket="3", target_ru_per_second="500"))._opts
    assert (opts.throughput_bucket, opts.target_ru_per_second) == (3, 500.0)
    assert client._client_kwargs(opts)["throughput_bucket"] == 3
    assert "throughput_bucket" not in client._client_kwargs(make_source(fake_options(store))._opts)
    with pytest.raises(CosmosConfigError, match="between 1 and 5"):
        make_source(fake_options(store, throughput_bucket="6"))
    with pytest.raises(CosmosConfigError, match="greater than 0"):
        make_source(fake_options(store, target_ru_per_second="0"))


def test_a_long_sleep_keeps_the_share(fake_clock: list[float], monkeypatch: pytest.MonkeyPatch) -> None:
    clock = [5000.0]
    monkeypatch.setattr(throughput.time, "monotonic", lambda: clock[0])
    waits: list[float] = []
    monkeypatch.setattr(throughput, "sleep", waits.append)
    a = SharedBucket(RuBudget("r5", 1.0))
    b = SharedBucket(RuBudget("r5", 1.0))
    a.consume(0.0)
    b.consume(100.0)  # 2 RU/s together: 49.5 s of debt, longer than STALE_SECONDS
    assert waits == [pytest.approx(49.5)]
    clock[0] += 40.0
    a.consume(0.0)
    assert waits[1] == pytest.approx(9.5)  # repaid at 2 RU/s: b still counts while it sleeps


def test_the_state_outlives_its_members_until_it_is_stale(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    bucket = SharedBucket(RuBudget("r6", 1000.0), str(tmp_path))
    other = SharedBucket(RuBudget("r6", 1000.0), str(tmp_path))
    bucket.consume(1.0)
    other.consume(1.0)
    bucket.leave()
    other.leave()
    state = tmp_path / "r6.json"
    assert state.exists()  # a task that joins later continues from it
    SharedBucket(RuBudget("r7", 1.0), str(tmp_path))
    assert state.exists()  # written less than STALE_SECONDS ago
    old = time.time() - throughput.STALE_SECONDS - 1
    os.utime(state, (old, old))
    (tmp_path / ".r6.123.tmp").write_text("{")  # left by a crashed writer
    os.utime(tmp_path / ".r6.123.tmp", (old, old))
    SharedBucket(RuBudget("r7", 1.0), str(tmp_path))
    assert sorted(p.name for p in tmp_path.iterdir()) == [".lock"]


def test_a_stale_file_with_a_sleeping_member_is_kept(tmp_path) -> None:
    sleeper = SharedBucket(RuBudget("r8", 1.0), str(tmp_path))
    with sleeper._state() as state:  # a member in a long sleep: seen until its sleep ends
        state.update(tokens=-500.0, updated=time.monotonic(), members={"x": [1.0, time.monotonic() + 500]})
    old = time.time() - throughput.STALE_SECONDS - 1
    os.utime(tmp_path / "r8.json", (old, old))
    SharedBucket(RuBudget("r9", 1.0), str(tmp_path))
    assert (tmp_path / "r8.json").exists()


class _ChargingBackend:
    """Point reads and pages of 90 RU that each take 50 ms on the fake clock."""

    def __init__(self, clock: list[float]) -> None:
        self.clock = clock

    def read_items(self, items):
        self.clock[0] += 0.05
        return [], 90.0

    def query_pages(self, *args, **kwargs):
        for _ in range(10):
            self.clock[0] += 0.05
            yield types.SimpleNamespace(request_charge=90.0)


def test_one_task_charges_one_bucket_for_its_whole_read(fake_clock: list[float], monkeypatch) -> None:
    clock = [1000.0]
    waits = fake_clock
    monkeypatch.setattr(throughput.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(throughput, "sleep", lambda s: (waits.append(s), clock.__setitem__(0, clock[0] + s)))
    task = ThrottledBackend(_ChargingBackend(clock), RuBudget("r10", 100.0))
    started = clock[0]
    for _ in range(50):  # 50 point-read chunks: 4,500 RU at 100 RU/s
        task.read_items([("a", "p")])
    for _ in range(3):  # resumed page iterators of the same task: 2,700 RU more
        list(task.query_pages())
    task.leave()
    assert clock[0] - started >= (7200 - 100) / 100  # one burst of one second for the whole task


def test_item_ids_reads_are_paced(make_store, fake_clock: list[float]) -> None:
    waits = fake_clock
    store = make_store(partitions=1)
    for i in range(5000):
        store.upsert({"id": f"d{i}", "pk": "p"})
    ids = [[f"d{i}", "p"] for i in range(5000)]
    rows, parts = run_batch(fake_options(store, item_ids=json.dumps(ids), target_ru_per_second="500"))
    assert len(rows) == 5000 and parts[0].ru_budget.ru_per_second == 500.0
    assert sum(waits) >= 9.0  # 5 chunks of 1,000 RU at 500 RU/s, less one second of burst


def test_damaged_state_is_reset(tmp_path) -> None:
    (tmp_path / "r11.json").write_text(json.dumps({"tokens": "lots", "updated": 1, "members": []}))
    bucket = SharedBucket(RuBudget("r11", 1000.0), str(tmp_path))
    bucket.consume(1.0)
    state = json.loads((tmp_path / "r11.json").read_text())
    assert state["tokens"] == pytest.approx(999.0) and list(state["members"]) == [bucket._member]


def test_the_default_directory_is_private_to_the_user(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(throughput.tempfile, "gettempdir", lambda: str(tmp_path))
    SharedBucket(RuBudget("r12", 10.0)).consume(1.0)
    (root,) = tmp_path.iterdir()
    assert root.name == f"cosmosdb-ru-budget-{os.getuid()}"
    assert stat.S_IMODE(root.stat().st_mode) & 0o077 == 0
    assert (root / "r12.json").exists()


def test_without_a_private_directory_the_bucket_is_per_process(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setattr(throughput.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(throughput, "_warned_local", False)
    shared = tmp_path / f"cosmosdb-ru-budget-{os.getuid()}"
    shared.mkdir(mode=0o777)
    shared.chmod(0o777)  # writable by others: not used
    with caplog.at_level("WARNING", logger="cosmosdb_datasource"):
        a = SharedBucket(RuBudget("r13", 100.0))
        b = SharedBucket(RuBudget("r13", 100.0))
    a.consume(1.0)
    b.consume(1.0)
    assert len(throughput._LOCAL_STATES["r13"]["members"]) == 2  # the members in this process share one bucket
    assert list(shared.iterdir()) == []
    assert sum("shared only by its tasks in this Python worker" in r.getMessage() for r in caplog.records) == 1


def test_a_lock_that_cannot_be_taken_falls_back_to_the_process(tmp_path, monkeypatch, caplog) -> None:
    monkeypatch.setattr(throughput, "_warned_local", False)

    def no_locks(fd, operation):
        raise OSError(errno.ENOLCK, "No locks available")

    monkeypatch.setattr(throughput.fcntl, "flock", no_locks)
    with caplog.at_level("WARNING", logger="cosmosdb_datasource"):
        bucket = SharedBucket(RuBudget("r14", 10.0), str(tmp_path))
        bucket.consume(5.0)
        bucket.consume(5.0)
    assert bucket.waited == 0 and throughput._LOCAL_STATES["r14"]["tokens"] == pytest.approx(0.0, abs=0.1)
    assert any("cannot be locked" in r.getMessage() for r in caplog.records)
