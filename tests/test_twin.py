"""设备孪生：版本化收敛、乐观并发冲突、多设备并发隔离。"""
import logging
import threading

import pytest

from gateway.models import ConflictPolicy, TelemetryMessage
from gateway.service import Gateway
from gateway.twin import TwinStore

log = logging.getLogger(__name__)

T0 = 1_000.0


def test_optimistic_concurrency_conflict_is_rejected():
    store = TwinStore(ConflictPolicy.REJECT)
    r1 = store.set_desired("dev-1", {"mode": "auto"}, expected_version=0)
    # 两个并发写都基于 version=1，只有一个能成功
    r2 = store.set_desired("dev-1", {"mode": "manual"}, expected_version=1)
    r3 = store.set_desired("dev-1", {"mode": "off"}, expected_version=1)
    log.info("输入: 两次基于 version=1 的写 | 判定: ok=%s/%s conflict=%s/%s | "
             "依据: REJECT 策略下后到写必须明确拒绝",
             r2.ok, r3.ok, r2.conflict, r3.conflict)
    assert r1.ok and r1.version == 1
    assert r2.ok and r2.version == 2
    assert not r3.ok and r3.conflict and r3.version == 2
    assert store.get("dev-1").desired == {"mode": "manual"}


def test_last_writer_wins_converges_deterministically():
    store = TwinStore(ConflictPolicy.LAST_WRITER_WINS)
    first = store.set_desired("dev-1", {"v": 1}, expected_version=1)
    ahead = store.set_desired("dev-1", {"v": 2}, expected_version=10)
    stale = store.set_desired("dev-1", {"v": 0}, expected_version=3)
    log.info("输入: 版本 1 -> 10 -> 3 的写 | 判定: ok=%s/%s/%s | "
             "依据: LWW 只接受严格更大的目标版本, 与到达顺序无关",
             first.ok, ahead.ok, stale.ok)
    assert first.ok and ahead.ok
    assert not stale.ok and stale.conflict
    twin = store.get("dev-1")
    assert twin.desired == {"v": 2} and twin.desired_version == 10


def test_concurrent_reported_updates_are_not_lost():
    store = TwinStore()
    barrier = threading.Barrier(50)

    def worker(i):
        barrier.wait()
        store.apply_reported("dev-1", {f"sensor-{i}": i})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    twin = store.get("dev-1")
    log.info("输入: 50 线程并发 apply_reported | 判定: version=%d keys=%d | "
             "依据: 锁内合并, 不得丢失更新", twin.reported_version, len(twin.reported))
    assert twin.reported_version == 50
    assert len(twin.reported) == 50


def test_concurrent_devices_do_not_pollute_each_other():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=60.0)
    for i in range(10):
        g.registry.register(f"dev-{i}", f"s{i}", now=T0)
    barrier = threading.Barrier(10)
    errors = []

    def worker(i):
        barrier.wait()
        for seq in range(1, 21):
            msg = TelemetryMessage(f"dev-{i}", f"s{i}", f"m{i}-{seq}", seq,
                                   T0, 1, {"value": i * 1000 + seq})
            r = g.ingest(msg, T0)
            if not r.accepted:
                errors.append((i, seq, r))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: 10 设备 x 20 条按序遥测并发上报 | 错误数=%d", len(errors))
    assert not errors
    for i in range(10):
        twin = g.get_twin(f"dev-{i}")
        expected = {"value": i * 1000 + 20}
        log.info("dev-%d: reported=%s version=%d | 依据: 每设备独立水位与孪生",
                 i, twin.reported, twin.reported_version)
        assert twin.reported == expected
        assert twin.reported_version == 20


def test_desired_and_reported_versions_are_independent():
    store = TwinStore()
    store.set_desired("dev-1", {"a": 1}, expected_version=0)
    store.apply_reported("dev-1", {"a": 1})
    store.apply_reported("dev-1", {"b": 2})
    twin = store.get("dev-1")
    log.info("判定依据: desired_version 与 reported_version 各自单调 | "
             "desired=%d reported=%d", twin.desired_version, twin.reported_version)
    assert twin.desired_version == 1
    assert twin.reported_version == 2
