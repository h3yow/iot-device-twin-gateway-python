"""孪生：版本化收敛、乐观并发冲突、多设备并发隔离。"""
import logging
import threading

import pytest

from gateway.models import ConflictPolicy
from gateway.twin import TwinStore, VersionConflict

log = logging.getLogger("test.twin")


def test_lww_conflict_is_deterministic():
    store = TwinStore(policy=ConflictPolicy.LAST_WRITER_WINS)
    store.update_desired("d1", {"mode": "auto"}, base_version=0, writer_id="ops", now=1.0)
    # 两个写者都基于过期版本 0 并发写入：每次与当前持有者按 writer_id 字典序仲裁，
    # 大者胜；最终收敛到字典序最大的写者，与到达顺序无关
    r_a = store.update_desired("d1", {"mode": "a"}, base_version=0, writer_id="writer-a", now=2.0)
    r_b = store.update_desired("d1", {"mode": "b"}, base_version=0, writer_id="writer-b", now=3.0)
    log.info("输入: writer-a/writer-b 均基于 v0 冲突写; 判定依据: LWW 字典序 -> a=%s b=%s",
             r_a.reason, r_b.reason)
    assert r_a.applied and r_b.applied  # 各自战胜了当时的持有者
    assert store.get("d1").desired.value == {"mode": "b"}  # 收敛到最大 writer_id
    assert store.get("d1").desired.version == 3

    # 交换到达顺序，结论必须一致（确定性）
    store2 = TwinStore(policy=ConflictPolicy.LAST_WRITER_WINS)
    store2.update_desired("d1", {"mode": "auto"}, base_version=0, writer_id="ops", now=1.0)
    store2.update_desired("d1", {"mode": "b"}, base_version=0, writer_id="writer-b", now=2.0)
    store2.update_desired("d1", {"mode": "a"}, base_version=0, writer_id="writer-a", now=3.0)
    log.info("输入: 交换到达顺序; 判定依据: 结论应与上一致 -> %s",
             store2.get("d1").desired.value)
    assert store2.get("d1").desired.value == {"mode": "b"}


def test_reject_on_conflict_policy():
    store = TwinStore(policy=ConflictPolicy.REJECT_ON_CONFLICT)
    store.update_desired("d1", {"x": 1}, base_version=0, writer_id="w1", now=1.0)
    log.info("输入: base_version=0 但当前 v1; 判定依据: REJECT_ON_CONFLICT 应抛 VersionConflict")
    with pytest.raises(VersionConflict) as exc:
        store.update_desired("d1", {"x": 2}, base_version=0, writer_id="w2", now=2.0)
    assert exc.value.current_version == 1
    assert store.get("d1").desired.value == {"x": 1}  # 状态未被污染


def test_reported_never_rolls_back():
    store = TwinStore()
    assert store.apply_reported("d1", {"t": 30}, seq=3, now=1.0)
    assert store.apply_reported("d1", {"t": 20}, seq=2, now=2.0) is False  # 乱序旧版本
    assert store.apply_reported("d1", {"t": 99}, seq=3, now=3.0) is False  # 重复版本
    log.info("输入: seq=2/3 在 seq=3 之后到达; 判定依据: 低版本忽略, reported 保持 t=30")
    assert store.get("d1").reported.value == {"t": 30}
    assert store.get("d1").reported.version == 3


def test_concurrent_same_device_no_lost_update():
    store = TwinStore()
    threads_n, attempts = 16, 200
    wins = []

    def worker(i):
        for _ in range(attempts):
            cur = store.get("d").desired.version
            r = store.update_desired("d", {"n": i}, base_version=cur,
                                     writer_id=f"w{i:03d}", now=0.0)
            if r.applied:
                wins.append(i)
                return
        raise AssertionError("worker never won")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: %d 线程乐观并发写同一设备; 判定依据: 每次成功版本+1, 终值=%d, 赢家=%d",
             threads_n, store.get("d").desired.version, len(wins))
    assert store.get("d").desired.version == threads_n  # 无丢失更新
    assert len(wins) == threads_n


def test_concurrent_multi_device_isolated():
    store = TwinStore()
    devices = [f"dev-{i}" for i in range(8)]

    def worker(dev):
        for seq in range(1, 51):
            store.apply_reported(dev, {"seq": seq}, seq=seq, now=float(seq))

    threads = [threading.Thread(target=worker, args=(d,)) for d in devices]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for d in devices:
        twin = store.get(d)
        log.info("判定依据: %s reported.version=%d value=%s",
                 d, twin.reported.version, twin.reported.value)
        assert twin.reported.version == 50
        assert twin.reported.value == {"seq": 50}  # 互不污染
