"""排查视图：按设备/分类查询、容量淘汰、并发写入计数不丢不重。"""
import logging
import threading

import pytest

from gateway.models import RejectionCategory, TelemetryMessage
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0
CAPACITY = 50


@pytest.fixture
def gateway():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=60.0,
                rejection_capacity=CAPACITY)
    g.registry.register("dev-1", "s1", now=T0)
    g.registry.register("dev-2", "s2", now=T0)
    return g


def msg(mid, seq, device="dev-1", secret="s1", ts=T0, payload=None):
    return TelemetryMessage(device, secret, mid, seq, ts, 1,
                            {"temp": 20.0} if payload is None else payload)


def test_query_by_device_and_category(gateway):
    gateway.ingest(msg("x1", 1, secret="bad"), T0)          # auth 失败
    gateway.ingest(msg("x2", 1, payload="junk"), T0)        # 负载不合规
    gateway.ingest(msg("x3", 1, device="dev-2", secret="s2",
                       payload={}), T0)                     # dev-2 不合规
    gateway.ingest(msg("ok1", 1), T0)                       # 正常接受，不留痕
    gateway.ingest(msg("ok1", 1), T0)                       # 重复
    records = gateway.rejections.query()
    for r in records:
        log.info("视图记录: device=%s category=%s 依据=%s mid=%s",
                 r.device_id, r.category.value, r.detail, r.message_id)
    assert len(records) == 4                                # 正常上报不留痕
    dev1 = gateway.rejections.query(device_id="dev-1")
    assert {r.message_id for r in dev1} == {"x1", "x2", "ok1"}
    non_compliant = gateway.rejections.query(
        category=RejectionCategory.PAYLOAD_NON_COMPLIANT)
    assert {r.message_id for r in non_compliant} == {"x2", "x3"}
    dev2_bad = gateway.rejections.query(
        device_id="dev-2",
        category=RejectionCategory.PAYLOAD_NON_COMPLIANT)
    assert [r.message_id for r in dev2_bad] == ["x3"]
    assert dev2_bad[0].raw is not None                      # 原始内容可查
    auth = gateway.rejections.query(category=RejectionCategory.AUTH_FAILURE)
    assert [r.message_id for r in auth] == ["x1"]
    assert auth[0].detail == "unknown_credential"           # 判定依据可查


def test_capacity_evicts_oldest_fifo(gateway):
    for i in range(CAPACITY + 10):
        gateway.ingest(msg(f"bad-{i}", 1, secret="bad"), T0)
    stats = gateway.rejections.stats()
    records = gateway.rejections.query()
    log.info("判定依据: 容量 %d 装满后 FIFO 淘汰最旧 | stats=%s 最旧留存=%s",
             CAPACITY, stats, records[-1].message_id)
    assert stats["size"] == CAPACITY
    assert stats["total_seen"] == CAPACITY + 10             # 计数不随淘汰回退
    assert stats["total_evicted"] == 10
    assert records[-1].message_id == "bad-10"               # 最旧的 0..9 已淘汰
    assert records[0].message_id == f"bad-{CAPACITY + 9}"   # 最新在前


def test_eviction_does_not_affect_twin_or_sequence(gateway):
    gateway.ingest(msg("ok1", 1, payload={"temp": 21.0}), T0)
    for i in range(CAPACITY * 2):
        gateway.ingest(msg(f"bad-{i}", 2, secret="bad"), T0)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 淘汰只影响视图 | reported=%s version=%d watermark=%d",
             twin.reported, twin.reported_version,
             gateway.telemetry.applied_seq("dev-1"))
    assert twin.reported == {"temp": 21.0} and twin.reported_version == 1
    assert gateway.telemetry.applied_seq("dev-1") == 1
    # 视图淘汰后正常上报链路不受影响
    r = gateway.ingest(msg("ok2", 2, payload={"temp": 22.0}), T0)
    assert r.accepted
    assert gateway.get_twin("dev-1").reported == {"temp": 22.0}


def test_concurrent_writes_counts_exact(gateway):
    threads_n, per_thread = 8, 25
    barrier = threading.Barrier(threads_n)

    def worker(dev, secret, prefix):
        barrier.wait()
        for i in range(per_thread):
            gateway.ingest(msg(f"{prefix}-{i}", 1, device=dev, secret=secret),
                           T0)

    threads = [
        threading.Thread(target=worker,
                         args=(f"dev-{(t % 2) + 1}", "bad", f"t{t}"))
        for t in range(threads_n)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = threads_n * per_thread
    stats = gateway.rejections.stats()
    totals = gateway.rejections.category_totals()
    log.info("判定依据: %d 线程并发写视图 | stats=%s 分类计数=%s",
             threads_n, stats, totals)
    assert stats["total_seen"] == total                     # 计数不丢不重
    assert stats["size"] == CAPACITY
    assert stats["total_evicted"] == total - CAPACITY
    assert totals[RejectionCategory.AUTH_FAILURE] == total
    # 留存的记录本身无重复
    records = gateway.rejections.query()
    assert len({r.message_id for r in records}) == len(records)


def test_queries_do_not_block_ingest(gateway):
    stop = threading.Event()
    query_count = [0]

    def reader():
        while not stop.is_set():
            gateway.rejections.query(device_id="dev-1")
            gateway.rejections.stats()
            query_count[0] += 1

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    accepted = 0
    for i in range(200):
        r = gateway.ingest(msg(f"m{i}", i + 1), T0)
        accepted += r.accepted
        gateway.ingest(msg(f"rej-{i}", 1, secret="bad"), T0)
    stop.set()
    for t in readers:
        t.join()
    log.info("判定依据: 查询与上报并发 | accepted=%d/200 查询次数=%d",
             accepted, query_count[0])
    assert accepted == 200
    assert query_count[0] > 0
    assert gateway.get_twin("dev-1").reported_version == 200
