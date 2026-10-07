"""排查视图：容量淘汰、按设备/分类检索、计数一致性与并发写入。"""
import logging
import threading

import pytest

from gateway.models import (
    PayloadIssue,
    RejectCategory,
    RejectReason,
    TelemetryMessage,
)
from gateway.rejection import RejectionView
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0


def test_capacity_eviction_is_fifo_and_counts_stay_consistent():
    view = RejectionView(capacity=3)
    for i in range(5):
        view.record(received_at=T0 + i, device_id="dev-1",
                    category=RejectCategory.MALFORMED_PAYLOAD,
                    reason="missing_field", detail=f"record-{i}",
                    message_id=f"m{i}")
    recs = view.query()
    log.info("容量3写入5条后留存 seq_no=%s | 依据: FIFO 淘汰最旧的 1,2",
             [r.seq_no for r in recs])
    assert len(view) == 3
    assert [r.seq_no for r in recs] == [5, 4, 3]  # 最新在前
    assert [r.detail for r in recs] == ["record-4", "record-3", "record-2"]
    counts = view.counts()
    assert counts == {"malformed_payload": 3}
    # 淘汰不影响外部语义：seq_no 全局单调、不重用
    assert all(r.msg_seq is None for r in recs)


def test_eviction_across_categories_and_devices_keeps_counts_exact():
    view = RejectionView(capacity=4)
    plan = [
        ("d1", RejectCategory.AUTH_FAILURE, "unknown_device"),
        ("d1", RejectCategory.MALFORMED_PAYLOAD, "invalid_structure"),
        ("d2", RejectCategory.MALFORMED_PAYLOAD, "missing_field"),
        ("d2", RejectCategory.STALE_TIMESTAMP, "stale_timestamp"),
        ("d1", RejectCategory.MALFORMED_PAYLOAD, "type_mismatch"),  # 淘汰#1
        ("d2", RejectCategory.AUTH_FAILURE, "device_revoked"),      # 淘汰#2
    ]
    for i, (dev, cat, reason) in enumerate(plan):
        view.record(received_at=T0 + i, device_id=dev, category=cat,
                    reason=reason, detail=f"r{i}")
    assert len(view) == 4
    log.info("混合淘汰后全局计数=%s d1=%s d2=%s | 留存=%s",
             view.counts(), view.counts("d1"), view.counts("d2"),
             [(r.device_id, r.reason) for r in view.query()])
    # 留存为 #3(d2,malformed), #4(d2,stale), #5(d1,malformed), #6(d2,auth)
    assert view.counts() == {"malformed_payload": 2, "stale_timestamp": 1,
                             "auth_failure": 1}
    assert view.counts("d1") == {"malformed_payload": 1}
    assert view.counts("d2") == {"malformed_payload": 1, "stale_timestamp": 1,
                                 "auth_failure": 1}
    assert view.counts("ghost") == {}


def test_query_filters_by_device_category_and_limit():
    view = RejectionView(capacity=100)
    view.record(received_at=T0, device_id="d1",
                category=RejectCategory.AUTH_FAILURE, reason="x", detail="a")
    view.record(received_at=T0 + 1, device_id="d1",
                category=RejectCategory.MALFORMED_PAYLOAD, reason="y", detail="b")
    view.record(received_at=T0 + 2, device_id="d2",
                category=RejectCategory.MALFORMED_PAYLOAD, reason="z", detail="c")
    d1 = view.query(device_id="d1")
    assert [r.detail for r in d1] == ["b", "a"]
    malformed = view.query(category=RejectCategory.MALFORMED_PAYLOAD)
    assert [r.detail for r in malformed] == ["c", "b"]
    limited = view.query(limit=2)
    assert [r.detail for r in limited] == ["c", "b"]
    both = view.query(device_id="d1", category=RejectCategory.AUTH_FAILURE)
    assert [r.detail for r in both] == ["a"]
    log.info("过滤查询: d1=%s malformed=%s limit2=%s | 依据: 设备/分类/条数过滤且最新在前",
             [r.detail for r in d1], [r.detail for r in malformed],
             [r.detail for r in limited])


def test_concurrent_writes_lose_nothing_and_duplicate_nothing():
    view = RejectionView(capacity=10_000)
    n_threads, per_thread = 16, 200
    barrier = threading.Barrier(n_threads)

    def worker(t):
        barrier.wait()
        for i in range(per_thread):
            view.record(received_at=T0 + t, device_id=f"dev-{t % 4}",
                        category=RejectCategory.MALFORMED_PAYLOAD,
                        reason="invalid_value", detail=f"t{t}-i{i}",
                        message_id=f"m-{t}-{i}")

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = n_threads * per_thread
    assert len(view) == total
    seq_nos = [r.seq_no for r in view.query(limit=total)]
    log.info("并发写入 %d 条: 实际留存=%d 唯一seq_no=%d | 依据: 不丢不重",
             total, len(view), len(set(seq_nos)))
    assert len(set(seq_nos)) == total
    assert sum(view.counts().values()) == total
    # 每设备计数精确
    for d in range(4):
        assert view.counts(f"dev-{d}") == {"malformed_payload":
                                           n_threads // 4 * per_thread}


def test_concurrent_writes_at_full_capacity_never_exceed():
    view = RejectionView(capacity=500)
    n_threads, per_thread = 16, 100
    barrier = threading.Barrier(n_threads)

    def worker(t):
        barrier.wait()
        for i in range(per_thread):
            view.record(received_at=float(t * per_thread + i),
                        device_id=f"dev-{t % 8}",
                        category=RejectCategory.STALE_TIMESTAMP,
                        reason="stale_timestamp", detail=f"{t}-{i}")

    threads = [threading.Thread(target=worker, args=(t,))
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    counts_total = sum(view.counts().values())
    log.info("容量500打满并发写: len=%d 计数和=%d | 依据: 恒定不超限",
             len(view), counts_total)
    assert len(view) == 500 and counts_total == 500


@pytest.fixture
def gateway():
    g = Gateway(max_clock_skew_seconds=60.0, rejection_capacity=1000)
    g.registry.register("dev-1", "s1", now=T0)
    g.registry.register("dev-2", "s2", now=T0)
    return g


def test_ingest_rejections_are_recorded_with_raw_snapshot(gateway):
    # 认证失败
    gateway.ingest(TelemetryMessage("ghost", "s", "a1", 1, T0, 1,
                                    {"temp": 1.0}), T0)
    # 负载不合规（不同固件版本的字段问题）
    gateway.ingest(TelemetryMessage("dev-1", "s1", "a2", 1, T0, 1,
                                    {"temp": float("nan")}), T0)
    gateway.ingest(TelemetryMessage("dev-2", "s2", "a3", 1, T0, 99,
                                    {"temp": 1.0}), T0)
    recs = gateway.rejections.query()
    for r in recs:
        log.info("留痕: seq_no=%d device=%s category=%s reason=%s detail=%s raw=%s",
                 r.seq_no, r.device_id, r.category.value, r.reason, r.detail,
                 r.raw)
    assert len(recs) == 3
    reasons = {(r.device_id, r.reason) for r in recs}
    assert ("ghost", "unknown_device") in reasons
    assert ("dev-1", "invalid_value") in reasons
    assert ("dev-2", "unknown_protocol_version") in reasons
    # 原始内容可翻查
    nan_rec = gateway.rejections.query(
        device_id="dev-1", category=RejectCategory.MALFORMED_PAYLOAD)[0]
    assert nan_rec.raw["message_id"] == "a2"
    assert nan_rec.msg_seq == 1
    # 重复/乱序（非拒绝）不进视图
    gateway.ingest(TelemetryMessage("dev-1", "s1", "ok", 1, T0, 1,
                                    {"temp": 1.0}), T0)
    gateway.ingest(TelemetryMessage("dev-1", "s1", "ok", 1, T0, 1,
                                    {"temp": 9.0}), T0)
    assert len(gateway.rejections) == 3


def test_eviction_does_not_touch_twin_or_sequence(gateway):
    g = Gateway(max_clock_skew_seconds=60.0, rejection_capacity=5)
    g.registry.register("dev-1", "s1", now=T0)
    for i in range(20):
        g.ingest(TelemetryMessage("dev-1", "s1", f"bad{i}", 1, T0, 1,
                                  "not-a-dict"), T0)
    assert len(g.rejections) == 5
    twin = g.get_twin("dev-1")
    log.info("20 条拒绝触发淘汰后: 视图=%d reported=%r version=%d watermark=%d | "
             "依据: 淘汰不影响孪生与顺序",
             len(g.rejections), twin.reported, twin.reported_version,
             g.telemetry.applied_seq("dev-1"))
    assert twin.reported == {} and twin.reported_version == 0
    assert g.telemetry.applied_seq("dev-1") == 0
