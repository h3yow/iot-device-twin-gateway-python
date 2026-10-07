"""批量补传：正序/乱序/重试下的收敛一致性与逐条结论。"""
import logging
import random

import pytest

from gateway.models import MessageFlag
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0


@pytest.fixture
def gateway():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=60.0)
    g.registry.register("dev-1", "s1", now=T0)
    g.registry.register("dev-2", "s2", now=T0)
    return g


def raw(mid, seq, device="dev-1", secret="s1", temp=20.0):
    return {"device_id": device, "secret": secret, "message_id": mid,
            "seq": seq, "timestamp": T0, "protocol_version": 1,
            "payload": {"temp": temp, "seq": seq}}


def log_batch(batch, raws):
    for item in batch.items:
        r = item.result
        log.info("批内条目: index=%d mid=%s seq=%s | 命中规则: %s | 结论: "
                 "accepted=%s %s", item.index,
                 raws[item.index].get("message_id"),
                 raws[item.index].get("seq"), r.detail or "-", r.accepted,
                 r.reject_reason.value if r.reject_reason else
                 [f.value for f in r.flags])


def test_in_order_batch_converges(gateway):
    raws = [raw(f"m{i}", i, temp=20.0 + i) for i in range(1, 6)]
    batch = gateway.ingest_batch("b1", raws, T0)
    log_batch(batch, raws)
    assert all(it.result.accepted for it in batch.items)
    assert [it.result.flags for it in batch.items] == \
        [(MessageFlag.ACCEPTED,)] * 5
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 正序批全部按序应用 | reported=%s version=%d",
             twin.reported, twin.reported_version)
    assert twin.reported == {"temp": 25.0, "seq": 5}
    assert twin.reported_version == 5
    assert gateway.telemetry.applied_seq("dev-1") == 5


def test_shuffled_batch_converges_to_same_state(gateway):
    base = [raw(f"m{i}", i, temp=20.0 + i) for i in range(1, 6)]
    gateway.ingest_batch("b-in-order", base, T0)
    expected = gateway.get_twin("dev-1").reported
    expected_version = gateway.get_twin("dev-1").reported_version

    # 同一批数据的任意排列，孪生收敛结果一致
    for seed in range(5):
        g = Gateway(max_clock_skew_seconds=60.0)
        g.registry.register("dev-1", "s1", now=T0)
        shuffled = list(base)
        random.Random(seed).shuffle(shuffled)
        batch = g.ingest_batch(f"b-shuffled-{seed}", shuffled, T0)
        log.info("排列 seed=%d 顺序=%s", seed,
                 [r["seq"] for r in shuffled])
        log_batch(batch, shuffled)
        twin = g.get_twin("dev-1")
        log.info("判定依据: 批内排列无关收敛 | reported=%s version=%d",
                 twin.reported, twin.reported_version)
        assert twin.reported == expected
        assert twin.reported_version == expected_version
        assert g.telemetry.applied_seq("dev-1") == 5
        assert all(it.result.accepted for it in batch.items)


def test_batch_retry_applies_exactly_once(gateway):
    raws = [raw(f"m{i}", i, temp=20.0 + i) for i in range(1, 4)]
    first = gateway.ingest_batch("b-retry", raws, T0)
    twin_after_first = gateway.get_twin("dev-1")
    retry = gateway.ingest_batch("b-retry", raws, T0)
    log.info("重试同一 batch_id | duplicate_batch=%s", retry.duplicate_batch)
    twin_after_retry = gateway.get_twin("dev-1")
    log.info("判定依据: 重试返回缓存结论且不重复生效 | version %d -> %d",
             twin_after_first.reported_version,
             twin_after_retry.reported_version)
    assert retry.duplicate_batch is True
    assert first.duplicate_batch is False
    assert [it.result for it in retry.items] == \
        [it.result for it in first.items]
    assert twin_after_retry.reported_version == 3   # 不重复计数
    assert twin_after_retry.reported == twin_after_first.reported
    assert gateway.telemetry.applied_seq("dev-1") == 3


def test_batch_internal_duplicate_and_gap_flagged(gateway):
    raws = [
        raw("m1", 1, temp=21.0),
        raw("m1", 1, temp=99.0),   # 批内重复：同 message_id
        raw("m4", 4, temp=24.0),   # 缺号：跳过 2、3
    ]
    batch = gateway.ingest_batch("b-dup-gap", raws, T0)
    log_batch(batch, raws)
    assert batch.items[0].result.flags == (MessageFlag.ACCEPTED,)
    assert batch.items[1].result.flags == (MessageFlag.DUPLICATE,)
    assert batch.items[2].result.flags == (MessageFlag.GAP,)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 批内重复不重复计数、缺号标记且确定性前进 | "
             "reported=%s version=%d", twin.reported, twin.reported_version)
    assert twin.reported == {"temp": 24.0, "seq": 4}
    assert twin.reported_version == 2          # 重复条不计数
    assert gateway.telemetry.applied_seq("dev-1") == 4


def test_batch_out_of_order_against_watermark_does_not_regress(gateway):
    gateway.ingest_batch("b-first", [raw(f"m{i}", i, temp=50.0 + i)
                                     for i in (1, 2, 3)], T0)
    # 补传批里混着已应用过的旧序号：乱序标记、不回退
    raws = [raw("old-1", 1, temp=1.0), raw("new-4", 4, temp=54.0)]
    batch = gateway.ingest_batch("b-backfill", raws, T0)
    log_batch(batch, raws)
    assert batch.items[0].result.flags == (MessageFlag.OUT_OF_ORDER,)
    assert batch.items[1].result.flags == (MessageFlag.ACCEPTED,)
    twin = gateway.get_twin("dev-1")
    assert twin.reported == {"temp": 54.0, "seq": 4}
    assert twin.reported_version == 4          # 乱序条未回退状态


def test_batch_spans_devices_without_cross_talk(gateway):
    raws = [
        raw("a1", 1, device="dev-1", secret="s1", temp=11.0),
        raw("b1", 1, device="dev-2", secret="s2", temp=22.0),
        raw("a2", 2, device="dev-1", secret="s1", temp=12.0),
        raw("b2", 2, device="dev-2", secret="s2", temp=23.0),
    ]
    batch = gateway.ingest_batch("b-multi", raws, T0)
    log_batch(batch, raws)
    assert all(it.result.accepted for it in batch.items)
    t1, t2 = gateway.get_twin("dev-1"), gateway.get_twin("dev-2")
    log.info("判定依据: 多设备同批互不污染 | dev-1=%s dev-2=%s",
             t1.reported, t2.reported)
    assert t1.reported == {"temp": 12.0, "seq": 2}
    assert t2.reported == {"temp": 23.0, "seq": 2}
    assert gateway.telemetry.applied_seq("dev-1") == 2
    assert gateway.telemetry.applied_seq("dev-2") == 2
