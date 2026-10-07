"""批量补传：正序/乱序/重试收敛，批内重复、乱序、缺号与不合规隔离。"""
import logging

import pytest

from gateway.models import MessageFlag, PayloadIssue, TelemetryMessage
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0
SKEW = 60.0


@pytest.fixture
def gateway():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=SKEW)
    g.registry.register("dev-1", "s1", now=T0)
    return g


def item(mid, seq, **over):
    base = {"message_id": mid, "seq": seq, "timestamp": T0,
            "protocol_version": 1, "payload": {"temp": float(seq)}}
    base.update(over)
    return base


def _by_index(result):
    return {v.index: v for v in result.items}


def test_batch_in_order_converges(gateway):
    items = [item(f"m{s}", s) for s in range(1, 6)]
    r = gateway.ingest_batch("batch-1", "dev-1", "s1", items, T0)
    for v in r.items:
        log.info("输入(正序): batch=batch-1 idx=%d mid=%s | 判定: accepted=%s flags=%s",
                 v.index, v.message_id, v.accepted, [f.value for f in v.flags])
    assert r.auth_ok and r.accepted_count == 5
    assert all(v.flags == (MessageFlag.ACCEPTED,) for v in r.items)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 正序 1..5 全部 accepted | reported=%s version=%d watermark=%d",
             twin.reported, twin.reported_version,
             gateway.telemetry.applied_seq("dev-1"))
    assert twin.reported == {"temp": 5.0}
    assert twin.reported_version == 5
    assert gateway.telemetry.applied_seq("dev-1") == 5


def test_batch_out_of_order_converges_same_result(gateway):
    """同一批数据无论怎么排列，孪生收敛结果一致。"""
    permutations = [
        [5, 3, 1, 4, 2],
        [1, 2, 3, 4, 5],
        [5, 4, 3, 2, 1],
        [2, 5, 1, 4, 3],
    ]
    for p in permutations:
        g = Gateway(max_clock_skew_seconds=SKEW)
        g.registry.register("dev-1", "s1", now=T0)
        items = [item(f"m{s}", s) for s in p]
        r = g.ingest_batch("b", "dev-1", "s1", items, T0)
        log.info("输入(乱序排列): %s | 判定 flags=%s", p,
                 [[f.value for f in v.flags] for v in r.items])
        twin = g.get_twin("dev-1")
        assert twin.reported == {"temp": 5.0}
        assert twin.reported_version == 5
        assert g.telemetry.applied_seq("dev-1") == 5
        # 每条 message_id 的结论与排列无关
        flags_by_mid = {v.message_id: v.flags for v in r.items}
        assert flags_by_mid["m5"] == (MessageFlag.ACCEPTED,)
    log.info("判定依据: 四种排列全部收敛到 reported={temp:5} version=5 watermark=5")


def test_batch_gap_is_flagged_per_item(gateway):
    items = [item("m1", 1), item("m5", 5)]
    r = gateway.ingest_batch("b-gap", "dev-1", "s1", items, T0)
    verdicts = _by_index(r)
    log.info("输入: 批内 seq=[1,5] | 判定: flags=%s,%s | 依据: 缺号条目标 gap 但仍应用",
             verdicts[0].flags, verdicts[1].flags)
    assert verdicts[0].flags == (MessageFlag.ACCEPTED,)
    assert verdicts[1].flags == (MessageFlag.GAP,)
    assert gateway.telemetry.applied_seq("dev-1") == 5
    assert gateway.get_twin("dev-1").reported == {"temp": 5.0}


def test_batch_same_seq_tie_break_is_deterministic(gateway):
    """批内同 seq：按 message_id 字典序决胜，另一条判 out_of_order。"""
    items = [item("m2z", 2, payload={"temp": 22.0}),
             item("m2a", 2, payload={"temp": 11.0}),
             item("m1", 1)]
    r = gateway.ingest_batch("b-tie", "dev-1", "s1", items, T0)
    verdicts = _by_index(r)
    log.info("输入: 同 seq=2 两条 m2a/m2z + m1 | 判定: m2a=%s m2z=%s m1=%s",
             verdicts[1].flags, verdicts[0].flags, verdicts[2].flags)
    assert verdicts[2].flags == (MessageFlag.ACCEPTED,)   # m1
    assert verdicts[1].flags == (MessageFlag.ACCEPTED,)   # m2a 字典序占先
    assert verdicts[0].flags == (MessageFlag.OUT_OF_ORDER,)
    twin = gateway.get_twin("dev-1")
    assert twin.reported == {"temp": 11.0}                 # 败者不得写孪生
    assert twin.reported_version == 2


def test_batch_duplicate_ids_within_batch(gateway):
    items = [item("dup", 1, payload={"temp": 1.0}),
             item("dup", 1, payload={"temp": 9.0})]
    r = gateway.ingest_batch("b-dup", "dev-1", "s1", items, T0)
    verdicts = _by_index(r)
    log.info("输入: 批内同 message_id=dup 两条 | 判定: %s,%s | 依据: 排序后后者 duplicate",
             verdicts[0].flags, verdicts[1].flags)
    assert verdicts[0].flags == (MessageFlag.ACCEPTED,)
    assert verdicts[1].flags == (MessageFlag.DUPLICATE,)
    assert gateway.get_twin("dev-1").reported_version == 1


def test_batch_retry_is_idempotent(gateway):
    items = [item("m2", 2), item("m1", 1), item("m4", 4)]  # 乱序+缺口
    first = gateway.ingest_batch("retry-b", "dev-1", "s1", items, T0)
    twin_after_first = dict(gateway.get_twin("dev-1").reported)
    version_after_first = gateway.get_twin("dev-1").reported_version
    wm_after_first = gateway.telemetry.applied_seq("dev-1")

    # 打乱条目顺序后用同一 batch_id 重试
    second = gateway.ingest_batch("retry-b", "dev-1", "s1",
                                  list(reversed(items)), T0)
    for a, b in zip(first.items, second.items):
        log.info("重试对位: idx=%d mid=%s 首次=%s/%s 重试=%s/%s",
                 a.index, a.message_id, a.accepted, [f.value for f in a.flags],
                 b.accepted, [f.value for f in b.flags])
        assert (a.message_id, a.accepted, a.flags, a.applied_seq,
                a.reject_reason) == (b.message_id, b.accepted, b.flags,
                                     b.applied_seq, b.reject_reason)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 重试不重复计数、不回退 | reported=%s version=%d(first=%d) "
             "watermark=%d(first=%d)",
             twin.reported, twin.reported_version, version_after_first,
             gateway.telemetry.applied_seq("dev-1"), wm_after_first)
    assert twin.reported == twin_after_first
    assert twin.reported_version == version_after_first
    assert gateway.telemetry.applied_seq("dev-1") == wm_after_first
    # 重试时同一 message_id 单条再补也仍是重复
    again = gateway.ingest_batch("retry-b", "dev-1", "s1", items, T0)
    assert again.accepted_count == first.accepted_count


def test_batch_after_offline_gap_backfill_then_catchup(gateway):
    """断网期间积压：先补一个带缺口的批，再补中间缺失序号。"""
    r1 = gateway.ingest_batch("backfill-1", "dev-1", "s1",
                              [item("m10", 10), item("m1", 1)], T0)
    v1 = _by_index(r1)
    log.info("补传1: seq=[10,1] | 判定 m1=%s m10=%s", v1[1].flags, v1[0].flags)
    assert v1[1].flags == (MessageFlag.ACCEPTED,)
    assert v1[0].flags == (MessageFlag.GAP,)
    assert gateway.get_twin("dev-1").reported == {"temp": 10.0}

    # 迟到的中间序号：不回退孪生、不占推进位置之外的计数
    r2 = gateway.ingest_batch("backfill-2", "dev-1", "s1",
                              [item("m5", 5), item("m3", 3)], T0)
    v2 = _by_index(r2)
    log.info("补传2: seq=[5,3] 落后水位10 | 判定 m5=%s m3=%s | 依据: 均 out_of_order",
             v2[0].flags, v2[1].flags)
    assert all(v.flags == (MessageFlag.OUT_OF_ORDER,) for v in r2.items)
    twin = gateway.get_twin("dev-1")
    assert twin.reported == {"temp": 10.0}
    assert twin.reported_version == 2
    assert gateway.telemetry.applied_seq("dev-1") == 10

    # 水位之后的新消息恢复正常
    r3 = gateway.ingest_batch("live", "dev-1", "s1",
                              [item("m11", 11)], T0)
    assert r3.items[0].flags == (MessageFlag.ACCEPTED,)
    assert gateway.get_twin("dev-1").reported == {"temp": 11.0}


def test_batch_malformed_items_do_not_block_others(gateway):
    items = [
        "not-an-object",
        {"seq": 1, "timestamp": T0, "payload": {"temp": 1.0}},          # 缺 message_id
        {"message_id": "bad-seq", "seq": 0, "timestamp": T0,
         "payload": {"temp": 1.0}},                                     # seq 非法
        {"message_id": "bad-ts", "seq": 1, "timestamp": "now",
         "payload": {"temp": 1.0}},                                     # timestamp 类型错
        item("ok1", 1),
        item("ok2", 2),
    ]
    r = gateway.ingest_batch("b-mixed", "dev-1", "s1", items, T0)
    assert r.auth_ok and len(r.items) == 6
    for v in r.items:
        log.info("混合批 idx=%d mid=%s | 判定: accepted=%s reject=%s issue=%s | %s",
                 v.index, v.message_id, v.accepted,
                 v.reject_reason.value if v.reject_reason else None,
                 v.payload_issue.value if v.payload_issue else None,
                 v.issue_detail)
    assert not r.items[0].accepted
    assert r.items[0].payload_issue is PayloadIssue.INVALID_STRUCTURE
    assert r.items[1].payload_issue is PayloadIssue.MISSING_FIELD
    assert r.items[2].payload_issue is PayloadIssue.INVALID_VALUE
    assert r.items[3].payload_issue is PayloadIssue.TYPE_MISMATCH
    assert r.items[4].flags == (MessageFlag.ACCEPTED,)
    assert r.items[5].flags == (MessageFlag.ACCEPTED,)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 4 条不合规不连累 2 条正常, 且不占顺序位置 | reported=%s version=%d",
             twin.reported, twin.reported_version)
    assert twin.reported == {"temp": 2.0}
    assert twin.reported_version == 2
    assert gateway.telemetry.applied_seq("dev-1") == 2


def test_batch_auth_failure_rejects_whole_batch(gateway):
    r = gateway.ingest_batch("b-auth", "dev-1", "wrong-secret",
                             [item("m1", 1)], T0)
    log.info("输入: 批级凭证错误 | 判定: auth_ok=%s failure=%s",
             r.auth_ok, r.auth_failure)
    assert not r.auth_ok and r.items == ()
    assert gateway.get_twin("dev-1").reported_version == 0
    assert gateway.telemetry.applied_seq("dev-1") == 0


def test_batch_interleaved_with_single_ingest_does_not_regress(gateway):
    """单条与批量交错提交：按设备串行化，孪生不回退。"""
    r_single = gateway.ingest(
        TelemetryMessage("dev-1", "s1", "s3", 3, T0, 1, {"temp": 3.0}), T0)
    assert r_single.flags == (MessageFlag.GAP,)
    r = gateway.ingest_batch("b-interleave", "dev-1", "s1",
                             [item("m1", 1), item("m2", 2)], T0)
    assert all(v.flags == (MessageFlag.OUT_OF_ORDER,) for v in r.items)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 单条 seq=3 先到后, 批内 1,2 只能 out_of_order | reported=%s",
             twin.reported)
    assert twin.reported == {"temp": 3.0}
    assert twin.reported_version == 1


def test_concurrent_batches_from_multiple_devices():
    """多设备并发批量补传：计数与记录不丢不重，各设备独立收敛。"""
    import threading
    g = Gateway(max_clock_skew_seconds=SKEW)
    n_devices, n_msgs = 12, 50
    for i in range(n_devices):
        g.registry.register(f"dev-{i}", f"s{i}", now=T0)
    barrier = threading.Barrier(n_devices)
    errors = []

    def worker(i):
        barrier.wait()
        # 每设备两批：先发低序号批，再发高序号批，模拟分两批补传
        items_a = [item(f"m{i}-{s}", s, payload={"v": s})
                   for s in range(1, n_msgs // 2 + 1)]
        items_b = [item(f"m{i}-{s}", s, payload={"v": s})
                   for s in range(n_msgs // 2 + 1, n_msgs + 1)]
        r1 = g.ingest_batch(f"b-{i}-a", f"dev-{i}", f"s{i}", items_a, T0)
        r2 = g.ingest_batch(f"b-{i}-b", f"dev-{i}", f"s{i}", items_b, T0)
        retry = g.ingest_batch(f"b-{i}-a", f"dev-{i}", f"s{i}",
                               list(reversed(items_a)), T0)
        if r1.accepted_count + r2.accepted_count != n_msgs:
            errors.append((i, r1.accepted_count, r2.accepted_count))
        # 重试返回首次判定（accepted_count 与首次相同），但不得重复生效
        if retry.accepted_count != r1.accepted_count:
            errors.append((f"retry-{i}", retry.accepted_count))

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_devices)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("输入: %d 设备 x 50 条乱序双批 + 重试并发 | 异常数=%d 明细=%s",
             n_devices, len(errors), errors[:5])
    assert not errors
    for i in range(n_devices):
        twin = g.get_twin(f"dev-{i}")
        assert twin.reported == {"v": n_msgs}
        assert twin.reported_version == n_msgs
        assert g.telemetry.applied_seq(f"dev-{i}") == n_msgs
    log.info("判定依据: 全部 %d 设备独立收敛到 v=%d, version=%d, 无丢更新",
             n_devices, n_msgs, n_msgs)
