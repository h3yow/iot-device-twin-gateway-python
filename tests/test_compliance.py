"""合规判定：各类不合规输入的归类、确定性与对孪生/顺序状态的零影响。"""
import logging

import pytest

from gateway.models import (
    MessageFlag,
    REJECT_REASON_CATEGORY,
    RejectReason,
    RejectionCategory,
)
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0


@pytest.fixture
def gateway():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=60.0)
    g.registry.register("dev-1", "s1", now=T0)
    return g


def good(**overrides):
    base = {"device_id": "dev-1", "secret": "s1", "message_id": "m1",
            "seq": 1, "timestamp": T0, "protocol_version": 1,
            "payload": {"temp": 20.0}}
    base.update(overrides)
    return base


def test_all_non_compliant_kinds_map_to_payload_non_compliant(gateway):
    # 逐类构造：缺失 / 类型 / 取值 / 结构
    cases = [
        ("条目非对象", "not-a-dict", RejectReason.MALFORMED_PAYLOAD),
        ("条目是列表", [1, 2], RejectReason.MALFORMED_PAYLOAD),
        ("缺 seq", {k: v for k, v in good().items() if k != "seq"},
         RejectReason.MISSING_FIELD),
        ("缺 payload", {k: v for k, v in good().items() if k != "payload"},
         RejectReason.MISSING_FIELD),
        ("seq 是字符串", good(seq="1"), RejectReason.INVALID_FIELD_TYPE),
        ("seq 是布尔", good(seq=True), RejectReason.INVALID_FIELD_TYPE),
        ("timestamp 是字符串", good(timestamp="now"),
         RejectReason.INVALID_FIELD_TYPE),
        ("device_id 是数字", good(device_id=7),
         RejectReason.INVALID_FIELD_TYPE),
        ("seq 为 0", good(seq=0), RejectReason.INVALID_FIELD_VALUE),
        ("message_id 为空串", good(message_id=""),
         RejectReason.INVALID_FIELD_VALUE),
        ("timestamp 为 NaN", good(timestamp=float("nan")),
         RejectReason.INVALID_FIELD_VALUE),
        ("protocol_version 为 0", good(protocol_version=0),
         RejectReason.INVALID_FIELD_VALUE),
        ("payload 非对象", good(payload="junk"), RejectReason.MALFORMED_PAYLOAD),
        ("payload 为空对象", good(payload={}), RejectReason.MALFORMED_PAYLOAD),
    ]
    for label, raw, expected in cases:
        r = gateway.ingest_raw(raw, T0)
        category = REJECT_REASON_CATEGORY[r.reject_reason]
        log.info("输入: %-18s raw=%r | 命中规则: %s | 结论: reject=%s 分类=%s",
                 label, raw, r.detail, r.reject_reason.value, category.value)
        assert not r.accepted
        assert r.reject_reason is expected
        assert category is RejectionCategory.PAYLOAD_NON_COMPLIANT
        assert r.detail  # 每条都有可解释的判定依据


def test_multiple_problems_first_rule_in_fixed_order_wins(gateway):
    # 同时缺 seq 且 timestamp 类型错：字段顺序 seq 在前，判 missing_field:seq
    raw = {k: v for k, v in good(timestamp="bad").items() if k != "seq"}
    r = gateway.ingest_raw(raw, T0)
    log.info("输入: 缺seq+timestamp类型错 | 命中规则: %s | 结论: %s",
             r.detail, r.reject_reason.value)
    assert r.reject_reason is RejectReason.MISSING_FIELD
    assert r.detail == "missing_field:seq"
    # 同一条同时 seq 类型错且 timestamp 取值非法：字段顺序 seq 在前
    r2 = gateway.ingest_raw(good(seq="x", timestamp=float("nan")), T0)
    log.info("输入: seq类型错+timestamp非法 | 命中规则: %s | 结论: %s",
             r2.detail, r2.reject_reason.value)
    assert r2.reject_reason is RejectReason.INVALID_FIELD_TYPE
    assert r2.detail == "invalid_type:seq expected int"


def test_non_compliant_never_touches_twin_or_watermark(gateway):
    bad_inputs = [
        "garbage",
        {k: v for k, v in good(message_id="b1").items() if k != "seq"},
        good(message_id="b2", seq=-3),
        good(message_id="b3", payload=None),
        good(message_id="b4", payload={}),
    ]
    for raw in bad_inputs:
        r = gateway.ingest_raw(raw, T0)
        log.info("输入: raw=%r | 命中规则: %s | 结论: reject=%s",
                 raw, r.detail, r.reject_reason.value)
        assert not r.accepted
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 不合规条目不得写入孪生或占用顺序位置 | reported=%s "
             "version=%d watermark=%d",
             twin.reported, twin.reported_version,
             gateway.telemetry.applied_seq("dev-1"))
    assert twin.reported == {} and twin.reported_version == 0
    assert gateway.telemetry.applied_seq("dev-1") == 0
    # 后续正常条目仍从 seq=1 按序接受
    r = gateway.ingest_raw(good(message_id="ok1"), T0)
    assert r.accepted and r.flags == (MessageFlag.ACCEPTED,)
    assert gateway.get_twin("dev-1").reported == {"temp": 20.0}


def test_non_compliant_items_do_not_poison_batch_mates(gateway):
    raws = [
        good(message_id="g1", seq=1, payload={"a": 1}),
        good(message_id="bad", seq="oops"),          # 不合规
        good(message_id="g2", seq=2, payload={"b": 2}),
    ]
    batch = gateway.ingest_batch("batch-mix", raws, T0)
    for item in batch.items:
        r = item.result
        log.info("批内条目: index=%d | 命中规则: %s | 结论: accepted=%s %s",
                 item.index, r.detail or "-", r.accepted,
                 r.reject_reason.value if r.reject_reason else
                 [f.value for f in r.flags])
    assert batch.items[0].result.accepted
    assert not batch.items[1].result.accepted
    assert batch.items[1].result.reject_reason is RejectReason.INVALID_FIELD_TYPE
    assert batch.items[2].result.accepted
    twin = gateway.get_twin("dev-1")
    assert twin.reported == {"a": 1, "b": 2} and twin.reported_version == 2
    assert gateway.telemetry.applied_seq("dev-1") == 2
