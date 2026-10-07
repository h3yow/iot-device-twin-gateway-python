"""负载合规判定：四类 PayloadIssue 归类、多问题优先级、对孪生与顺序零影响。"""
import json
import logging

import pytest

from gateway.models import (
    MessageFlag,
    PayloadIssue,
    RejectReason,
    TelemetryMessage,
)
from gateway.service import Gateway
from gateway.validation import check_envelope, check_message

log = logging.getLogger(__name__)

T0 = 1_000.0
SKEW = 60.0


@pytest.fixture
def gateway():
    g = Gateway(max_clock_skew_seconds=SKEW)
    g.registry.register("dev-1", "s1", now=T0)
    return g


_DEFAULT = object()


def tmsg(mid="m1", seq=1, ts=T0, pv=1, payload=_DEFAULT, device="dev-1",
         secret="s1"):
    return TelemetryMessage(device, secret, mid, seq, ts, pv,
                            {"temp": 20.0} if payload is _DEFAULT else payload)


def raw(mid="m1", seq=1, ts=T0, pv=1, payload=_DEFAULT):
    return {"message_id": mid, "seq": seq, "timestamp": ts,
            "protocol_version": pv,
            "payload": {"temp": 20.0} if payload is _DEFAULT else payload}


# ---------------------------------------------------------------------------
# invalid_structure
# ---------------------------------------------------------------------------

def test_invalid_structure_cases(gateway):
    cases = [
        ("payload 是字符串", tmsg(payload="not-a-dict")),
        ("payload 是列表", tmsg(payload=[{"temp": 1}])),
        ("payload 是空对象", tmsg(payload={})),
        ("payload 是 None", tmsg(payload=None)),
        ("嵌套键为空", tmsg(payload={"a": {"": 1}})),
        ("嵌套值非法类型", tmsg(payload={"a": b"bytes"})),
        ("嵌套数组超限", tmsg(payload={"a": list(range(300))})),
        ("嵌套对象键过多", tmsg(payload={str(i): i for i in range(300)})),
        ("嵌套过深", tmsg(payload={"a": {"b": {"c": {"d": {"e": {
            "f": {"g": {"h": {"i": {"j": 1}}}}}}}}}})),
    ]
    for why, msg in cases:
        r = gateway.ingest(msg, T0)
        log.info("输入(%s): payload=%r | 判定: reject=%s issue=%s detail=%s",
                 why, _short(msg.payload), r.reject_reason, r.payload_issue,
                 r.issue_detail)
        assert not r.accepted
        assert r.reject_reason is RejectReason.MALFORMED_PAYLOAD
        assert r.payload_issue is PayloadIssue.INVALID_STRUCTURE

    # 批量入口对离谱条目也返回结构化判定
    for why, entry in [("条目是字符串", "garbage"), ("条目是数字", 42),
                      ("条目是列表", [1, 2]), ("条目是 None", None)]:
        r = check_envelope(entry)
        log.info("批量条目(%s): %r | 判定: issue=%s detail=%s",
                 why, entry, r.payload_issue, r.issue_detail)
        assert r is not None and r.payload_issue is PayloadIssue.INVALID_STRUCTURE


def _short(value):
    text = repr(value)
    return text if len(text) <= 60 else text[:57] + "..."


# ---------------------------------------------------------------------------
# missing_field
# ---------------------------------------------------------------------------

def test_missing_field_cases(gateway):
    entries = [
        {k: v for k, v in raw().items() if k != "message_id"},
        {k: v for k, v in raw().items() if k != "seq"},
        {k: v for k, v in raw().items() if k != "timestamp"},
        {k: v for k, v in raw().items() if k != "payload"},
        {"message_id": None, "seq": 1, "timestamp": T0, "payload": {"a": 1}},
        {"message_id": "   ", "seq": 1, "timestamp": T0, "payload": {"a": 1}},
    ]
    for entry in entries:
        r = check_envelope(entry)
        log.info("输入: 缺字段条目 keys=%s | 判定: issue=%s detail=%s",
                 sorted(entry.keys()), r.payload_issue, r.issue_detail)
        assert r is not None and r.payload_issue is PayloadIssue.MISSING_FIELD

    # 经批量链路同样归类
    batch = gateway.ingest_batch("b-missing", "dev-1", "s1", entries, T0)
    assert all(not v.accepted and v.payload_issue is PayloadIssue.MISSING_FIELD
               for v in batch.items)


# ---------------------------------------------------------------------------
# type_mismatch
# ---------------------------------------------------------------------------

def test_type_mismatch_cases(gateway):
    entries = [
        ("seq 是字符串", raw(seq="1")),
        ("seq 是浮点", raw(seq=1.5)),
        ("seq 是 bool", raw(seq=True)),
        ("timestamp 是字符串", raw(ts="1000")),
        ("protocol_version 是字符串", raw(pv="1")),
        ("message_id 是数字", raw(mid=123)),
    ]
    for why, entry in entries:
        r = check_envelope(entry)
        log.info("输入(%s): entry=%s | 判定: issue=%s detail=%s",
                 why, {k: v for k, v in entry.items() if k != "payload"},
                 r.payload_issue, r.issue_detail)
        assert r is not None and r.payload_issue is PayloadIssue.TYPE_MISMATCH


# ---------------------------------------------------------------------------
# invalid_value
# ---------------------------------------------------------------------------

def test_invalid_value_cases():
    # 信封取值问题（check_envelope 覆盖）
    envelope_cases = [
        ("seq=0", raw(seq=0)),
        ("seq=-3", raw(seq=-3)),
        ("timestamp=NaN", raw(ts=float("nan"))),
        ("timestamp=inf", raw(ts=float("inf"))),
    ]
    for why, entry in envelope_cases:
        r = check_envelope(entry)
        log.info("输入(%s) | 判定: issue=%s detail=%s", why,
                 r.payload_issue, r.issue_detail)
        assert r is not None and r.payload_issue is PayloadIssue.INVALID_VALUE
    # 负载取值/结构限制（check_message 才会递归校验 payload）
    payload_cases = [
        ("payload 含 NaN", raw(payload={"a": float("nan")}), PayloadIssue.INVALID_VALUE),
        ("payload 含超长字符串", raw(payload={"a": "x" * 5000}), PayloadIssue.INVALID_VALUE),
        ("payload 值为 null", raw(payload={"a": None}), PayloadIssue.INVALID_VALUE),
    ]
    for why, entry, expected_issue in payload_cases:
        msg = TelemetryMessage("dev-1", "s1", entry["message_id"], entry["seq"],
                               entry["timestamp"], entry.get("protocol_version", 1),
                               entry["payload"])
        r = check_message(msg, T0, SKEW)
        log.info("输入(%s) | 判定: issue=%s detail=%s", why,
                 r.payload_issue, r.issue_detail)
        assert r is not None and r.payload_issue is expected_issue


# ---------------------------------------------------------------------------
# 多问题命中：确定性优先级，可解释、可复现
# ---------------------------------------------------------------------------

def test_priority_when_multiple_issues():
    # 同时缺字段且结构非法：结构 -> 缺字段
    assert check_envelope("garbage").payload_issue is PayloadIssue.INVALID_STRUCTURE
    # 同时缺 message_id 且 seq 类型错：缺失优先（字段检查先统一过一遍存在性）
    entry = {"seq": "x", "timestamp": T0, "payload": {"a": 1}}
    r = check_envelope(entry)
    log.info("多问题(缺 mid + seq 类型错): issue=%s | 依据: missing_field 优先",
             r.payload_issue)
    assert r.payload_issue is PayloadIssue.MISSING_FIELD
    # message_id 存在后，seq 类型错才暴露
    entry["message_id"] = "m"
    r = check_envelope(entry)
    assert r.payload_issue is PayloadIssue.TYPE_MISMATCH
    # seq 类型修正后，seq=0 的取值错暴露
    entry["seq"] = 0
    r = check_envelope(entry)
    assert r.payload_issue is PayloadIssue.INVALID_VALUE
    # 取值修正后，深层负载问题才暴露（check_message 递归校验 payload）
    entry["seq"] = 1
    entry["payload"] = "bad"
    msg = TelemetryMessage("dev-1", "s1", entry["message_id"], entry["seq"],
                           entry["timestamp"], 1, entry["payload"])
    r = check_message(msg, T0, SKEW)
    assert r.payload_issue is PayloadIssue.INVALID_STRUCTURE


def test_other_reject_reasons_remain_distinct(gateway):
    pv = gateway.ingest(tmsg(pv=99), T0)
    stale = gateway.ingest(tmsg(ts=T0 + SKEW + 1), T0)
    log.info("输入: pv=99 | 判定: %s/%s", pv.reject_reason, pv.issue_detail)
    log.info("输入: 过期时间戳 | 判定: %s/%s", stale.reject_reason, stale.issue_detail)
    assert pv.reject_reason is RejectReason.UNKNOWN_PROTOCOL_VERSION
    assert stale.reject_reason is RejectReason.STALE_TIMESTAMP
    # 协议版本类型错属于 malformed/type_mismatch，而不是 unknown_protocol_version
    r = check_envelope(raw(pv="v1"))
    assert r.payload_issue is PayloadIssue.TYPE_MISMATCH


# ---------------------------------------------------------------------------
# 不合规输入：不报错中断、不写孪生、不占顺序位置
# ---------------------------------------------------------------------------

def test_malformed_never_raises_and_never_advances_state(gateway):
    wild_inputs: list = [
        None, 1, -1, 1.5, True, "x", [], {}, [None], {"nested": ["x", {}]},
        raw(mid="", seq="x", ts=None, pv=None, payload=object()),
        {"message_id": 1, "seq": 1.5, "timestamp": [], "payload": set()},
    ]
    for i, entry in enumerate(wild_inputs):
        # 任何输入都必须返回结构化结果，绝不抛异常
        r = gateway.ingest_batch(f"wild-{i}", "dev-1", "s1", [entry], T0)
        assert r.auth_ok and len(r.items) == 1 and not r.items[0].accepted
        assert r.items[0].reject_reason is RejectReason.MALFORMED_PAYLOAD
        log.info("离谱输入#%d: %s | 判定 issue=%s detail=%s", i,
                 _short(entry), r.items[0].payload_issue, r.items[0].issue_detail)

    twin = gateway.get_twin("dev-1")
    log.info("判定依据: %d 条离谱输入后 reported=%r version=%d watermark=%d（全部为零值）",
             len(wild_inputs), twin.reported, twin.reported_version,
             gateway.telemetry.applied_seq("dev-1"))
    assert twin.reported == {} and twin.reported_version == 0
    assert gateway.telemetry.applied_seq("dev-1") == 0

    # 水位仍为 0：随后正常消息以 seq=1 accepted，而不是被当成乱序
    r = gateway.ingest(tmsg("ok", 1), T0)
    assert r.accepted and r.flags == (MessageFlag.ACCEPTED,)


def test_valid_payload_shapes_are_accepted(gateway):
    for i, payload in enumerate([
        {"temp": 21.5},
        {"ok": True, "name": "sensor-1"},
        {"nested": {"a": [1, 2, 3], "b": {"c": "x"}}},
        {"flags": []},  # 空数组是合法值；仅空对象非法
    ]):
        r = gateway.ingest(tmsg(f"valid-{i}", i + 1, payload=payload), T0)
        log.info("合法 payload=%r | 判定: accepted=%s flags=%s",
                 payload, r.accepted, [f.value for f in r.flags])
        assert r.accepted
