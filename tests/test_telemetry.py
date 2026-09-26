"""遥测上报：乱序、重复、缺口、过期时间戳、畸形负载与未知协议版本。"""
import logging

import pytest

from gateway.models import (
    AuthFailure,
    MessageFlag,
    RejectReason,
    TelemetryMessage,
)
from gateway.service import Gateway

log = logging.getLogger(__name__)

T0 = 1_000.0
SKEW = 60.0


@pytest.fixture
def gateway():
    g = Gateway(rotation_overlap_seconds=100.0, max_clock_skew_seconds=SKEW)
    g.registry.register("dev-1", "s1", now=T0)
    return g


def msg(mid, seq, device="dev-1", secret="s1", ts=T0, pv=1, payload=None):
    return TelemetryMessage(device, secret, mid, seq, ts, pv,
                            {"temp": 20.0} if payload is None else payload)


def test_duplicate_and_out_of_order_do_not_regress_twin(gateway):
    sequence = [
        msg("m1", 1, payload={"temp": 21.0}),
        msg("m2", 2, payload={"temp": 22.0}),
        msg("m1", 1, payload={"temp": 99.0}),   # 重复：同 message_id
        msg("m3", 1, payload={"temp": 88.0}),   # 乱序：新 id 但 seq 落后
        msg("m4", 3, payload={"temp": 23.0}),
    ]
    results = [gateway.ingest(m, T0) for m in sequence]
    for m, r in zip(sequence, results):
        log.info("输入: message_id=%s seq=%d payload=%s | 判定: accepted=%s flags=%s",
                 m.message_id, m.seq, m.payload, r.accepted,
                 [f.value for f in r.flags])
    assert results[0].flags == (MessageFlag.ACCEPTED,)
    assert results[1].flags == (MessageFlag.ACCEPTED,)
    assert results[2].flags == (MessageFlag.DUPLICATE,)
    assert results[3].flags == (MessageFlag.OUT_OF_ORDER,)
    assert results[4].flags == (MessageFlag.ACCEPTED,)
    twin = gateway.get_twin("dev-1")
    log.info("判定依据: 重复/乱序不得回退状态或重复计数 | reported=%s version=%d",
             twin.reported, twin.reported_version)
    assert twin.reported == {"temp": 23.0}      # 未被 99/88 回退
    assert twin.reported_version == 3           # 只应用了 3 条


def test_gap_is_flagged_but_applies(gateway):
    r1 = gateway.ingest(msg("m1", 1), T0)
    r2 = gateway.ingest(msg("m5", 5), T0)       # 跳过 2..4
    log.info("输入: seq=1 后接 seq=5 | 判定: flags=%s | 依据: 缺口标记且确定性前进",
             [f.value for f in r2.flags])
    assert r1.flags == (MessageFlag.ACCEPTED,)
    assert r2.accepted and r2.flags == (MessageFlag.GAP,)
    assert gateway.telemetry.applied_seq("dev-1") == 5


def test_reject_reasons_are_distinguishable(gateway):
    cases = [
        (msg("b1", 1, payload="not-a-dict"), RejectReason.MALFORMED_PAYLOAD),
        (msg("b2", 1, payload={}), RejectReason.MALFORMED_PAYLOAD),
        (msg("b3", 1, pv=99), RejectReason.UNKNOWN_PROTOCOL_VERSION),
        (msg("b4", 1, ts=T0 - SKEW - 1), RejectReason.STALE_TIMESTAMP),
        (msg("b5", 1, ts=T0 + SKEW + 1), RejectReason.STALE_TIMESTAMP),
    ]
    for m, expected in cases:
        r = gateway.ingest(m, T0)
        log.info("输入: message_id=%s pv=%d ts=%.1f payload=%r | 判定: reject=%s",
                 m.message_id, m.protocol_version, m.timestamp, m.payload,
                 r.reject_reason)
        assert not r.accepted and r.reject_reason is expected
    # 被拒绝的消息不占用 seq 水位，后续正常消息仍按序接受
    r = gateway.ingest(msg("ok1", 1), T0)
    assert r.accepted and r.flags == (MessageFlag.ACCEPTED,)


def test_auth_failures_block_ingest(gateway):
    gateway.registry.revoke("dev-1")
    cases = [
        (msg("a1", 1, device="ghost"), AuthFailure.UNKNOWN_DEVICE),
        (msg("a2", 1, secret="bad"), AuthFailure.DEVICE_REVOKED),
        (msg("a3", 1), AuthFailure.DEVICE_REVOKED),
    ]
    for m, expected in cases:
        r = gateway.ingest(m, T0)
        log.info("输入: device=%s secret=%s | 判定: auth_failure=%s",
                 m.device_id, m.secret, r.auth_failure)
        assert not r.accepted and r.auth_failure is expected
