"""遥测：乱序、重复、缺口、过期时间戳、畸形与未知版本。"""
import logging

import pytest

from gateway.models import TelemetryMessage, TelemetryVerdict as V
from gateway.registry import DeviceRegistry
from gateway.telemetry import TelemetryProcessor

log = logging.getLogger("test.telemetry")

SKEW = 100.0


@pytest.fixture
def proc():
    reg = DeviceRegistry(rotation_overlap=60.0)
    reg.register("d1", "s1", now=0.0)
    return TelemetryProcessor(reg, max_clock_skew=SKEW)


def msg(seq, ts, secret="s1", payload=None, version=1, device="d1"):
    return TelemetryMessage(device, secret, seq, ts, {"t": seq} if payload is None else payload, version)


def ingest(proc, m, now, why):
    r = proc.ingest(m, now)
    log.info("输入: seq=%s ts=%s ver=%s now=%s; 判定依据: %s -> %s (%s)",
             m.seq, m.timestamp, m.version, now, why, r.verdict.value, r.detail)
    return r


def test_out_of_order_and_duplicate(proc):
    assert ingest(proc, msg(1, 10.0), 10.0, "首条按序").verdict is V.ACCEPTED
    assert ingest(proc, msg(3, 10.0), 10.0, "跳过 seq=2 产生缺口").verdict is V.GAP_DETECTED
    assert ingest(proc, msg(2, 10.0), 10.0, "乱序补缺口, 不回退").verdict is V.ACCEPTED_OUT_OF_ORDER
    assert ingest(proc, msg(2, 10.0), 10.0, "重复 seq=2").verdict is V.DUPLICATE
    assert ingest(proc, msg(1, 10.0), 10.0, "重复 seq=1").verdict is V.DUPLICATE
    assert ingest(proc, msg(4, 10.0), 10.0, "缺口补齐后按序").verdict is V.ACCEPTED


def test_duplicate_not_counted_twice(proc):
    stream = proc._streams
    ingest(proc, msg(1, 10.0), 10.0, "接受")
    ingest(proc, msg(1, 10.0), 10.0, "重复")
    ingest(proc, msg(1, 10.0), 10.0, "重复")
    log.info("判定依据: seen 集合中 seq=1 仅出现一次 -> %s", stream["d1"].seen)
    assert stream["d1"].seen == {1}


def test_stale_timestamp_rejected(proc):
    r = ingest(proc, msg(1, 10.0), 10.0 + SKEW + 1, "ts 超出最大时钟偏移")
    assert r.verdict is V.STALE_TIMESTAMP
    r = ingest(proc, msg(1, 10.0), 10.0 + SKEW, "边界: 恰好 skew 内仍接受")
    assert r.verdict is V.ACCEPTED


def test_malformed_and_unknown_version(proc):
    bad_seq = msg(0, 10.0)
    assert ingest(proc, bad_seq, 10.0, "seq 必须为正整数").verdict is V.REJECTED_MALFORMED
    bad_payload = msg(1, 10.0, payload=["not", "a", "dict"])
    assert ingest(proc, bad_payload, 10.0, "payload 必须是 dict").verdict is V.REJECTED_MALFORMED
    bad_ver = msg(1, 10.0, version=99)
    assert ingest(proc, bad_ver, 10.0, "未知协议版本").verdict is V.REJECTED_UNKNOWN_VERSION


def test_unauthorized_reasons_propagate(proc):
    from gateway.models import AuthFailure
    r = ingest(proc, msg(1, 10.0, secret="wrong"), 10.0, "密钥错误")
    assert r.verdict is V.REJECTED_UNAUTHORIZED and r.auth_failure is AuthFailure.BAD_CREDENTIAL
    r = ingest(proc, msg(1, 10.0, device="ghost"), 10.0, "未知设备")
    assert r.verdict is V.REJECTED_UNAUTHORIZED and r.auth_failure is AuthFailure.UNKNOWN_DEVICE
    proc.registry.revoke("d1")
    r = ingest(proc, msg(1, 10.0), 10.0, "设备已撤销")
    assert r.verdict is V.REJECTED_UNAUTHORIZED and r.auth_failure is AuthFailure.DEVICE_REVOKED


def test_streams_are_isolated_per_device(proc):
    proc.registry.register("d2", "s2", now=0.0)
    ingest(proc, msg(5, 10.0), 10.0, "d1 从 seq=5 开始(缺口)")
    r = ingest(proc, msg(1, 10.0, device="d2", secret="s2"), 10.0, "d2 独立流, 不受 d1 影响")
    assert r.verdict is V.ACCEPTED
