"""端到端：注册 -> 上报 -> 孪生收敛；轮换窗口内的上报。"""
import logging

import pytest

from gateway.models import TelemetryMessage, TelemetryVerdict as V
from gateway.service import GatewayService

log = logging.getLogger("test.service")


@pytest.fixture
def gw():
    return GatewayService(rotation_overlap=60.0, max_clock_skew=100.0)


def msg(device, secret, seq, ts, payload):
    return TelemetryMessage(device, secret, seq, ts, payload)


def test_ingest_updates_twin_and_converges(gw):
    gw.register("d1", "s1", now=0.0)
    gw.update_desired("d1", {"temp": 22}, base_version=0, writer_id="ops", now=1.0)
    assert not gw.get_twin("d1").converged
    r = gw.ingest(msg("d1", "s1", 1, 10.0, {"temp": 22}), now=10.0)
    log.info("输入: 上报与期望一致; 判定依据: %s -> converged=%s", r.verdict.value,
             gw.get_twin("d1").converged)
    assert r.verdict is V.ACCEPTED
    assert gw.get_twin("d1").converged


def test_out_of_order_does_not_rollback_twin(gw):
    gw.register("d1", "s1", now=0.0)
    gw.ingest(msg("d1", "s1", 2, 10.0, {"temp": 22}), now=10.0)  # gap
    gw.ingest(msg("d1", "s1", 1, 10.0, {"temp": 20}), now=10.0)  # 乱序旧值
    twin = gw.get_twin("d1")
    log.info("输入: seq=2 先于 seq=1; 判定依据: 孪生以 seq 为版本, 不回退 -> %s",
             twin.reported.value)
    assert twin.reported.value == {"temp": 22} and twin.reported.version == 2


def test_duplicate_does_not_double_apply(gw):
    gw.register("d1", "s1", now=0.0)
    gw.ingest(msg("d1", "s1", 1, 10.0, {"temp": 20}), now=10.0)
    r = gw.ingest(msg("d1", "s1", 1, 10.0, {"temp": 99}), now=10.0)
    log.info("输入: 同 seq 不同负载重发; 判定依据: 判重拒绝 -> %s", r.verdict.value)
    assert r.verdict is V.DUPLICATE
    assert gw.get_twin("d1").reported.value == {"temp": 20}


def test_reporting_across_rotation_window(gw):
    gw.register("d1", "old", now=0.0)
    gw.rotate_credential("d1", "new", now=100.0)
    r1 = gw.ingest(msg("d1", "old", 1, 110.0, {"a": 1}), now=110.0)
    r2 = gw.ingest(msg("d1", "new", 2, 110.0, {"a": 2}), now=110.0)
    r3 = gw.ingest(msg("d1", "old", 3, 165.0, {"a": 3}), now=165.0)  # 窗口(160)外
    log.info("输入: 窗口内旧/新凭证 + 窗口外旧凭证; 判定依据: %s/%s/%s",
             r1.verdict.value, r2.verdict.value, r3.verdict.value)
    assert r1.verdict is V.ACCEPTED and r2.verdict is V.ACCEPTED
    assert r3.verdict is V.REJECTED_UNAUTHORIZED
    assert gw.get_twin("d1").reported.version == 2  # 被拒上报未污染孪生
