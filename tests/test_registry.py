"""设备注册与凭证生命周期：幂等注册、轮换窗口边界、撤销与失败原因区分。"""
import logging

import pytest

from gateway.models import AuthFailure
from gateway.registry import DeviceRegistry

log = logging.getLogger(__name__)

T0 = 1_000.0


@pytest.fixture
def registry():
    return DeviceRegistry(rotation_overlap_seconds=100.0)


def test_register_is_idempotent(registry):
    d1 = registry.register("dev-1", "secret-a", now=T0)
    d2 = registry.register("dev-1", "secret-b", now=T0 + 10)
    log.info("输入: 重复注册 device_id=dev-1, 新 secret=secret-b | "
             "判定依据: 幂等注册应返回既有设备且凭证不变")
    assert d1 is d2
    assert d2.credentials[0].secret == "secret-a"
    assert registry.authenticate("dev-1", "secret-a", T0 + 10).ok
    assert registry.authenticate("dev-1", "secret-b", T0 + 10).failure \
        is AuthFailure.UNKNOWN_CREDENTIAL


def test_rotation_overlap_window_boundaries(registry):
    registry.register("dev-1", "old", now=T0, ttl_seconds=1000.0)
    registry.rotate_credential("dev-1", "new", now=T0 + 500, ttl_seconds=1000.0)
    # 重叠窗口 = [1500, 1600)
    cases = [
        ("old", T0 + 599.9, True, "窗口内旧凭证应有效"),
        ("old", T0 + 600.0, False, "窗口右边界(不含)旧凭证应过期"),
        ("new", T0 + 500.0, True, "新凭证立即生效"),
    ]
    for secret, now, expect_ok, why in cases:
        result = registry.authenticate("dev-1", secret, now)
        log.info("输入: secret=%s now=%.1f | 判定依据: %s | 结果: ok=%s failure=%s",
                 secret, now, why, result.ok, result.failure)
        assert result.ok is expect_ok
        if not expect_ok:
            assert result.failure is AuthFailure.CREDENTIAL_EXPIRED


def test_credential_expiry(registry):
    registry.register("dev-1", "s", now=T0, ttl_seconds=60.0)
    result = registry.authenticate("dev-1", "s", T0 + 60.0)
    log.info("输入: ttl=60s 的凭证在 t+60 认证 | 判定依据: not_after 为开区间")
    assert result.failure is AuthFailure.CREDENTIAL_EXPIRED


def test_failure_reasons_are_distinguishable(registry):
    registry.register("dev-1", "s", now=T0)
    registry.revoke("dev-1")
    cases = [
        ("ghost", "s", AuthFailure.UNKNOWN_DEVICE),
        ("dev-1", "wrong", AuthFailure.DEVICE_REVOKED),  # 撤销优先于凭证检查
        ("dev-1", "s", AuthFailure.DEVICE_REVOKED),
    ]
    for device_id, secret, expected in cases:
        result = registry.authenticate(device_id, secret, T0)
        log.info("输入: device=%s secret=%s | 判定依据: 期望失败原因 %s | 实际: %s",
                 device_id, secret, expected, result.failure)
        assert result.failure is expected
    # 未撤销设备的错误凭证
    registry.register("dev-2", "s2", now=T0)
    result = registry.authenticate("dev-2", "nope", T0)
    assert result.failure is AuthFailure.UNKNOWN_CREDENTIAL


def test_revoked_device_cannot_ingest_even_with_valid_credential(registry):
    registry.register("dev-1", "s", now=T0)
    assert registry.revoke("dev-1") is True
    assert registry.revoke("ghost") is False
    result = registry.authenticate("dev-1", "s", T0)
    log.info("输入: 已撤销设备用有效凭证认证 | 判定依据: 撤销状态优先, failure=%s",
             result.failure)
    assert result.failure is AuthFailure.DEVICE_REVOKED
