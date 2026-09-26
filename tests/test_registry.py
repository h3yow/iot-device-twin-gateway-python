"""注册幂等与凭证生命周期：轮换窗口边界、撤销、原因可区分。"""
import logging

import pytest

from gateway.models import AuthFailure, DeviceStatus
from gateway.registry import DeviceRegistry

log = logging.getLogger("test.registry")

OVERLAP = 60.0


@pytest.fixture
def registry():
    return DeviceRegistry(rotation_overlap=OVERLAP)


def test_duplicate_register_is_idempotent(registry):
    d1 = registry.register("dev-a", "s1", now=100.0)
    d2 = registry.register("dev-a", "s2", now=200.0)  # 不同密钥重复注册
    log.info("输入: 重复注册 dev-a(s1->s2); 判定依据: 幂等, 应返回既有设备且凭证不变")
    assert d1 is d2
    assert len(d2.credentials) == 1 and d2.credentials[0].secret == "s1"
    assert registry.authenticate("dev-a", "s1", now=300.0).ok
    assert registry.authenticate("dev-a", "s2", now=300.0).failure is AuthFailure.BAD_CREDENTIAL


def test_rotation_overlap_window_boundaries(registry):
    registry.register("dev-b", "old", now=0.0)
    registry.rotate_credential("dev-b", "new", now=1000.0)
    cases = [
        ("old", 1000.0, True, "轮换瞬间旧凭证仍在窗口内"),
        ("old", 1000.0 + OVERLAP - 0.001, True, "窗口右边界前旧凭证有效"),
        ("old", 1000.0 + OVERLAP, False, "窗口右边界(not_after)旧凭证过期"),
        ("new", 1000.0 + OVERLAP, True, "新凭证不受窗口影响"),
    ]
    for secret, now, expect_ok, why in cases:
        res = registry.authenticate("dev-b", secret, now)
        log.info("输入: secret=%s now=%.3f; 判定依据: %s -> ok=%s", secret, now, why, res.ok)
        assert res.ok is expect_ok, why
        if not expect_ok:
            assert res.failure is AuthFailure.CREDENTIAL_EXPIRED


def test_auth_failure_reasons_are_distinct(registry):
    registry.register("dev-c", "s1", now=0.0)
    registry.rotate_credential("dev-c", "s2", now=100.0)
    registry.register("dev-d", "s3", now=0.0)
    registry.revoke("dev-d")
    cases = [
        ("ghost", "s1", 150.0, AuthFailure.UNKNOWN_DEVICE),
        ("dev-c", "nope", 150.0, AuthFailure.BAD_CREDENTIAL),
        ("dev-c", "s1", 100.0 + OVERLAP + 1, AuthFailure.CREDENTIAL_EXPIRED),
        ("dev-d", "s3", 150.0, AuthFailure.DEVICE_REVOKED),
    ]
    for device_id, secret, now, expected in cases:
        res = registry.authenticate(device_id, secret, now)
        log.info("输入: device=%s secret=%s now=%s; 判定依据: 期望 %s, 实际 %s",
                 device_id, secret, now, expected.value, res.failure)
        assert res.failure is expected
    assert len({c[3] for c in cases}) == 4  # 四种原因互不相同


def test_revoked_device_stays_revoked(registry):
    registry.register("dev-e", "s1", now=0.0)
    registry.revoke("dev-e")
    registry.rotate_credential("dev-e", "s2", now=10.0)  # 轮换不能解除撤销
    res = registry.authenticate("dev-e", "s2", now=20.0)
    log.info("输入: 已撤销设备用新凭证认证; 判定依据: 撤销优先 -> %s", res.failure)
    assert res.failure is AuthFailure.DEVICE_REVOKED
    assert registry.get("dev-e").status is DeviceStatus.REVOKED
