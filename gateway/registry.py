"""设备注册与凭证生命周期管理。"""
from __future__ import annotations

import threading

from .models import AuthFailure, AuthResult, Credential, Device, DeviceStatus


class DeviceRegistry:
    """进程内设备注册表：幂等注册、凭证轮换重叠窗口、撤销与认证。"""

    def __init__(self, rotation_overlap_seconds: float = 300.0) -> None:
        self._lock = threading.RLock()
        self._overlap = rotation_overlap_seconds
        self._devices: dict[str, Device] = {}

    def register(self, device_id: str, secret: str, now: float,
                 ttl_seconds: float = 3600.0) -> Device:
        """幂等注册：重复注册返回既有设备，不重置状态与凭证。"""
        with self._lock:
            existing = self._devices.get(device_id)
            if existing is not None:
                return existing
            device = Device(
                device_id=device_id,
                credentials=[Credential(secret=secret, not_before=now,
                                        not_after=now + ttl_seconds)],
                registered_at=now,
            )
            self._devices[device_id] = device
            return device

    def rotate_credential(self, device_id: str, new_secret: str, now: float,
                          ttl_seconds: float = 3600.0) -> Credential:
        """轮换凭证：新凭证立即生效；当前仍有效的旧凭证截断到 now+overlap，
        形成明确的新旧共存重叠窗口。"""
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                raise KeyError(f"unknown device: {device_id}")
            for cred in device.credentials:
                if cred.is_valid_at(now):
                    # 旧凭证截断：重叠窗口结束后即失效
                    idx = device.credentials.index(cred)
                    device.credentials[idx] = Credential(
                        secret=cred.secret,
                        not_before=cred.not_before,
                        not_after=min(cred.not_after, now + self._overlap),
                    )
            new_cred = Credential(secret=new_secret, not_before=now,
                                  not_after=now + ttl_seconds)
            device.credentials.append(new_cred)
            return new_cred

    def revoke(self, device_id: str) -> bool:
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                return False
            device.status = DeviceStatus.REVOKED
            return True

    def authenticate(self, device_id: str, secret: str, now: float) -> AuthResult:
        """认证并区分失败原因：未知设备 / 已撤销 / 凭证不匹配 / 凭证过期。"""
        with self._lock:
            device = self._devices.get(device_id)
            if device is None:
                return AuthResult(ok=False, failure=AuthFailure.UNKNOWN_DEVICE)
            if device.status is DeviceStatus.REVOKED:
                return AuthResult(ok=False, device_id=device_id,
                                  failure=AuthFailure.DEVICE_REVOKED)
            matched = [c for c in device.credentials if c.fingerprint == secret]
            if not matched:
                return AuthResult(ok=False, device_id=device_id,
                                  failure=AuthFailure.UNKNOWN_CREDENTIAL)
            if not any(c.is_valid_at(now) for c in matched):
                return AuthResult(ok=False, device_id=device_id,
                                  failure=AuthFailure.CREDENTIAL_EXPIRED)
            return AuthResult(ok=True, device_id=device_id)

    def get(self, device_id: str) -> Device | None:
        with self._lock:
            return self._devices.get(device_id)
