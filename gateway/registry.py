"""设备注册与凭证生命周期管理。"""
from __future__ import annotations

from .models import AuthFailure, AuthResult, Credential, Device, DeviceStatus


class DeviceRegistry:
    """进程内设备注册表。

    - 重复注册幂等：同 device_id 重复注册返回既有设备，不重置凭证与状态。
    - 凭证轮换：新凭证立即生效；旧凭证的 not_after 被截断为
      now + rotation_overlap，重叠窗口内新旧共存，窗口外旧凭证判过期。
    - 撤销设备或失效凭证的上报被拒绝，且原因可区分。
    """

    def __init__(self, rotation_overlap: float = 60.0) -> None:
        self.rotation_overlap = rotation_overlap
        self._devices: dict[str, Device] = {}

    def register(self, device_id: str, secret: str, now: float) -> Device:
        existing = self._devices.get(device_id)
        if existing is not None:
            return existing  # 幂等：重复注册不产生副作用
        device = Device(
            device_id=device_id,
            credentials=[Credential(secret=secret, not_before=now, not_after=float("inf"))],
        )
        self._devices[device_id] = device
        return device

    def rotate_credential(self, device_id: str, new_secret: str, now: float) -> None:
        device = self._devices[device_id]
        for cred in device.credentials:
            if cred.valid_at(now):
                # 旧凭证进入重叠窗口：窗口结束后即过期
                cred.not_after = min(cred.not_after, now + self.rotation_overlap)
        device.credentials.append(
            Credential(secret=new_secret, not_before=now, not_after=float("inf"))
        )

    def revoke(self, device_id: str) -> None:
        self._devices[device_id].status = DeviceStatus.REVOKED

    def authenticate(self, device_id: str, secret: str, now: float) -> AuthResult:
        device = self._devices.get(device_id)
        if device is None:
            return AuthResult(ok=False, failure=AuthFailure.UNKNOWN_DEVICE)
        if device.status is DeviceStatus.REVOKED:
            return AuthResult(ok=False, failure=AuthFailure.DEVICE_REVOKED)
        matched = [c for c in device.credentials if c.secret == secret]
        if not matched:
            return AuthResult(ok=False, failure=AuthFailure.BAD_CREDENTIAL)
        if any(c.valid_at(now) for c in matched):
            return AuthResult(ok=True)
        # 密钥匹配但已不在有效期（含轮换窗口结束后的旧凭证）
        return AuthResult(ok=False, failure=AuthFailure.CREDENTIAL_EXPIRED)

    def get(self, device_id: str) -> Device | None:
        return self._devices.get(device_id)
