"""门面：串联注册、遥测与孪生。"""
from __future__ import annotations

from typing import Any

from .models import (
    AuthResult,
    ConflictPolicy,
    Device,
    TelemetryMessage,
    TelemetryResult,
    TelemetryVerdict,
    Twin,
)
from .registry import DeviceRegistry
from .telemetry import TelemetryProcessor
from .twin import DesiredUpdateResult, TwinStore

_APPLIED_VERDICTS = {
    TelemetryVerdict.ACCEPTED,
    TelemetryVerdict.GAP_DETECTED,
    TelemetryVerdict.ACCEPTED_OUT_OF_ORDER,
}


class GatewayService:
    """进程内网关：注册 -> 认证 -> 遥测判定 -> 孪生收敛。"""

    def __init__(
        self,
        rotation_overlap: float = 60.0,
        max_clock_skew: float = 300.0,
        conflict_policy: ConflictPolicy = ConflictPolicy.LAST_WRITER_WINS,
    ) -> None:
        self.registry = DeviceRegistry(rotation_overlap=rotation_overlap)
        self.telemetry = TelemetryProcessor(self.registry, max_clock_skew=max_clock_skew)
        self.twins = TwinStore(policy=conflict_policy)

    # --- 设备生命周期 ---
    def register(self, device_id: str, secret: str, now: float) -> Device:
        return self.registry.register(device_id, secret, now)

    def rotate_credential(self, device_id: str, new_secret: str, now: float) -> None:
        self.registry.rotate_credential(device_id, new_secret, now)

    def revoke(self, device_id: str) -> None:
        self.registry.revoke(device_id)

    def authenticate(self, device_id: str, secret: str, now: float) -> AuthResult:
        return self.registry.authenticate(device_id, secret, now)

    # --- 遥测接入 ---
    def ingest(self, msg: TelemetryMessage, now: float) -> TelemetryResult:
        result = self.telemetry.ingest(msg, now)
        if result.verdict in _APPLIED_VERDICTS:
            # 孪生层以 seq 为版本兜底：乱序/重复不会回退或重复计数
            self.twins.apply_reported(msg.device_id, msg.payload, msg.seq, now)
        return result

    # --- 孪生 ---
    def get_twin(self, device_id: str) -> Twin:
        return self.twins.get(device_id)

    def update_desired(
        self,
        device_id: str,
        value: dict[str, Any],
        base_version: int,
        writer_id: str,
        now: float,
    ) -> DesiredUpdateResult:
        return self.twins.update_desired(device_id, value, base_version, writer_id, now)
