"""网关门面：组合注册表、遥测处理与孪生存储。"""
from __future__ import annotations

from typing import Any

from .models import IngestResult, TelemetryMessage, TwinState, WriteResult
from .registry import DeviceRegistry
from .telemetry import TelemetryProcessor
from .twin import TwinStore


class Gateway:
    """对外统一入口：注册/轮换/撤销/认证/上报/期望配置。"""

    def __init__(self, rotation_overlap_seconds: float = 300.0,
                 max_clock_skew_seconds: float = 300.0) -> None:
        self.registry = DeviceRegistry(rotation_overlap_seconds)
        self.telemetry = TelemetryProcessor(max_clock_skew_seconds)
        self.twins = TwinStore()

    def ingest(self, msg: TelemetryMessage, now: float) -> IngestResult:
        """完整上报链路：认证 -> 负载校验 -> 幂等/顺序判定 -> 应用到孪生。"""
        auth = self.registry.authenticate(msg.device_id, msg.secret, now)
        if not auth.ok:
            return IngestResult(accepted=False, auth_failure=auth.failure)
        rejection = self.telemetry.validate(msg, now)
        if rejection is not None:
            return rejection
        result = self.telemetry.classify(msg)
        if result.accepted:
            self.twins.apply_reported(msg.device_id, msg.payload)
        return result

    def get_twin(self, device_id: str) -> TwinState:
        return self.twins.get(device_id)

    def set_desired(self, device_id: str, desired: dict[str, Any],
                    expected_version: int) -> WriteResult:
        return self.twins.set_desired(device_id, desired, expected_version)
