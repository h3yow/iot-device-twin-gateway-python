"""共享类型定义：状态枚举、判定原因与数据模型。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

PROTOCOL_VERSION = 1


class DeviceStatus(enum.Enum):
    ACTIVE = "active"
    REVOKED = "revoked"


class AuthFailure(enum.Enum):
    """认证失败原因，彼此可区分。"""

    UNKNOWN_DEVICE = "unknown_device"
    DEVICE_REVOKED = "device_revoked"
    BAD_CREDENTIAL = "bad_credential"
    CREDENTIAL_EXPIRED = "credential_expired"


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    failure: AuthFailure | None = None


@dataclass
class Credential:
    secret: str
    not_before: float
    not_after: float  # float("inf") 表示不过期

    def valid_at(self, now: float) -> bool:
        return self.not_before <= now < self.not_after


@dataclass
class Device:
    device_id: str
    status: DeviceStatus = DeviceStatus.ACTIVE
    credentials: list[Credential] = field(default_factory=list)


class TelemetryVerdict(enum.Enum):
    ACCEPTED = "accepted"            # 新鲜且按序
    ACCEPTED_OUT_OF_ORDER = "accepted_out_of_order"  # 乱序但不回退
    DUPLICATE = "duplicate"          # 重复消息，不重复计数
    GAP_DETECTED = "gap_detected"    # 接受但存在序号缺口
    STALE_TIMESTAMP = "stale_timestamp"  # 时间戳过期
    REJECTED_MALFORMED = "rejected_malformed"        # 畸形负载
    REJECTED_UNKNOWN_VERSION = "rejected_unknown_version"  # 未知协议版本
    REJECTED_UNAUTHORIZED = "rejected_unauthorized"  # 认证失败


@dataclass(frozen=True)
class TelemetryMessage:
    device_id: str
    secret: str
    seq: int
    timestamp: float
    payload: dict[str, Any]
    version: int = PROTOCOL_VERSION


@dataclass(frozen=True)
class TelemetryResult:
    verdict: TelemetryVerdict
    auth_failure: AuthFailure | None = None
    detail: str = ""


class ConflictPolicy(enum.Enum):
    """期望状态并发冲突的确定性收敛规则。"""

    LAST_WRITER_WINS = "last_writer_wins"  # 高版本覆盖，同版本按 writer_id 字典序
    REJECT_ON_CONFLICT = "reject_on_conflict"  # 基版本不匹配则明确拒绝


@dataclass
class VersionedValue:
    value: dict[str, Any]
    version: int
    writer_id: str = ""
    updated_at: float = 0.0


@dataclass
class Twin:
    device_id: str
    desired: VersionedValue = field(default_factory=lambda: VersionedValue({}, 0))
    reported: VersionedValue = field(default_factory=lambda: VersionedValue({}, 0))

    @property
    def converged(self) -> bool:
        return self.desired.value == self.reported.value
