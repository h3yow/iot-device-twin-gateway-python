"""核心类型定义：设备、凭证、遥测消息、孪生状态与判定结果。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# 枚举：所有可区分的判定原因
# ---------------------------------------------------------------------------

class DeviceStatus(enum.Enum):
    ACTIVE = "active"
    REVOKED = "revoked"


class AuthFailure(enum.Enum):
    """认证失败原因，彼此可区分。"""
    UNKNOWN_DEVICE = "unknown_device"        # 设备不存在
    DEVICE_REVOKED = "device_revoked"        # 设备已撤销
    UNKNOWN_CREDENTIAL = "unknown_credential"  # 凭证指纹不匹配
    CREDENTIAL_EXPIRED = "credential_expired"  # 凭证已过期（含轮换窗口结束）


class RejectReason(enum.Enum):
    """遥测负载层面的拒绝原因（认证通过之后）。"""
    MALFORMED_PAYLOAD = "malformed_payload"          # 负载结构非法
    MISSING_FIELD = "missing_field"                  # 必填字段缺失
    INVALID_FIELD_TYPE = "invalid_field_type"        # 字段类型不符
    INVALID_FIELD_VALUE = "invalid_field_value"      # 字段取值非法
    UNKNOWN_PROTOCOL_VERSION = "unknown_protocol_version"  # 协议版本不支持
    STALE_TIMESTAMP = "stale_timestamp"              # 时间戳超出允许偏移


class RejectionCategory(enum.Enum):
    """排查视图的分类维度：所有未接受结论都归入且仅归入一类。"""
    AUTH_FAILURE = "auth_failure"                    # 认证失败
    PAYLOAD_NON_COMPLIANT = "payload_non_compliant"  # 负载不合规
    UNKNOWN_PROTOCOL_VERSION = "unknown_protocol_version"  # 协议版本不支持
    STALE_TIMESTAMP = "stale_timestamp"              # 时间戳超偏
    DUPLICATE = "duplicate"                          # 重复（幂等忽略）
    OUT_OF_ORDER = "out_of_order"                    # 乱序（不回退）


#: RejectReason -> RejectionCategory 的确定映射。
#: 字段缺失/类型不符/取值非法/结构非法统一归入“负载不合规”，
#: 与其余拒绝原因（协议版本、时间戳）清楚区分。
REJECT_REASON_CATEGORY: dict[RejectReason, RejectionCategory] = {
    RejectReason.MALFORMED_PAYLOAD: RejectionCategory.PAYLOAD_NON_COMPLIANT,
    RejectReason.MISSING_FIELD: RejectionCategory.PAYLOAD_NON_COMPLIANT,
    RejectReason.INVALID_FIELD_TYPE: RejectionCategory.PAYLOAD_NON_COMPLIANT,
    RejectReason.INVALID_FIELD_VALUE: RejectionCategory.PAYLOAD_NON_COMPLIANT,
    RejectReason.UNKNOWN_PROTOCOL_VERSION: RejectionCategory.UNKNOWN_PROTOCOL_VERSION,
    RejectReason.STALE_TIMESTAMP: RejectionCategory.STALE_TIMESTAMP,
}


class MessageFlag(enum.Enum):
    """消息被接受时的标记（非拒绝）。"""
    ACCEPTED = "accepted"          # 正常按序接受
    DUPLICATE = "duplicate"        # 重复消息，幂等忽略，不重复计数
    OUT_OF_ORDER = "out_of_order"  # 乱序：seq 落后于已应用水位，不回退状态
    GAP = "gap"                    # 存在缺口：seq 跳过了一个或多个序号


class ConflictPolicy(enum.Enum):
    """孪生写冲突的确定性收敛规则。"""
    REJECT = "reject"              # 乐观并发：版本不符直接拒绝
    LAST_WRITER_WINS = "last_writer_wins"  # 确定性取版本较大者收敛


# ---------------------------------------------------------------------------
# 设备与凭证
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Credential:
    """设备凭证。轮换期间新旧凭证按重叠窗口共存。"""
    secret: str
    not_before: float          # 生效时刻（epoch 秒）
    not_after: float           # 失效时刻（epoch 秒）

    def is_valid_at(self, now: float) -> bool:
        return self.not_before <= now < self.not_after

    @property
    def fingerprint(self) -> str:
        # 进程内模拟：用 secret 本身作指纹，避免引入哈希细节
        return self.secret


@dataclass
class Device:
    device_id: str
    status: DeviceStatus = DeviceStatus.ACTIVE
    credentials: list[Credential] = field(default_factory=list)
    registered_at: float = 0.0


@dataclass(frozen=True)
class AuthResult:
    ok: bool
    device_id: str | None = None
    failure: AuthFailure | None = None


# ---------------------------------------------------------------------------
# 遥测
# ---------------------------------------------------------------------------

SUPPORTED_PROTOCOL_VERSIONS: frozenset[int] = frozenset({1})


@dataclass(frozen=True)
class TelemetryMessage:
    device_id: str
    secret: str
    message_id: str            # 幂等键
    seq: int                   # 设备侧单调序号
    timestamp: float           # 设备侧时间戳
    protocol_version: int
    payload: Any               # 期望为 dict[str, Any]


@dataclass(frozen=True)
class IngestResult:
    accepted: bool
    flags: tuple[MessageFlag, ...] = ()
    auth_failure: AuthFailure | None = None
    reject_reason: RejectReason | None = None
    applied_seq: int | None = None
    detail: str = ""           # 命中的具体规则，如 "missing_field:seq"


# ---------------------------------------------------------------------------
# 批量补传
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BatchItemResult:
    """批内单条结论：index 为该条在原始提交中的下标。"""
    index: int
    result: IngestResult


@dataclass(frozen=True)
class BatchResult:
    """整批结论。duplicate_batch=True 表示同一 batch_id 的重试，
    items 为首次处理时缓存的结论，本次未重复生效。"""
    batch_id: str
    items: tuple[BatchItemResult, ...]
    duplicate_batch: bool = False


# ---------------------------------------------------------------------------
# 排查视图
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RejectionRecord:
    """一条未接受上报的留痕：设备、分类、判定依据与原始内容。"""
    device_id: str | None
    category: RejectionCategory
    detail: str                # 判定依据（具体规则/失败原因/标记）
    message_id: str | None
    seq: int | None
    raw: Any                   # 原始内容，供排查回放
    recorded_at: float


# ---------------------------------------------------------------------------
# 设备孪生
# ---------------------------------------------------------------------------

@dataclass
class TwinState:
    """单设备孪生：期望值与上报值各自带版本。"""
    desired: dict[str, Any] = field(default_factory=dict)
    desired_version: int = 0
    reported: dict[str, Any] = field(default_factory=dict)
    reported_version: int = 0


@dataclass(frozen=True)
class WriteResult:
    ok: bool
    version: int = 0
    conflict: bool = False
    detail: str = ""
