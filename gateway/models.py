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
    MALFORMED_PAYLOAD = "malformed_payload"          # 负载不合规（含结构/字段/类型/取值）
    UNKNOWN_PROTOCOL_VERSION = "unknown_protocol_version"  # 协议版本不支持
    STALE_TIMESTAMP = "stale_timestamp"              # 时间戳超出允许偏移


class PayloadIssue(enum.Enum):
    """``MALFORMED_PAYLOAD`` 下可解释的细分子类（归类规则见 README）。"""
    INVALID_STRUCTURE = "invalid_structure"  # 顶层/嵌套结构非法（非对象、键非法等）
    MISSING_FIELD = "missing_field"          # 必填字段缺失或为空
    TYPE_MISMATCH = "type_mismatch"          # 字段类型与协议契约不符
    INVALID_VALUE = "invalid_value"          # 类型正确但取值不合法（如 seq<1、NaN）


class RejectCategory(enum.Enum):
    """排查视图使用的一级分类：每条拒绝恰好归入其中一类。"""
    AUTH_FAILURE = "auth_failure"
    MALFORMED_PAYLOAD = "malformed_payload"
    UNKNOWN_PROTOCOL_VERSION = "unknown_protocol_version"
    STALE_TIMESTAMP = "stale_timestamp"


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
    payload_issue: PayloadIssue | None = None       # reject_reason=MALFORMED 时的细分子类
    issue_detail: str = ""                          # 人类可读、可写入文档的命中规则说明
    applied_seq: int | None = None
    message_id: str | None = None                   # 原始条目的幂等键（便于批量结果对位）
    msg_seq: int | None = None                      # 原始条目序号（排查视图留痕用，含被拒绝值）


@dataclass(frozen=True)
class BatchItemResult:
    """批量补传中单条上报的判定：索引与 message_id 用于和提交顺序对位。"""
    index: int
    message_id: str | None
    accepted: bool
    flags: tuple[MessageFlag, ...] = ()
    reject_reason: RejectReason | None = None
    payload_issue: PayloadIssue | None = None
    issue_detail: str = ""
    applied_seq: int | None = None


@dataclass(frozen=True)
class BatchResult:
    """整批补传结论：批级认证结论 + 每条各自的判定。"""
    batch_id: str | None
    auth_ok: bool
    auth_failure: AuthFailure | None = None
    items: tuple[BatchItemResult, ...] = ()

    @property
    def accepted_count(self) -> int:
        return sum(1 for i in self.items if i.accepted)


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


# ---------------------------------------------------------------------------
# 排查视图：被拒绝上报的留存记录
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RejectionRecord:
    """一次被拒绝上报的结构化留痕，供运维按设备/分类检索。"""
    seq_no: int                          # 视图内单调递增的全局序号
    received_at: float                   # 网关接收时刻（epoch 秒）
    device_id: str | None
    category: RejectCategory             # 一级分类
    reason: str                          # 细分原因代码（AuthFailure/RejectReason/PayloadIssue 的值）
    detail: str                          # 命中的具体规则说明
    message_id: str | None = None
    msg_seq: int | None = None
    raw: Any = None                      # 原始上报内容的可序列化快照（可能被截断）
