"""原始上报的合规校验：把任意输入判定为 TelemetryMessage 或结构化拒绝。

归类规则（确定、可解释、可写文档）：
1. 条目本身不是对象 -> malformed_payload。
2. 按固定字段顺序 device_id, secret, message_id, seq, timestamp,
   protocol_version, payload 逐项检查；每项依次判 缺失 -> 类型 -> 取值。
3. 一条上报同时命中多个问题时，按上述顺序的第一条规则归类，
   detail 记录命中的规则与字段，便于排查与审计。
以上四类（结构非法/缺失/类型/取值）统一归入“负载不合规”分类，
与协议版本、时间戳等其他拒绝原因清楚区分。
"""
from __future__ import annotations

import math
from typing import Any

from .models import IngestResult, RejectReason, TelemetryMessage

#: 字段检查顺序固定，保证同一输入永远得到同一结论
_FIELD_ORDER = ("device_id", "secret", "message_id", "seq",
                "timestamp", "protocol_version", "payload")


def _reject(reason: RejectReason, detail: str) -> tuple[None, IngestResult]:
    return None, IngestResult(accepted=False, reject_reason=reason, detail=detail)


def _is_int(v: Any) -> bool:
    # bool 是 int 的子类，但 true/false 不是合法序号或版本号
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def parse_message(raw: Any) -> tuple[TelemetryMessage | None, IngestResult | None]:
    """校验原始输入。返回 (message, None) 或 (None, 拒绝结果)。

    任何离谱输入都返回结构化拒绝结果，绝不抛异常中断批处理。
    """
    if not isinstance(raw, dict):
        return _reject(RejectReason.MALFORMED_PAYLOAD,
                       f"entry must be an object, got {type(raw).__name__}")

    for name in _FIELD_ORDER:
        if name not in raw:
            return _reject(RejectReason.MISSING_FIELD, f"missing_field:{name}")
        value = raw[name]
        problem = _check_field(name, value)
        if problem is not None:
            return _reject(*problem)

    return TelemetryMessage(
        device_id=raw["device_id"], secret=raw["secret"],
        message_id=raw["message_id"], seq=raw["seq"],
        timestamp=float(raw["timestamp"]),
        protocol_version=raw["protocol_version"],
        payload=raw["payload"]), None


def _check_field(name: str, value: Any) -> tuple[RejectReason, str] | None:
    """单字段的类型与取值检查；返回 None 表示通过。"""
    if name in ("device_id", "secret", "message_id"):
        if not isinstance(value, str):
            return (RejectReason.INVALID_FIELD_TYPE,
                    f"invalid_type:{name} expected str")
        if not value:
            return (RejectReason.INVALID_FIELD_VALUE,
                    f"invalid_value:{name} must be non-empty")
        return None
    if name == "seq":
        if not _is_int(value):
            return (RejectReason.INVALID_FIELD_TYPE,
                    "invalid_type:seq expected int")
        if value < 1:
            return (RejectReason.INVALID_FIELD_VALUE,
                    "invalid_value:seq must be >= 1")
        return None
    if name == "timestamp":
        if not _is_number(value):
            return (RejectReason.INVALID_FIELD_TYPE,
                    "invalid_type:timestamp expected number")
        if not math.isfinite(value):
            return (RejectReason.INVALID_FIELD_VALUE,
                    "invalid_value:timestamp must be finite")
        return None
    if name == "protocol_version":
        if not _is_int(value):
            return (RejectReason.INVALID_FIELD_TYPE,
                    "invalid_type:protocol_version expected int")
        if value < 1:
            return (RejectReason.INVALID_FIELD_VALUE,
                    "invalid_value:protocol_version must be >= 1")
        return None
    # payload 只在此确认存在；结构（非空对象）由下游 validate 判定，
    # 与单条链路保持同一规则（malformed_payload）
    return None
