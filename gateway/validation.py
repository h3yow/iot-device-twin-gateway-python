"""上报内容合规判定：字段缺失、类型不符、取值/结构非法的确定性归类。

归类总规则（同时命中多个问题时按此固定优先级，只返回第一个命中项）：

1. 入口结构不是对象                      -> invalid_structure
2. 必填字段缺失/为空                     -> missing_field
3. 字段类型不符                          -> type_mismatch
4. 类型正确但取值非法                    -> invalid_value
5. 协议版本不支持                        -> unknown_protocol_version（独立拒绝类）
6. 时间戳越界                            -> stale_timestamp（独立拒绝类）
7. payload 负载体递归校验，深度优先、首个命中即返回

信封字段固定检查顺序：message_id -> seq -> timestamp -> protocol_version -> payload。
因此对任意输入，结论都是确定、可解释、可复现的。
"""
from __future__ import annotations

import math
from typing import Any

from .models import (
    IngestResult,
    PayloadIssue,
    RejectReason,
    SUPPORTED_PROTOCOL_VERSIONS,
    TelemetryMessage,
)


# 协议契约允许的遥测值类型（bool 是 int 子类，单独先判 bool 以避免混淆）
_SCALAR_TYPES = (int, float, str, bool)
_MAX_DEPTH = 8
_MAX_CONTAINER_SIZE = 256
_MAX_STR_LEN = 4096

# 批量条目中信封层的必填字段（设备身份/凭证在批级统一提供）
_REQUIRED_FIELDS: tuple[str, ...] = ("message_id", "seq", "timestamp", "payload")


def reject(issue: PayloadIssue, detail: str, message_id: str | None = None,
           msg_seq: int | None = None) -> IngestResult:
    """构造一条 MALFORMED_PAYLOAD 拒绝结果。"""
    return IngestResult(
        accepted=False,
        reject_reason=RejectReason.MALFORMED_PAYLOAD,
        payload_issue=issue,
        issue_detail=detail,
        message_id=message_id,
        msg_seq=msg_seq,
    )


def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_real_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_scalar(path: str, value: Any) -> IngestResult | None:
    if isinstance(value, bool):
        return None  # bool 是合法标量
    if isinstance(value, str):
        if len(value) > _MAX_STR_LEN:
            return reject(
                PayloadIssue.INVALID_VALUE,
                f"{path}: string length {len(value)} exceeds limit {_MAX_STR_LEN}")
        return None
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return reject(
                PayloadIssue.INVALID_VALUE,
                f"{path}: number must be finite (got {value!r})")
        return None
    return None


def _check_value(path: str, value: Any, depth: int) -> IngestResult | None:
    """递归校验单个遥测值，深度优先、按插入顺序，首个命中即返回。"""
    if value is None:
        return reject(PayloadIssue.INVALID_VALUE, f"{path}: value must not be null")
    if isinstance(value, dict):
        if depth > _MAX_DEPTH:
            return reject(PayloadIssue.INVALID_STRUCTURE,
                          f"{path}: nesting depth exceeds limit {_MAX_DEPTH}")
        if len(value) > _MAX_CONTAINER_SIZE:
            return reject(PayloadIssue.INVALID_STRUCTURE,
                          f"{path}: object size {len(value)} exceeds limit {_MAX_CONTAINER_SIZE}")
        for key, sub in value.items():
            if not isinstance(key, str) or not key:
                return reject(PayloadIssue.INVALID_STRUCTURE,
                              f"{path}: object keys must be non-empty strings (got {key!r})")
            sub_path = f"{path}.{key}" if path else key
            hit = _check_value(sub_path, sub, depth + 1)
            if hit is not None:
                return hit
        return None
    if isinstance(value, list):
        if depth > _MAX_DEPTH:
            return reject(PayloadIssue.INVALID_STRUCTURE,
                          f"{path}: nesting depth exceeds limit {_MAX_DEPTH}")
        if len(value) > _MAX_CONTAINER_SIZE:
            return reject(PayloadIssue.INVALID_STRUCTURE,
                          f"{path}: array size {len(value)} exceeds limit {_MAX_CONTAINER_SIZE}")
        for i, sub in enumerate(value):
            hit = _check_value(f"{path}[{i}]", sub, depth + 1)
            if hit is not None:
                return hit
        return None
    if isinstance(value, _SCALAR_TYPES):
        return _check_scalar(path, value)
    # JSON 之外的类型（bytes/set/自定义对象等）
    return reject(PayloadIssue.INVALID_STRUCTURE,
                  f"{path}: unsupported value type {type(value).__name__}")


def _check_payload(payload: Any, msg_seq: int | None = None) -> IngestResult | None:
    if not isinstance(payload, dict):
        return reject(PayloadIssue.INVALID_STRUCTURE,
                      "payload: must be a JSON object", msg_seq=msg_seq)
    if not payload:
        return reject(PayloadIssue.INVALID_STRUCTURE,
                      "payload: must be a non-empty object", msg_seq=msg_seq)
    hit = _check_value("payload", payload, depth=1)
    if hit is None:
        return None
    # 透传消息序号，便于排查视图对位（detail/payload_issue 保持首个命中项）
    return IngestResult(accepted=False, reject_reason=hit.reject_reason,
                        payload_issue=hit.payload_issue,
                        issue_detail=hit.issue_detail,
                        message_id=hit.message_id, msg_seq=msg_seq)


def _check_envelope_fields(raw: dict[str, Any]) -> IngestResult | None:
    # 1) 必填字段存在性（缺失优先于类型，先统一检查一遍）
    mid_value = raw.get("message_id")
    safe_mid = mid_value if isinstance(mid_value, str) else None
    seq_value = raw.get("seq")
    safe_seq = seq_value if _is_real_int(seq_value) else None
    for name in _REQUIRED_FIELDS:
        if name not in raw or raw[name] is None:
            return reject(PayloadIssue.MISSING_FIELD,
                          f"missing required field: {name}",
                          message_id=safe_mid, msg_seq=safe_seq)
    message_id = raw["message_id"]
    # 2) message_id
    if not isinstance(message_id, str):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      "message_id: expected string", msg_seq=safe_seq)
    if not message_id.strip():
        return reject(PayloadIssue.MISSING_FIELD,
                      "message_id: must not be empty", msg_seq=safe_seq)
    # 3) seq
    seq = raw["seq"]
    if not _is_real_int(seq):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      "seq: expected integer", message_id=message_id)
    if seq < 1:
        return reject(PayloadIssue.INVALID_VALUE,
                      f"seq: must be >= 1 (got {seq})", message_id=message_id)
    # 4) timestamp
    ts = raw["timestamp"]
    if not _is_real_number(ts):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      "timestamp: expected number", message_id=message_id,
                      msg_seq=safe_seq)
    if not math.isfinite(float(ts)):
        return reject(PayloadIssue.INVALID_VALUE,
                      "timestamp: must be a finite number",
                      message_id=message_id, msg_seq=safe_seq)
    # 5) protocol_version（可选，缺省按 1 处理）
    pv = raw.get("protocol_version", 1)
    if not _is_real_int(pv):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      "protocol_version: expected integer",
                      message_id=message_id, msg_seq=safe_seq)
    return None


def check_envelope(raw: Any) -> IngestResult | None:
    """对不可信的原始条目做信封级合规校验（批量入口防御性使用）。

    返回 None 表示通过；否则返回结构化 MALFORMED_PAYLOAD 结果。
    """
    if not isinstance(raw, dict):
        return reject(PayloadIssue.INVALID_STRUCTURE,
                      f"entry: must be a JSON object (got {type(raw).__name__})")
    return _check_envelope_fields(raw)


def check_message(msg: TelemetryMessage, now: float,
                  max_clock_skew_seconds: float) -> IngestResult | None:
    """完整合规校验：信封 -> 协议版本 -> 时间戳 -> 负载体。

    已构造消息的 device_id/secret 由调用链保证，这里只校验可能出现空值/越界的
    消息字段；返回 None 表示通过。
    """
    safe_seq = msg.seq if _is_real_int(msg.seq) else None
    if not isinstance(msg.message_id, str):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      f"message_id: expected string (got {type(msg.message_id).__name__})",
                      msg_seq=safe_seq)
    if not msg.message_id.strip():
        return reject(PayloadIssue.MISSING_FIELD,
                      "message_id: must not be empty", msg_seq=safe_seq)
    if not _is_real_int(msg.seq):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      f"seq: expected integer (got {type(msg.seq).__name__})",
                      message_id=msg.message_id, msg_seq=safe_seq)
    if msg.seq < 1:
        return reject(PayloadIssue.INVALID_VALUE,
                      f"seq: must be >= 1 (got {msg.seq})",
                      message_id=msg.message_id, msg_seq=safe_seq)
    if not _is_real_number(msg.timestamp):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      f"timestamp: expected number (got {type(msg.timestamp).__name__})",
                      message_id=msg.message_id, msg_seq=safe_seq)
    if not math.isfinite(float(msg.timestamp)):
        return reject(PayloadIssue.INVALID_VALUE,
                      "timestamp: must be a finite number",
                      message_id=msg.message_id, msg_seq=safe_seq)
    if not _is_real_int(msg.protocol_version):
        return reject(PayloadIssue.TYPE_MISMATCH,
                      "protocol_version: expected integer",
                      message_id=msg.message_id, msg_seq=safe_seq)
    if msg.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
        return IngestResult(
            accepted=False,
            reject_reason=RejectReason.UNKNOWN_PROTOCOL_VERSION,
            issue_detail=(f"protocol_version {msg.protocol_version} not supported; "
                          f"supported={sorted(SUPPORTED_PROTOCOL_VERSIONS)}"),
            message_id=msg.message_id, msg_seq=safe_seq)
    if abs(msg.timestamp - now) > max_clock_skew_seconds:
        return IngestResult(
            accepted=False,
            reject_reason=RejectReason.STALE_TIMESTAMP,
            issue_detail=(f"timestamp {msg.timestamp} deviates from now {now} "
                          f"by more than {max_clock_skew_seconds}s"),
            message_id=msg.message_id, msg_seq=safe_seq)
    return _check_payload(msg.payload, msg_seq=safe_seq)
