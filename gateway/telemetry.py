"""遥测上报处理：幂等、顺序、缺口与畸形负载判定。"""
from __future__ import annotations

import threading
from typing import Any

from .models import (
    IngestResult,
    MessageFlag,
    RejectReason,
    SUPPORTED_PROTOCOL_VERSIONS,
    TelemetryMessage,
)


class TelemetryProcessor:
    """按设备维度判定重复/乱序/缺口/过期时间戳/畸形负载。

    顺序模型：每设备维护已应用的最大 seq 水位 watermark（初始 0）。
    - seq == watermark + 1      -> ACCEPTED，应用并推进水位
    - seq >  watermark + 1      -> GAP，应用并推进水位（确定性前进而非回退）
    - seq <= watermark          -> OUT_OF_ORDER，不应用，状态不回退
    - message_id 已见           -> DUPLICATE，幂等忽略，不重复计数
    """

    def __init__(self, max_clock_skew_seconds: float = 300.0) -> None:
        self._lock = threading.RLock()
        self._max_skew = max_clock_skew_seconds
        self._seen_ids: dict[str, set[str]] = {}
        self._watermark: dict[str, int] = {}

    def validate(self, msg: TelemetryMessage, now: float) -> IngestResult | None:
        """负载层面校验；返回 None 表示通过，否则返回可区分的拒绝结果。"""
        if msg.protocol_version not in SUPPORTED_PROTOCOL_VERSIONS:
            return IngestResult(
                accepted=False,
                reject_reason=RejectReason.UNKNOWN_PROTOCOL_VERSION,
                detail=f"unsupported_protocol_version:{msg.protocol_version}")
        if not isinstance(msg.payload, dict) or not msg.payload:
            return IngestResult(
                accepted=False, reject_reason=RejectReason.MALFORMED_PAYLOAD,
                detail="payload must be a non-empty object")
        if abs(msg.timestamp - now) > self._max_skew:
            return IngestResult(
                accepted=False, reject_reason=RejectReason.STALE_TIMESTAMP,
                detail=f"clock_skew_exceeded:{abs(msg.timestamp - now):.1f}s"
                       f" > {self._max_skew:.1f}s")
        return None

    def classify(self, msg: TelemetryMessage) -> IngestResult:
        """幂等与顺序判定。accepted=True 时调用方应将 payload 应用到孪生。"""
        with self._lock:
            seen = self._seen_ids.setdefault(msg.device_id, set())
            if msg.message_id in seen:
                return IngestResult(
                    accepted=False, flags=(MessageFlag.DUPLICATE,),
                    applied_seq=self._watermark.get(msg.device_id, 0))
            watermark = self._watermark.get(msg.device_id, 0)
            if msg.seq <= watermark:
                # 乱序旧消息：记录 message_id 保证其重发判重，但不应用、不回退
                seen.add(msg.message_id)
                return IngestResult(accepted=False,
                                    flags=(MessageFlag.OUT_OF_ORDER,),
                                    applied_seq=watermark)
            flags = (MessageFlag.GAP,) if msg.seq > watermark + 1 \
                else (MessageFlag.ACCEPTED,)
            seen.add(msg.message_id)
            self._watermark[msg.device_id] = msg.seq
            return IngestResult(accepted=True, flags=flags, applied_seq=msg.seq)

    def applied_seq(self, device_id: str) -> int:
        with self._lock:
            return self._watermark.get(device_id, 0)
