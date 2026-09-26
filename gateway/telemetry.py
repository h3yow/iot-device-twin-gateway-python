"""遥测上报的幂等与顺序处理。"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from .models import (
    PROTOCOL_VERSION,
    TelemetryMessage,
    TelemetryResult,
    TelemetryVerdict,
)
from .registry import DeviceRegistry


@dataclass
class _DeviceStream:
    """单设备上报流的判定状态。"""

    max_seq: int = 0
    seen: set[int] = field(default_factory=set)  # 已接受序号，用于去重


class TelemetryProcessor:
    """按设备维度判定上报。

    判定顺序（先判先出，保证结论确定且原因可区分）：
    1. 认证失败            -> REJECTED_UNAUTHORIZED（附 AuthFailure）
    2. 畸形负载            -> REJECTED_MALFORMED
    3. 未知协议版本        -> REJECTED_UNKNOWN_VERSION
    4. 重复序号            -> DUPLICATE（不重复计数、不改状态）
    5. 时间戳过期          -> STALE_TIMESTAMP（拒绝，不计数）
    6. seq > max_seq + 1   -> GAP_DETECTED（接受，记录缺口）
       seq == max_seq + 1  -> ACCEPTED
       seq <= max_seq 且未见 -> ACCEPTED_OUT_OF_ORDER（接受，不回退）
    """

    def __init__(self, registry: DeviceRegistry, max_clock_skew: float = 300.0) -> None:
        self.registry = registry
        self.max_clock_skew = max_clock_skew
        self._streams: dict[str, _DeviceStream] = {}

    def _malformed_reason(self, msg: TelemetryMessage) -> str | None:
        if not isinstance(msg.device_id, str) or not msg.device_id:
            return "device_id must be a non-empty string"
        if not isinstance(msg.seq, int) or isinstance(msg.seq, bool) or msg.seq < 1:
            return "seq must be a positive integer"
        if (
            not isinstance(msg.timestamp, (int, float))
            or isinstance(msg.timestamp, bool)
            or not math.isfinite(msg.timestamp)
        ):
            return "timestamp must be a finite number"
        if not isinstance(msg.payload, dict):
            return "payload must be a dict"
        return None

    def ingest(self, msg: TelemetryMessage, now: float) -> TelemetryResult:
        auth = self.registry.authenticate(msg.device_id, msg.secret, now)
        if not auth.ok:
            return TelemetryResult(
                verdict=TelemetryVerdict.REJECTED_UNAUTHORIZED,
                auth_failure=auth.failure,
                detail=f"auth failed: {auth.failure.value}",
            )

        reason = self._malformed_reason(msg)
        if reason is not None:
            return TelemetryResult(
                verdict=TelemetryVerdict.REJECTED_MALFORMED, detail=reason
            )

        if msg.version != PROTOCOL_VERSION:
            return TelemetryResult(
                verdict=TelemetryVerdict.REJECTED_UNKNOWN_VERSION,
                detail=f"unsupported version {msg.version}, expect {PROTOCOL_VERSION}",
            )

        stream = self._streams.setdefault(msg.device_id, _DeviceStream())

        if msg.seq in stream.seen:
            return TelemetryResult(
                verdict=TelemetryVerdict.DUPLICATE,
                detail=f"seq {msg.seq} already accepted",
            )

        if msg.timestamp < now - self.max_clock_skew:
            return TelemetryResult(
                verdict=TelemetryVerdict.STALE_TIMESTAMP,
                detail=f"timestamp {msg.timestamp} older than skew window at {now}",
            )

        stream.seen.add(msg.seq)
        if msg.seq > stream.max_seq:
            verdict = (
                TelemetryVerdict.ACCEPTED
                if msg.seq == stream.max_seq + 1
                else TelemetryVerdict.GAP_DETECTED
            )
            detail = (
                "in order"
                if verdict is TelemetryVerdict.ACCEPTED
                else f"gap: expected {stream.max_seq + 1}, got {msg.seq}"
            )
            stream.max_seq = msg.seq
            return TelemetryResult(verdict=verdict, detail=detail)

        return TelemetryResult(
            verdict=TelemetryVerdict.ACCEPTED_OUT_OF_ORDER,
            detail=f"out of order: seq {msg.seq} behind max {stream.max_seq}, no rollback",
        )
