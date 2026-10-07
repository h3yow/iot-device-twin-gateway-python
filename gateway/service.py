"""网关门面：组合注册表、遥测处理、孪生存储与排查视图。"""
from __future__ import annotations

import logging
import threading
from typing import Any

from .models import (
    BatchResult,
    IngestResult,
    RejectCategory,
    TelemetryMessage,
    TwinState,
    WriteResult,
)
from .rejection import RejectionView
from .registry import DeviceRegistry
from .telemetry import DEFAULT_BATCH_CACHE, TelemetryProcessor
from .twin import TwinStore
from .validation import check_envelope, check_message

log = logging.getLogger(__name__)


def _category_for(result: IngestResult) -> RejectCategory | None:
    """把一条拒绝结论映射到排查视图的一级分类。"""
    if result.auth_failure is not None:
        return RejectCategory.AUTH_FAILURE
    if result.reject_reason is not None:
        return RejectCategory(result.reject_reason.value)
    return None


class Gateway:
    """对外统一入口：注册/轮换/撤销/认证/单条上报/批量补传/期望配置。"""

    def __init__(self, rotation_overlap_seconds: float = 300.0,
                 max_clock_skew_seconds: float = 300.0,
                 rejection_capacity: int = 1000,
                 batch_cache_size: int = DEFAULT_BATCH_CACHE) -> None:
        self.registry = DeviceRegistry(rotation_overlap_seconds)
        self.telemetry = TelemetryProcessor(batch_cache_size=batch_cache_size)
        self.twins = TwinStore()
        self.rejections = RejectionView(capacity=rejection_capacity)
        self._max_skew = max_clock_skew_seconds
        # 每设备串行化 ingest 临界区，保证单条与批量交错时孪生不会被旧值回退
        self._device_locks: dict[str, threading.RLock] = {}
        self._device_locks_guard = threading.Lock()

    def _device_lock(self, device_id: str) -> threading.RLock:
        with self._device_locks_guard:
            lock = self._device_locks.get(device_id)
            if lock is None:
                lock = threading.RLock()
                self._device_locks[device_id] = lock
            return lock

    def _record_rejection(self, now: float, device_id: str | None,
                          result: IngestResult, raw: Any) -> None:
        category = _category_for(result)
        if category is None:
            return
        if result.auth_failure is not None:
            reason, detail = result.auth_failure.value, "authentication failed"
        else:
            reason = result.reject_reason.value  # type: ignore[union-attr]
            detail = result.issue_detail
        self.rejections.record(
            received_at=now, device_id=device_id, category=category,
            reason=result.payload_issue.value if result.payload_issue else reason,
            detail=detail, message_id=result.message_id,
            msg_seq=result.msg_seq, raw=raw)

    def ingest(self, msg: TelemetryMessage, now: float) -> IngestResult:
        """完整单条上报链路：认证 -> 合规校验 -> 幂等/顺序判定 -> 应用到孪生。

        任何输入都返回结构化判定；拒绝写入排查视图，不写孪生、不推进顺序。
        """
        lock = self._device_lock(msg.device_id)
        with lock:
            auth = self.registry.authenticate(msg.device_id, msg.secret, now)
            if not auth.ok:
                safe_mid = msg.message_id if isinstance(msg.message_id, str) else None
                safe_seq = msg.seq if isinstance(msg.seq, int) and not isinstance(
                    msg.seq, bool) else None
                result = IngestResult(accepted=False, auth_failure=auth.failure,
                                      message_id=safe_mid, msg_seq=safe_seq)
                log.info("单条上报被拒: device=%s mid=%s 规则=auth:%s",
                         msg.device_id, result.message_id, auth.failure)
                self._record_rejection(now, msg.device_id, result,
                                       _snapshot(msg))
                return result
            rejection = check_message(msg, now, self._max_skew)
            if rejection is not None:
                log.info("单条上报被拒: device=%s mid=%s 规则=%s/%s | %s",
                         msg.device_id, msg.message_id,
                         rejection.reject_reason, rejection.payload_issue,
                         rejection.issue_detail)
                self._record_rejection(now, msg.device_id, rejection,
                                       _snapshot(msg))
                return rejection
            result = self.telemetry.classify(msg)
            if result.accepted:
                self.twins.apply_reported(msg.device_id, msg.payload)
            log.info("单条上报: device=%s mid=%s seq=%s -> %s",
                     msg.device_id, msg.message_id, msg.seq,
                     result.flags[0].value if result.flags else "?")
            return result

    def ingest_batch(self, batch_id: str | None, device_id: str, secret: str,
                     raw_items: list[Any], now: float) -> BatchResult:
        """批量补传：批级一次认证；每条各自判定；不合规条目不连累其他条目。

        同一 batch_id 重试时整批判定幂等（批缓存 + message_id 双重保证）。
        批内排列不影响最终孪生收敛（内部按 (seq, message_id) 排序后判定）。
        """
        lock = self._device_lock(device_id)
        with lock:
            auth = self.registry.authenticate(device_id, secret, now)
            if not auth.ok:
                result = IngestResult(accepted=False, auth_failure=auth.failure)
                self._record_rejection(now, device_id, result, {
                    "batch_id": batch_id, "items": raw_items})
                log.info("批量补传被拒: device=%s batch_id=%s 规则=auth:%s",
                         device_id, batch_id, auth.failure)
                return BatchResult(batch_id=batch_id, auth_ok=False,
                                   auth_failure=auth.failure, items=())

            # 批级幂等：已成功处理过的 batch_id 原样返回首次判定，不重复计数、
            # 不重复留痕、不再应用孪生
            cached = self.telemetry.lookup_batch(device_id, batch_id)
            if cached is not None:
                log.info("批量补传命中批缓存: device=%s batch_id=%s -> 原样返回首次判定",
                         device_id, batch_id)
                return BatchResult(batch_id=batch_id, auth_ok=True, items=cached)

            messages: list[TelemetryMessage | None] = []
            prefilled: dict[int, IngestResult] = {}
            for idx, raw in enumerate(raw_items):
                bad = check_envelope(raw)
                if bad is not None:
                    messages.append(None)
                    prefilled[idx] = bad
                    self._record_rejection(now, device_id, bad, {
                        "batch_id": batch_id, "index": idx, "entry": raw})
                    log.info("批量条目不合规: device=%s batch_id=%s idx=%d 规则=%s/%s | %s",
                             device_id, batch_id, idx, bad.reject_reason,
                             bad.payload_issue, bad.issue_detail)
                    continue
                pv = raw.get("protocol_version", 1)
                msg = TelemetryMessage(
                    device_id=device_id, secret=secret,
                    message_id=raw["message_id"], seq=raw["seq"],
                    timestamp=raw["timestamp"], protocol_version=pv,
                    payload=raw["payload"])
                bad = check_message(msg, now, self._max_skew)
                if bad is not None:
                    messages.append(None)
                    prefilled[idx] = bad
                    self._record_rejection(now, device_id, bad, {
                        "batch_id": batch_id, "index": idx, "entry": raw})
                    log.info("批量条目被拒: device=%s batch_id=%s idx=%s 规则=%s | %s",
                             device_id, batch_id, idx, bad.reject_reason,
                             bad.issue_detail)
                    continue
                messages.append(msg)

            verdicts, applied = self.telemetry.classify_batch(
                device_id, batch_id, messages, prefilled)
            # 与判定期同锁，且严格按规范序 (seq,message_id) 应用，
            # 因此批内任意排列最终孪生收敛一致、不会被旧值回退
            for i in applied:
                self.twins.apply_reported(device_id, messages[i].payload)  # type: ignore[union-attr]
            return BatchResult(batch_id=batch_id, auth_ok=True, items=verdicts)

    def get_twin(self, device_id: str) -> TwinState:
        return self.twins.get(device_id)

    def set_desired(self, device_id: str, desired: dict[str, Any],
                    expected_version: int) -> WriteResult:
        return self.twins.set_desired(device_id, desired, expected_version)


def _safe_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _snapshot(msg: TelemetryMessage) -> dict[str, Any]:
    """单条上报原始内容快照（用于排查视图留痕）。"""
    return {
        "device_id": msg.device_id,
        "message_id": _safe_str(msg.message_id),
        "seq": msg.seq if isinstance(msg.seq, int) else repr(msg.seq),
        "timestamp": msg.timestamp,
        "protocol_version": msg.protocol_version,
        "payload": msg.payload,
    }
