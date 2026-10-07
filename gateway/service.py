"""网关门面：组合注册表、遥测处理、孪生存储、合规校验与排查视图。"""
from __future__ import annotations

import threading
from typing import Any

from .compliance import parse_message
from .models import (
    AuthFailure,
    BatchItemResult,
    BatchResult,
    IngestResult,
    MessageFlag,
    REJECT_REASON_CATEGORY,
    RejectionCategory,
    RejectionRecord,
    TelemetryMessage,
    TwinState,
    WriteResult,
)
from .registry import DeviceRegistry
from .rejection_log import RejectionLog
from .telemetry import TelemetryProcessor
from .twin import TwinStore


class Gateway:
    """对外统一入口：注册/轮换/撤销/认证/上报（单条与批量）/期望配置/排查视图。"""

    def __init__(self, rotation_overlap_seconds: float = 300.0,
                 max_clock_skew_seconds: float = 300.0,
                 rejection_capacity: int = 1000) -> None:
        self.registry = DeviceRegistry(rotation_overlap_seconds)
        self.telemetry = TelemetryProcessor(max_clock_skew_seconds)
        self.twins = TwinStore()
        self.rejections = RejectionLog(rejection_capacity)
        self._batch_lock = threading.Lock()
        self._batches: dict[str, BatchResult] = {}

    # ------------------------------------------------------------------
    # 单条上报
    # ------------------------------------------------------------------

    def ingest(self, msg: TelemetryMessage, now: float) -> IngestResult:
        """完整上报链路：认证 -> 负载校验 -> 幂等/顺序判定 -> 应用到孪生。"""
        auth = self.registry.authenticate(msg.device_id, msg.secret, now)
        if not auth.ok:
            result = IngestResult(accepted=False, auth_failure=auth.failure)
            self._record(msg, result, raw=None, now=now)
            return result
        rejection = self.telemetry.validate(msg, now)
        if rejection is not None:
            self._record(msg, rejection, raw=None, now=now)
            return rejection
        result = self.telemetry.classify(msg)
        if result.accepted:
            self.twins.apply_reported(msg.device_id, msg.payload)
        else:
            self._record(msg, result, raw=None, now=now)
        return result

    def ingest_raw(self, raw: Any, now: float) -> IngestResult:
        """原始输入单条上报：先做合规校验，再走完整链路。

        任何离谱输入都返回结构化判定结果，不会抛异常中断。
        """
        msg, rejection = parse_message(raw)
        if rejection is not None:
            self._record(None, rejection, raw=raw, now=now)
            return rejection
        return self.ingest(msg, now)

    # ------------------------------------------------------------------
    # 批量补传
    # ------------------------------------------------------------------

    def ingest_batch(self, batch_id: str, raws: list[Any],
                     now: float) -> BatchResult:
        """批量补传：每条各自下结论，整批幂等，收敛与批内排列无关。

        - 逐条做合规校验与认证，失败条目只影响自身，不连累其他条目。
        - 通过校验的条目按 (device_id, seq, 原始下标) 排序后依次判定，
          因此同一批数据无论批内如何排列，孪生收敛结果一致。
        - 同一 batch_id 重试直接返回首次缓存的结论，不重复生效。
        """
        if not batch_id:
            raise ValueError("batch_id must be non-empty")
        with self._batch_lock:
            cached = self._batches.get(batch_id)
            if cached is not None:
                return BatchResult(batch_id=batch_id, items=cached.items,
                                   duplicate_batch=True)
            items = self._process_batch(raws, now)
            result = BatchResult(batch_id=batch_id, items=tuple(items))
            self._batches[batch_id] = result
            return result

    def _process_batch(self, raws: list[Any],
                       now: float) -> list[BatchItemResult]:
        results: dict[int, IngestResult] = {}
        ready: list[tuple[int, TelemetryMessage]] = []
        for idx, raw in enumerate(raws):
            msg, rejection = parse_message(raw)
            if rejection is not None:
                self._record(None, rejection, raw=raw, now=now)
                results[idx] = rejection
                continue
            auth = self.registry.authenticate(msg.device_id, msg.secret, now)
            if not auth.ok:
                r = IngestResult(accepted=False, auth_failure=auth.failure)
                self._record(msg, r, raw=raw, now=now)
                results[idx] = r
                continue
            rejection = self.telemetry.validate(msg, now)
            if rejection is not None:
                self._record(msg, rejection, raw=raw, now=now)
                results[idx] = rejection
                continue
            ready.append((idx, msg))
        # 顺序无关收敛：按设备分组、组内按 (seq, 原始下标) 稳定排序后判定
        ready.sort(key=lambda t: (t[1].device_id, t[1].seq, t[0]))
        for idx, msg in ready:
            r = self.telemetry.classify(msg)
            if r.accepted:
                self.twins.apply_reported(msg.device_id, msg.payload)
            else:
                self._record(msg, r, raw=None, now=now)
            results[idx] = r
        return [BatchItemResult(index=i, result=results[i])
                for i in range(len(raws))]

    # ------------------------------------------------------------------
    # 排查视图
    # ------------------------------------------------------------------

    def _record(self, msg: TelemetryMessage | None, result: IngestResult,
                raw: Any, now: float) -> None:
        """把未接受的结论留痕到排查视图（视图自身有界，淘汰不影响状态）。"""
        category, detail = self._categorize(result)
        if category is None:
            return
        self.rejections.record(RejectionRecord(
            device_id=msg.device_id if msg else _raw_device_id(raw),
            category=category, detail=detail,
            message_id=msg.message_id if msg else _raw_field(raw, "message_id"),
            seq=msg.seq if msg else _raw_field(raw, "seq"),
            raw=raw if raw is not None else (
                {"device_id": msg.device_id, "message_id": msg.message_id,
                 "seq": msg.seq, "payload": msg.payload} if msg else None),
            recorded_at=now,
        ))

    @staticmethod
    def _categorize(result: IngestResult):
        """IngestResult -> (分类, 判定依据)；已接受返回 (None, '')。"""
        if result.auth_failure is not None:
            return RejectionCategory.AUTH_FAILURE, result.auth_failure.value
        if result.reject_reason is not None:
            category = REJECT_REASON_CATEGORY[result.reject_reason]
            detail = result.detail or result.reject_reason.value
            return category, detail
        if MessageFlag.DUPLICATE in result.flags:
            return RejectionCategory.DUPLICATE, MessageFlag.DUPLICATE.value
        if MessageFlag.OUT_OF_ORDER in result.flags:
            return RejectionCategory.OUT_OF_ORDER, MessageFlag.OUT_OF_ORDER.value
        return None, ""

    # ------------------------------------------------------------------
    # 孪生
    # ------------------------------------------------------------------

    def get_twin(self, device_id: str) -> TwinState:
        return self.twins.get(device_id)

    def set_desired(self, device_id: str, desired: dict[str, Any],
                    expected_version: int) -> WriteResult:
        return self.twins.set_desired(device_id, desired, expected_version)


def _raw_device_id(raw: Any) -> str | None:
    value = _raw_field(raw, "device_id")
    return value if isinstance(value, str) else None


def _raw_field(raw: Any, name: str) -> Any:
    return raw.get(name) if isinstance(raw, dict) else None
