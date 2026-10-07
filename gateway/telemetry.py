"""遥测上报处理：幂等、顺序、缺口与批量补传判定。"""
from __future__ import annotations

import logging
import threading
from collections import OrderedDict

from .models import (
    BatchItemResult,
    IngestResult,
    MessageFlag,
    TelemetryMessage,
)

log = logging.getLogger(__name__)

# 批判定缓存的默认容量（每设备 LRU 键数）；缓存淘汰后重试仍由 message_id 幂等兜底
DEFAULT_BATCH_CACHE = 10_000


class TelemetryProcessor:
    """按设备维度判定重复/乱序/缺口。

    顺序模型：每设备维护已应用的最大 seq 水位 watermark（初始 0）。
    - seq == watermark + 1      -> ACCEPTED，应用并推进水位
    - seq >  watermark + 1      -> GAP，应用并推进水位（确定性前进而非回退）
    - seq <= watermark          -> OUT_OF_ORDER，不应用，不回退状态
    - message_id 已见           -> DUPLICATE，幂等忽略，不重复计数

    批量补传（classify_batch）在同一把锁内对整批排序后判定，因此批内条目无论
    按什么顺序提交，收敛结果都一致；批内同 seq 以 message_id 字典序决胜，先者
    ACCEPTED/GAP，其余判 OUT_OF_ORDER，规则确定。batch_id 命中缓存时原样返回
    首次判定，重试只生效一次。
    """

    def __init__(self, batch_cache_size: int = DEFAULT_BATCH_CACHE) -> None:
        self._lock = threading.RLock()
        self._seen_ids: dict[str, set[str]] = {}
        self._watermark: dict[str, int] = {}
        self._batch_cache_size = batch_cache_size
        self._batch_cache: dict[str, "OrderedDict[str, tuple[BatchItemResult, ...]]"] = {}

    def classify(self, msg: TelemetryMessage) -> IngestResult:
        """幂等与顺序判定。accepted=True 时调用方应将 payload 应用到孪生。"""
        with self._lock:
            seen = self._seen_ids.setdefault(msg.device_id, set())
            if msg.message_id in seen:
                return IngestResult(
                    accepted=False, flags=(MessageFlag.DUPLICATE,),
                    applied_seq=self._watermark.get(msg.device_id, 0),
                    message_id=msg.message_id)
            watermark = self._watermark.get(msg.device_id, 0)
            if msg.seq <= watermark:
                # 乱序旧消息：记录 message_id 保证其重发判重，但不应用、不回退
                seen.add(msg.message_id)
                return IngestResult(accepted=False,
                                    flags=(MessageFlag.OUT_OF_ORDER,),
                                    applied_seq=watermark,
                                    message_id=msg.message_id)
            flags = (MessageFlag.GAP,) if msg.seq > watermark + 1 \
                else (MessageFlag.ACCEPTED,)
            seen.add(msg.message_id)
            self._watermark[msg.device_id] = msg.seq
            return IngestResult(accepted=True, flags=flags, applied_seq=msg.seq,
                                message_id=msg.message_id)

    def lookup_batch(self, device_id: str,
                     batch_id: str | None) -> tuple[BatchItemResult, ...] | None:
        """查询批判定缓存；命中返回首次判定，未命中返回 None。"""
        if batch_id is None:
            return None
        with self._lock:
            cache = self._batch_cache.get(device_id)
            if cache is None or batch_id not in cache:
                return None
            cache.move_to_end(batch_id)
            return cache[batch_id]

    def classify_batch(
        self,
        device_id: str,
        batch_id: str | None,
        messages: list[TelemetryMessage | None],
        prefilled: dict[int, IngestResult],
    ) -> tuple[tuple[BatchItemResult, ...], tuple[int, ...]]:
        """对一批补传做原子的幂等/顺序判定。

        ``messages[i]`` 为 None 表示该位置是合规性未通过的条目，其判定取自
        ``prefilled[i]``；其余条目按 (seq, message_id) 排序后依次判定。
        返回 ``(与提交顺序逐位对齐的结果元组, 本次新接受条目在规范序下的索引)``，
        并按 batch_id 缓存（每设备 LRU）。调用方应只对后者在规范序下应用孪生。
        """
        with self._lock:
            results: list[BatchItemResult | None] = [None] * len(messages)
            for idx, rejection in prefilled.items():
                results[idx] = BatchItemResult(
                    index=idx, message_id=rejection.message_id, accepted=False,
                    reject_reason=rejection.reject_reason,
                    payload_issue=rejection.payload_issue,
                    issue_detail=rejection.issue_detail)

            order = sorted(
                (i for i, m in enumerate(messages) if m is not None),
                key=lambda i: (messages[i].seq, messages[i].message_id))
            seen = self._seen_ids.setdefault(device_id, set())
            watermark = self._watermark.get(device_id, 0)
            applied: list[int] = []
            for i in order:
                m: TelemetryMessage = messages[i]  # type: ignore[assignment]
                if m.message_id in seen:
                    verdict = BatchItemResult(
                        index=i, message_id=m.message_id, accepted=False,
                        flags=(MessageFlag.DUPLICATE,), applied_seq=watermark)
                elif m.seq <= watermark:
                    seen.add(m.message_id)
                    verdict = BatchItemResult(
                        index=i, message_id=m.message_id, accepted=False,
                        flags=(MessageFlag.OUT_OF_ORDER,), applied_seq=watermark)
                else:
                    flags = (MessageFlag.GAP,) if m.seq > watermark + 1 \
                        else (MessageFlag.ACCEPTED,)
                    seen.add(m.message_id)
                    watermark = m.seq
                    self._watermark[device_id] = watermark
                    applied.append(i)
                    verdict = BatchItemResult(
                        index=i, message_id=m.message_id, accepted=True,
                        flags=flags, applied_seq=watermark)
                results[i] = verdict
                log.info("批量条目: device=%s batch_id=%s idx=%d mid=%s seq=%d -> %s",
                         device_id, batch_id, i, m.message_id, m.seq,
                         verdict.flags[0].value if verdict.flags
                         else (verdict.reject_reason.value if verdict.reject_reason
                               else "rejected"))

            packed = tuple(r for r in results if r is not None)
            if batch_id is not None:
                cache = self._batch_cache.setdefault(device_id, OrderedDict())
                cache[batch_id] = packed
                cache.move_to_end(batch_id)
                while len(cache) > self._batch_cache_size:
                    cache.popitem(last=False)
            return packed, tuple(applied)

    def applied_seq(self, device_id: str) -> int:
        with self._lock:
            return self._watermark.get(device_id, 0)
