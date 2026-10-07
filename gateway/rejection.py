"""有容量上限的排查视图：留存最近被拒绝的上报及判定依据。

淘汰规则（确定性）：
- 视图持有全局 FIFO 缓冲，容量固定为 ``capacity``（默认 1000）。
- 容量打满后，每写入一条新记录，最旧的一条（最小 seq_no）被淘汰。
- 淘汰只影响排查视图本身，不触碰设备孪生与顺序水位。
- 并发：所有读写在同一把锁内完成；容量有界使临界区恒定较短，
  查询复制快照后在锁外返回，不会堵住正常上报链路。
"""
from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any

from .models import RejectCategory, RejectionRecord

log = logging.getLogger(__name__)


class RejectionView:
    """全局 FIFO 环形留存：容量打满后淘汰最旧记录，按设备/分类检索。"""

    def __init__(self, capacity: int = 1000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._lock = threading.Lock()
        self._records: deque[RejectionRecord] = deque(maxlen=capacity)
        self._next_seq = 1
        # 计数随淘汰同步维护，避免查询时全量重算
        self._total_counts: dict[str, int] = {}
        self._per_device_counts: dict[str, dict[str, int]] = {}

    @property
    def capacity(self) -> int:
        return self._capacity

    def record(self, *, received_at: float, device_id: str | None,
               category: RejectCategory, reason: str, detail: str,
               message_id: str | None = None, msg_seq: int | None = None,
               raw: Any = None) -> RejectionRecord:
        """写入一条拒绝留痕；容量满时按 FIFO 淘汰最旧记录。"""
        with self._lock:
            evicted = self._records[0] if len(self._records) == self._capacity else None
            rec = RejectionRecord(
                seq_no=self._next_seq, received_at=received_at,
                device_id=device_id, category=category, reason=reason,
                detail=detail, message_id=message_id, msg_seq=msg_seq, raw=raw)
            self._next_seq += 1
            # deque(maxlen=...) 会在追加时自动挤出最左端（最旧）记录
            self._records.append(rec)
            key = category.value
            self._total_counts[key] = self._total_counts.get(key, 0) + 1
            if device_id is not None:
                bucket = self._per_device_counts.setdefault(device_id, {})
                bucket[key] = bucket.get(key, 0) + 1
            if evicted is not None:
                self._total_counts[evicted.category.value] -= 1
                if self._total_counts[evicted.category.value] == 0:
                    self._total_counts.pop(evicted.category.value, None)
                if evicted.device_id is not None:
                    bucket = self._per_device_counts.get(evicted.device_id)
                    if bucket is not None:
                        bucket[evicted.category.value] -= 1
                        if bucket[evicted.category.value] == 0:
                            bucket.pop(evicted.category.value, None)
                        if not bucket:
                            self._per_device_counts.pop(evicted.device_id, None)
        log.info("拒绝留痕: seq_no=%d device=%s category=%s reason=%s mid=%s msg_seq=%s | %s",
                 rec.seq_no, rec.device_id, rec.category.value, rec.reason,
                 rec.message_id, rec.msg_seq, rec.detail)
        return rec

    def query(self, device_id: str | None = None,
              category: RejectCategory | None = None,
              limit: int | None = None) -> list[RejectionRecord]:
        """按设备/分类过滤，默认最新在前；limit 截断返回条数。"""
        with self._lock:
            snapshot = list(self._records)
        result: list[RejectionRecord] = []
        for rec in reversed(snapshot):  # 最新在前
            if device_id is not None and rec.device_id != device_id:
                continue
            if category is not None and rec.category is not category:
                continue
            result.append(rec)
            if limit is not None and len(result) >= limit:
                break
        return result

    def counts(self, device_id: str | None = None) -> dict[str, int]:
        """按一级分类返回计数；传 device_id 只统计该设备。"""
        with self._lock:
            if device_id is None:
                return dict(self._total_counts)
            return dict(self._per_device_counts.get(device_id, {}))

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)
