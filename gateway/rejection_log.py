"""有容量上限的拒绝排查视图：按设备/分类查询最近未接受的上报。

- 容量装满后按 FIFO 淘汰最旧记录（确定规则），淘汰只影响视图本身，
  不影响设备孪生与顺序状态。
- 计数（total_seen / total_evicted / 分类累计）单调递增，不随淘汰回退，
  并发记录下不丢不重。
- 视图持独立锁：查询与正常上报链路互不阻塞。
"""
from __future__ import annotations

import threading
from collections import Counter, deque

from .models import RejectionCategory, RejectionRecord


class RejectionLog:
    """容量有界的拒绝记录视图。"""

    def __init__(self, capacity: int = 1000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._lock = threading.Lock()
        self._capacity = capacity
        self._records: deque[RejectionRecord] = deque()
        self._total_seen = 0
        self._total_evicted = 0
        self._category_totals: Counter[RejectionCategory] = Counter()

    def record(self, record: RejectionRecord) -> None:
        """追加一条记录；满容时淘汰最旧的一条（FIFO）。"""
        with self._lock:
            while len(self._records) >= self._capacity:
                self._records.popleft()
                self._total_evicted += 1
            self._records.append(record)
            self._total_seen += 1
            self._category_totals[record.category] += 1

    def query(self, device_id: str | None = None,
              category: RejectionCategory | None = None,
              limit: int | None = None) -> list[RejectionRecord]:
        """按设备与/或分类过滤，按记录时间从新到旧返回。"""
        with self._lock:
            snapshot = list(self._records)
        matched = [
            r for r in snapshot
            if (device_id is None or r.device_id == device_id)
            and (category is None or r.category is category)
        ]
        matched.reverse()  # 最新在前
        return matched if limit is None else matched[:limit]

    def stats(self) -> dict[str, int]:
        """容量、当前条数、累计记录数、累计淘汰数。"""
        with self._lock:
            return {
                "capacity": self._capacity,
                "size": len(self._records),
                "total_seen": self._total_seen,
                "total_evicted": self._total_evicted,
            }

    def category_totals(self) -> dict[RejectionCategory, int]:
        """各分类累计记录数（单调，不随淘汰减少）。"""
        with self._lock:
            return dict(self._category_totals)
