"""设备孪生存储：期望值/上报值版本化与乐观并发控制。"""
from __future__ import annotations

import threading
from typing import Any

from .models import ConflictPolicy, TwinState, WriteResult


class TwinStore:
    """每设备独立的 TwinState；单把可重入锁保证多设备并发互不污染、不丢更新。"""

    def __init__(self, conflict_policy: ConflictPolicy = ConflictPolicy.REJECT) -> None:
        self._lock = threading.RLock()
        self._policy = conflict_policy
        self._twins: dict[str, TwinState] = {}

    def get(self, device_id: str) -> TwinState:
        with self._lock:
            return self._twins.setdefault(device_id, TwinState())

    def set_desired(self, device_id: str, desired: dict[str, Any],
                    expected_version: int) -> WriteResult:
        """乐观并发写期望值。

        REJECT 策略：expected_version != 当前版本时明确拒绝（冲突可见）。
        LAST_WRITER_WINS 策略：expected_version 视为写入方指派的目标版本，
        仅当其严格大于当前版本时接受并直接采用该版本——与到达顺序无关地
        确定性收敛到最高版本，落后写明确拒绝。
        """
        with self._lock:
            twin = self._twins.setdefault(device_id, TwinState())
            current = twin.desired_version
            if self._policy is ConflictPolicy.REJECT:
                if expected_version != current:
                    return WriteResult(
                        ok=False, version=current, conflict=True,
                        detail=f"expected_version={expected_version} != current={current}")
                twin.desired = dict(desired)
                twin.desired_version = current + 1
            else:  # LAST_WRITER_WINS
                if expected_version <= current:
                    return WriteResult(
                        ok=False, version=current, conflict=True,
                        detail=f"stale write: version={expected_version} <= current={current}")
                twin.desired = dict(desired)
                twin.desired_version = expected_version
            return WriteResult(ok=True, version=twin.desired_version)

    def apply_reported(self, device_id: str, reported: dict[str, Any]) -> WriteResult:
        """遥测驱动的上报值合并更新，reported_version 单调递增。"""
        with self._lock:
            twin = self._twins.setdefault(device_id, TwinState())
            twin.reported.update(reported)
            twin.reported_version += 1
            return WriteResult(ok=True, version=twin.reported_version)
