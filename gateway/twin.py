"""设备孪生：期望值与上报值的版本化收敛。"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .models import ConflictPolicy, Twin, VersionedValue


@dataclass(frozen=True)
class DesiredUpdateResult:
    applied: bool
    twin: Twin
    reason: str


class VersionConflict(Exception):
    """REJECT_ON_CONFLICT 策略下基版本不匹配时抛出。"""

    def __init__(self, device_id: str, base_version: int, current_version: int) -> None:
        super().__init__(
            f"desired update conflict on {device_id}: "
            f"base {base_version} != current {current_version}"
        )
        self.device_id = device_id
        self.base_version = base_version
        self.current_version = current_version


class TwinStore:
    """进程内孪生存储。

    - desired：乐观并发控制，base_version 必须等于当前版本；
      冲突时按 ConflictPolicy 确定性收敛（LWW：writer_id 字典序大者胜）
      或明确拒绝（REJECT_ON_CONFLICT 抛 VersionConflict）。
    - reported：以遥测 seq 作为版本，仅接受更高版本，乱序上报不会回退。
    - 每设备一把锁：并发更新互斥，不同设备互不污染。
    """

    def __init__(self, policy: ConflictPolicy = ConflictPolicy.LAST_WRITER_WINS) -> None:
        self.policy = policy
        self._twins: dict[str, Twin] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, device_id: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(device_id, threading.Lock())

    def get(self, device_id: str) -> Twin:
        with self._lock_for(device_id):
            return self._twins.setdefault(device_id, Twin(device_id=device_id))

    def update_desired(
        self,
        device_id: str,
        value: dict[str, Any],
        base_version: int,
        writer_id: str,
        now: float,
    ) -> DesiredUpdateResult:
        with self._lock_for(device_id):
            twin = self._twins.setdefault(device_id, Twin(device_id=device_id))
            current = twin.desired
            if base_version == current.version:
                twin.desired = VersionedValue(dict(value), current.version + 1, writer_id, now)
                return DesiredUpdateResult(True, twin, "applied on matching base version")

            # 冲突：基版本过期
            if self.policy is ConflictPolicy.REJECT_ON_CONFLICT:
                raise VersionConflict(device_id, base_version, current.version)

            # LAST_WRITER_WINS：确定性仲裁——writer_id 字典序大者胜，
            # 与到达顺序无关，保证并发下结论可预期。
            if writer_id >= current.writer_id:
                twin.desired = VersionedValue(dict(value), current.version + 1, writer_id, now)
                return DesiredUpdateResult(
                    True,
                    twin,
                    f"conflict (base {base_version} != current {current.version}), "
                    f"won by writer order: {writer_id} >= {current.writer_id}",
                )
            return DesiredUpdateResult(
                False,
                twin,
                f"conflict (base {base_version} != current {current.version}), "
                f"lost by writer order: {writer_id} < {current.writer_id}",
            )

    def apply_reported(
        self, device_id: str, value: dict[str, Any], seq: int, now: float
    ) -> bool:
        """以 seq 为版本应用上报状态；低版本（乱序/重复）直接忽略，返回是否应用。"""
        with self._lock_for(device_id):
            twin = self._twins.setdefault(device_id, Twin(device_id=device_id))
            if seq <= twin.reported.version:
                return False  # 不回退、不重复计数
            twin.reported = VersionedValue(dict(value), seq, writer_id=device_id, updated_at=now)
            return True
