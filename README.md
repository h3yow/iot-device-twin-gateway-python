# IoT Device Twin Gateway（进程内模拟）

基于 FastAPI + 进程内状态模拟的设备遥测接入与设备孪生同步服务。
重复、乱序、越权与并发更新场景下的结论可预期、可追溯。

## 结构

| 模块 | 职责 |
| --- | --- |
| `gateway/models.py` | 状态枚举、判定原因（`AuthFailure`/`TelemetryVerdict`）、数据模型 |
| `gateway/registry.py` | 设备注册（幂等）、凭证轮换（重叠窗口）、撤销、认证 |
| `gateway/telemetry.py` | 遥测判定：去重、乱序、缺口、过期时间戳、畸形、版本检查 |
| `gateway/twin.py` | 孪生：desired/reported 版本化收敛与乐观并发控制 |
| `gateway/service.py` | 门面：注册 → 认证 → 遥测判定 → 孪生收敛 |
| `main.py` | FastAPI HTTP 接入层 |

## 接入协议

遥测消息（`TelemetryMessage`，HTTP `POST /telemetry`）：

```json
{
  "device_id": "d1",
  "secret": "s1",
  "seq": 1,
  "timestamp": 1727308800.0,
  "payload": {"temp": 22},
  "version": 1
}
```

- `seq`：设备侧单调递增正整数，用于去重、乱序与缺口判定。
- `timestamp`：设备时钟（秒），与网关时钟偏差超过 `max_clock_skew`（默认 300s）判过期。
- `version`：协议版本，当前仅接受 `1`（`PROTOCOL_VERSION`）。
- `payload`：上报状态字典，原样写入孪生 reported。

## 状态与版本模型

### 设备与凭证

- 设备状态：`active` / `revoked`。撤销不可逆，撤销优先于一切凭证判定。
- 注册幂等：同 `device_id` 重复注册返回既有设备，不重置凭证。
- 凭证轮换：新凭证立即生效；旧凭证 `not_after` 截断为 `now + rotation_overlap`，
  重叠窗口内新旧共存，窗口外旧凭证判 `credential_expired`。
- 认证失败原因互斥可区分：`unknown_device` / `device_revoked` /
  `bad_credential` / `credential_expired`。

### 遥测判定（按序判定，先判先出）

1. 认证失败 → `rejected_unauthorized`（附 `auth_failure`）
2. 畸形负载（seq 非正整数 / payload 非 dict / 时间戳非法）→ `rejected_malformed`
3. 未知协议版本 → `rejected_unknown_version`
4. seq 已接受过 → `duplicate`（不重复计数、不改孪生）
5. 时间戳超出偏移窗口 → `stale_timestamp`（拒绝）
6. 顺序判定：`seq == max+1` → `accepted`；`seq > max+1` → `gap_detected`（接受并记录缺口）；
   `seq <= max` 且未见 → `accepted_out_of_order`（接受，不回退）

### 孪生版本模型

- `desired` 与 `reported` 均为 `VersionedValue(value, version, writer_id, updated_at)`。
- `desired` 版本：单调递增整数，更新需携带 `base_version`（乐观并发控制）。
  - `base_version == 当前版本`：应用，版本 +1。
  - 冲突时按 `ConflictPolicy` 收敛：
    - `LAST_WRITER_WINS`（默认）：与当前持有者按 `writer_id` 字典序仲裁，大者胜；
      最终必收敛到字典序最大的写者，与到达顺序无关（确定性）。
    - `REJECT_ON_CONFLICT`：抛 `VersionConflict`（HTTP 409），状态不被污染。
- `reported` 版本：即遥测 `seq`，仅接受更高版本——乱序/重复上报不会回退或重复计数。
- 收敛判定：`converged = (desired.value == reported.value)`。
- 并发安全：每设备一把锁，多设备并发更新互不污染、不丢更新。

## 兼容规则

- 协议版本：仅 `version == 1` 被接受，其余明确拒绝（`rejected_unknown_version`），不静默丢弃。
- 畸形负载一律明确拒绝并给出原因，不进入任何状态机。
- 被拒绝（未授权/过期/畸形/版本）的上报不触碰孪生状态。

## 验证方法

```bash
uv run pytest -q          # 19 个用例，日志打印每个输入与判定依据
uv run python main.py     # 启动 HTTP 服务（127.0.0.1，随机端口）
```

测试覆盖：

- `tests/test_registry.py`：注册幂等、轮换窗口边界（含 `not_after` 精确边界）、
  四种认证失败原因区分、撤销不可解除。
- `tests/test_telemetry.py`：乱序/重复/缺口/过期时间戳/畸形/未知版本/越权、
  重复不重复计数、多设备流隔离。
- `tests/test_twin.py`：LWW 冲突确定性（交换到达顺序结论一致）、拒绝策略、
  reported 不回退、同设备并发不丢更新、多设备并发不互相污染。
- `tests/test_service.py`：端到端收敛、乱序不回退、重复不双写、轮换窗口内上报。
