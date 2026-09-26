# IoT 设备孪生网关（进程内模拟）

基于 FastAPI 的本地设备遥测接入与设备孪生同步服务。所有状态保存在进程内，
用于模拟设备注册、凭证生命周期、遥测上报与期望/上报值收敛，使重复、乱序、
越权与并发更新下的结论可预期且可追溯。

## 运行与验证

```bash
uv sync                 # 安装依赖
uv run uvicorn main:app # 启动 HTTP 服务
uv run pytest tests/ -v # 运行测试（日志打印每条输入与判定依据）
```

## 接入协议

遥测消息（`POST /telemetry`，对应 `TelemetryMessage`）：

| 字段 | 说明 |
| --- | --- |
| `device_id` / `secret` | 设备标识与凭证指纹 |
| `message_id` | 幂等键，同一设备内唯一 |
| `seq` | 设备侧单调序号，从 1 开始 |
| `timestamp` | 设备侧时间戳（epoch 秒） |
| `protocol_version` | 协议版本，当前支持 `1` |
| `payload` | 非空 JSON 对象，合并进孪生 reported 侧 |

处理链路：`认证 → 负载校验 → 幂等/顺序判定 → 应用到孪生`。

## 状态与版本模型

### 设备与凭证

- 设备状态：`active` / `revoked`。注册幂等：重复注册返回既有设备，不重置凭证。
- 凭证带 `[not_before, not_after)` 有效期。轮换时新凭证立即生效，仍有效的旧凭证
  被截断到 `now + rotation_overlap_seconds`，形成明确的新旧共存重叠窗口。
- 认证失败原因可区分：`unknown_device`、`device_revoked`（优先于凭证检查）、
  `unknown_credential`、`credential_expired`。

### 遥测顺序模型

每设备维护已应用的最大 `seq` 水位（初始 0）：

| 条件 | 标记 | 行为 |
| --- | --- | --- |
| `seq == 水位 + 1` | `accepted` | 应用并推进水位 |
| `seq > 水位 + 1` | `gap` | 应用并推进水位（确定性前进） |
| `seq <= 水位` | `out_of_order` | 不应用，状态不回退 |
| `message_id` 已见 | `duplicate` | 幂等忽略，不重复计数 |

负载层面拒绝（不占用水位、不计数）：`malformed_payload`（非对象或空对象）、
`unknown_protocol_version`、`stale_timestamp`（与本地时钟偏差超过
`max_clock_skew_seconds`）。

### 孪生版本模型

每设备独立的 `TwinState`：`desired`/`desired_version` 与
`reported`/`reported_version` 各自单调递增、互不干扰。

- 期望值写入采用乐观并发控制，`ConflictPolicy.REJECT`（默认）：
  `expected_version` 必须等于当前版本，否则明确拒绝并返回当前版本。
- `ConflictPolicy.LAST_WRITER_WINS`：`expected_version` 视为写入方指派的目标
  版本，仅严格大于当前版本时接受并采用该版本——与到达顺序无关地收敛到最高
  版本，落后写明确拒绝。
- 上报值由遥测驱动合并更新，`reported_version` 单调递增；重复与乱序消息不会
  令其回退或多计。

## 并发与隔离

- 注册表、遥测处理器、孪生存储各自持有可重入锁，判定与状态修改原子完成。
- 孪生按设备隔离：多设备并发上报互不污染、不丢更新
  （见 `test_concurrent_devices_do_not_pollute_each_other`）。

## 兼容规则

- 仅接受 `SUPPORTED_PROTOCOL_VERSIONS`（当前为 `{1}`）内的协议版本，未知版本
  返回 `unknown_protocol_version` 拒绝而非静默丢弃。
- 时间戳容忍双向时钟偏移 `max_clock_skew_seconds`（默认 300s），超出即拒绝。
- 凭证轮换重叠窗口默认 300s，窗口内新旧凭证均可认证，窗口外旧凭证按
  `credential_expired` 拒绝。

## 验证方法

`tests/` 下每个用例通过 `logging` 打印输入与判定依据（`pyproject.toml` 已配置
`log_cli`，运行时可见）：

- `test_registry.py`：幂等注册、轮换窗口边界（含开区间右端点）、撤销、
  四种认证失败原因区分。
- `test_telemetry.py`：乱序/重复/缺口上报、畸形负载、未知协议版本、过期
  时间戳、认证失败阻断。
- `test_twin.py`：乐观并发冲突拒绝、LWW 确定性收敛、并发不丢更新、
  多设备并发隔离、desired/reported 版本独立。

HTTP 接口一览：`POST /devices`、`POST /devices/{id}/rotate`、`POST /devices/{id}/revoke`、
`POST /telemetry`、`GET /twin/{id}`、`POST /twin/{id}/desired`。
