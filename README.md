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

## 批量补传

设备恢复连接后可把积压遥测打包提交：`POST /telemetry/batch`，
请求体 `{"batch_id": "...", "messages": [...]}`（Python 侧
`Gateway.ingest_batch(batch_id, raws, now)`），返回 `BatchResult`：

- 每条各自下结论（`items[*].index` 对应原始提交下标），批内个别条目
  不合规/认证失败不影响其他正常条目。
- **顺序无关收敛**：通过合规与认证的条目按 `(device_id, seq, 原始下标)`
  排序后依次判定，因此同一批数据无论批内如何排列，孪生收敛结果一致；
  批内的重复（`duplicate`）、乱序（`out_of_order`）、缺号（`gap`）
  分别标记，幂等与顺序语义与单条链路完全相同。
- **整批幂等**：同一 `batch_id` 重试直接返回首次缓存的结论
  （`duplicate_batch=true`），不重复计数、状态不回退。
- 时间戳偏移等负载校验与单条链路一致；补传陈旧数据时需相应调大
  `max_clock_skew_seconds`，否则超偏条目被逐条判 `stale_timestamp`
  （不影响批内其他条目）。

## 合规判定与归类规则

原始输入（`POST /telemetry/raw` 或批量条目）先经 `compliance.parse_message`
校验，**任何输入都返回结构化判定结果，绝不抛异常中断**。字段按固定顺序
`device_id → secret → message_id → seq → timestamp → protocol_version →
payload` 逐项检查，每项依次判 缺失 → 类型 → 取值，**第一条命中的规则即
结论**（确定、可解释），`detail` 记录命中的规则（如 `missing_field:seq`）：

| 拒绝 | 触发 | 分类 |
| --- | --- | --- |
| `missing_field` | 必填字段缺失 | `payload_non_compliant` |
| `invalid_field_type` | 类型不符（如 seq 为字符串/布尔） | `payload_non_compliant` |
| `invalid_field_value` | 取值非法（如 seq<1、空 message_id、NaN 时间戳） | `payload_non_compliant` |
| `malformed_payload` | 条目非对象 / payload 非对象或空对象 | `payload_non_compliant` |
| `unknown_protocol_version` | 协议版本不支持 | `unknown_protocol_version` |
| `stale_timestamp` | 时钟偏移超 `max_clock_skew_seconds` | `stale_timestamp` |

前四种统一归入**负载不合规**（`payload_non_compliant`），与协议版本、
时间戳等其他拒绝原因清楚区分（映射见 `REJECT_REASON_CATEGORY`）。
不合规条目**不写入孪生、不占用顺序水位、不计数**。

## 排查视图

`RejectionLog`（`GET /rejections?device_id=&category=&limit=`、
`GET /rejections/stats`）留痕所有未接受的上报：设备、分类、判定依据、
原始内容（`raw`）。分类：`auth_failure` / `payload_non_compliant` /
`unknown_protocol_version` / `stale_timestamp` / `duplicate` /
`out_of_order`。

- 容量上限 `rejection_capacity`（默认 1000），装满后 **FIFO 淘汰最旧
  记录**；淘汰只影响视图，不影响孪生与顺序状态。
- `total_seen` / `total_evicted` / 分类累计计数单调递增，不随淘汰回退，
  并发记录下不丢不重。
- 视图持独立锁，查询不阻塞正常上报链路。

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
- `test_compliance.py`：各类不合规输入的归类、多问题命中时固定顺序取首条、
  不合规条目对孪生与顺序状态零影响、批内坏条目不连累正常条目。
- `test_batch.py`：批量补传正序/乱序收敛一致、整批重试只生效一次、
  批内重复与缺号标记、跨设备同批隔离。
- `test_rejection_log.py`：按设备/分类查询、容量 FIFO 淘汰、并发写入计数
  不丢不重、淘汰不影响孪生与顺序、查询不阻塞上报。

HTTP 接口一览：`POST /devices`、`POST /devices/{id}/rotate`、`POST /devices/{id}/revoke`、
`POST /telemetry`、`POST /telemetry/raw`、`POST /telemetry/batch`、
`GET /twin/{id}`、`POST /twin/{id}/desired`、
`GET /rejections`、`GET /rejections/stats`。
