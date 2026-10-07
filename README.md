# IoT 设备孪生网关（进程内模拟）

基于 FastAPI 的本地设备遥测接入与设备孪生同步服务。所有状态保存在进程内，
用于模拟设备注册、凭证生命周期、单条/批量遥测上报、负载合规判定、排查留痕
与期望/上报值收敛，使重复、乱序、越权与并发更新下的结论可预期且可追溯。

## 运行与验证

```bash
uv sync                          # 安装依赖
uv run uvicorn main:app          # 启动 HTTP 服务
uv run pytest tests/ -v          # 运行测试（日志打印每条输入命中的规则与结论）
uv run pytest tests/test_batch.py tests/test_validation.py tests/test_rejection.py -v
```

## 接入协议

### 单条遥测 `POST /telemetry`（对应 `TelemetryMessage`）

| 字段 | 说明 |
| --- | --- |
| `device_id` / `secret` | 设备标识与凭证指纹 |
| `message_id` | 幂等键，同一设备内唯一 |
| `seq` | 设备侧单调序号，从 1 开始 |
| `timestamp` | 设备侧时间戳（epoch 秒） |
| `protocol_version` | 协议版本，当前支持 `1` |
| `payload` | 非空 JSON 对象，合并进孪生 reported 侧 |

处理链路：`认证 → 合规校验 → 幂等/顺序判定 → 应用到孪生`。

### 批量补传 `POST /telemetry/batch`

设备恢复连接后把积压遥测打包提交，网关对每条各自下结论：

```json
{
  "batch_id": "device-local-unique-id",
  "device_id": "dev-1",
  "secret": "s1",
  "items": [
    {"message_id": "m2", "seq": 2, "timestamp": 1790000000.0,
     "protocol_version": 1, "payload": {"temp": 22.0}},
    {"message_id": "m1", "seq": 1, "timestamp": 1790000000.0,
     "payload": {"temp": 21.0}}
  ]
}
```

- `batch_id` 可选但强烈建议提供：同一设备 + 同一 `batch_id` 的重试整批幂等，
  原样返回首次判定，不重复计数、不留新痕、不让状态回退。未带 `batch_id` 时
  仍由每条 `message_id` 保证幂等。
- 网关在锁内按 `(seq, message_id)` 排序后判定，**批内条目无论按什么顺序提交，
  最终孪生收敛结果都一致**。
- 批级只做一次认证：认证失败整批拒绝，不写任何状态。
- 每条返回 `accepted / flags / reject_reason / payload_issue / issue_detail /
  applied_seq`，并带 `index` 与 `message_id` 与提交顺序对位。
- 批级请求本身不做严格 schema 绑定：`device_id/secret/items` 结构非法时返回
  `error=malformed_batch_request` 的结构化响应（HTTP 200），不抛异常中断；
  批内个别条目不合规只拒绝该条，绝不连累同批其他条目。

批内语义与单条完全一致，且每条都能单独标记：

| 命中条件 | 标记/结论 | 是否写孪生 |
| --- | --- | --- |
| 条目合规问题（见下节） | `malformed_payload` + `payload_issue` | 否，不占顺序位置 |
| 协议版本/时间戳问题 | `unknown_protocol_version` / `stale_timestamp` | 否 |
| `message_id` 已见（含批内重复、跨批/跨单条重复） | `duplicate` | 否 |
| `seq <= 当前水位`（含批内同 `seq` 决胜败者） | `out_of_order` | 否 |
| `seq == 水位 + 1` | `accepted` | 是 |
| `seq > 水位 + 1` | `gap`（缺号单独标记） | 是，确定性前进 |

批内同 `seq` 的决胜规则固定为 **`message_id` 字典序占先**，其余判
`out_of_order`，因此任意排列下每条的结论也是确定的。

## 判定结果与归类规则

### 一级分类（排查视图使用，`RejectCategory`）

| 分类 | 含义 |
| --- | --- |
| `auth_failure` | 认证未通过：`unknown_device` / `device_revoked` / `unknown_credential` / `credential_expired` |
| `malformed_payload` | 上报内容不合规（负载问题总类），细分见 `PayloadIssue` |
| `unknown_protocol_version` | 协议版本号类型正确但不在支持集合内 |
| `stale_timestamp` | 时间戳与本地时钟偏差超过 `max_clock_skew_seconds` |

`duplicate` / `out_of_order` / `gap` 是**接受侧标记而非拒绝**，不进入排查视图。

### 负载不合规的细分子类（`PayloadIssue`）

| 子类 | 命中场景 |
| --- | --- |
| `invalid_structure` | 条目/负载不是 JSON 对象、空对象、对象键非非空字符串、值类型不受支持（bytes/set 等）、嵌套深度超过 8、对象/数组元素超过 256 |
| `missing_field` | 必填字段 `message_id/seq/timestamp/payload` 缺失、为 `null`，或 `message_id` 为空白字符串 |
| `type_mismatch` | 字段类型不符：`seq` 非整数（含浮点、bool）、`timestamp` 非数值、`protocol_version` 非整数、`message_id` 非字符串 |
| `invalid_value` | 类型正确但取值非法：`seq < 1`、`timestamp` 为 NaN/Inf、遥测数值为 NaN/Inf、字符串长度超过 4096、字段值为 `null` |

### 多问题命中时的确定性优先级

一条上报同时命中多个问题时，严格按下列顺序只返回第一个命中项，结论可解释、
可复现、可写进文档：

1. 入口结构不是对象 → `invalid_structure`
2. 必填字段缺失/为空 → `missing_field`（存在性先统一检查一遍，故优先于类型）
3. 字段类型不符 → `type_mismatch`
4. 类型正确但取值非法 → `invalid_value`
5. 协议版本不支持 → `unknown_protocol_version`
6. 时间戳越界 → `stale_timestamp`
7. `payload` 负载体递归校验：深度优先、按对象插入顺序/数组下标，首个命中即返回

所有拒绝结果都带人类可读的 `issue_detail`（明确字段路径与命中规则）。
任何离谱输入都只产生一条结构化判定：不抛异常、不写孪生、不占用顺序推进位置。

## 排查视图（拒绝留痕）

进程内有容量上限的最近拒绝记录视图（`RejectionView`，默认容量 1000，
可由 `Gateway(rejection_capacity=...)` 调整）。

- `GET /rejections?device_id=&category=&limit=`：按设备/一级分类过滤，
  默认最新在前，`limit` 截断；每条含 `seq_no/received_at/device_id/category/
  reason/detail/message_id/msg_seq/raw`，`raw` 为原始上报内容快照，可直接翻查。
- `GET /rejections/counts?device_id=`：按一级分类返回计数（不传设备为全局）。
- 容量与淘汰：容量打满后每写入一条淘汰 **`seq_no` 最小（最旧）** 的一条（FIFO）；
  `seq_no` 全局单调、不复用；计数随淘汰同步增减。
- 淘汰只影响视图本身，**不触碰设备孪生与顺序水位**。
- 并发：写入/查询各自加锁，临界区恒定且有界，查询取快照后在锁外返回，
  多设备并发上报时计数与记录不丢、不重，也不会堵住正常上报链路。

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

批量补传在同一设备锁内原子完成排序判定与孪生应用（按规范序应用），单条上报
与批量补传交错提交时孪生同样不会被旧值回退。批判定缓存按每设备 LRU 维护
（默认 10000 个 batch_id，`batch_cache_size` 可调）；缓存淘汰后重试仍有
`message_id` 幂等兜底。

## 孪生版本模型

每设备独立的 `TwinState`：`desired`/`desired_version` 与
`reported`/`reported_version` 各自单调递增、互不干扰。

- 期望值写入采用乐观并发控制，`ConflictPolicy.REJECT`（默认）：
  `expected_version` 必须等于当前版本，否则明确拒绝并返回当前版本。
- `ConflictPolicy.LAST_WRITER_WINS`：`expected_version` 视为写入方指派的目标
  版本，仅严格大于当前版本时接受并采用该版本——与到达顺序无关地收敛到最高
  版本，落后写明确拒绝。
- 上报值由遥测驱动合并更新，`reported_version` 单调递增；重复、乱序与不合规
  消息都不会令其回退或多计。

## 并发与隔离

- 注册表、遥测处理器、孪生存储、排查视图各自持锁，判定与状态修改原子完成；
  Gateway 另按设备串行化上报临界区，保证单条与批量交错时孪生不回退。
- 孪生与顺序水位按设备隔离：多设备并发上报互不污染、不丢更新
  （见 `test_concurrent_devices_do_not_pollute_each_other` 与
  `test_concurrent_batches_from_multiple_devices`）。

## 兼容规则

- 仅接受 `SUPPORTED_PROTOCOL_VERSIONS`（当前为 `{1}`）内的协议版本，版本号
  类型错误归 `malformed_payload/type_mismatch`，类型正确但不支持归
  `unknown_protocol_version`。
- 时间戳容忍双向时钟偏移 `max_clock_skew_seconds`（默认 300s），超出即拒绝。
- 凭证轮换重叠窗口默认 300s，窗口内新旧凭证均可认证，窗口外旧凭证按
  `credential_expired` 拒绝。

## 验证方法

`tests/` 下每个用例通过 `logging` 打印输入、命中规则与判定依据
（`pyproject.toml` 已配置 `log_cli`，运行时可见）：

- `test_registry.py`：幂等注册、轮换窗口边界（含开区间右端点）、撤销、
  四种认证失败原因区分。
- `test_telemetry.py`：乱序/重复/缺口上报、畸形负载、未知协议版本、过期
  时间戳、认证失败阻断（不回归）。
- `test_twin.py`：乐观并发冲突拒绝、LWW 确定性收敛、并发不丢更新、
  多设备并发隔离、desired/reported 版本独立（不回归）。
- `test_batch.py`：批量正序/乱序排列收敛一致、缺号标记、同 seq 决胜、
  批内重复、同批重试只生效一次、断网补传后追赶、单条与批量交错不回退、
  多设备并发双批+重试不丢不重、不合规条目不连累正常条目、批级认证失败。
- `test_validation.py`：四类 `PayloadIssue` 归类、多问题优先级、离谱输入
  绝不抛异常且不写孪生不占顺序位置、合法形态接受。
- `test_rejection.py`：FIFO 容量淘汰与计数一致性、按设备/分类/条数检索、
  并发写入不丢不重、容量打满不超限、拒绝留痕含原始快照、淘汰不影响孪生与顺序。

HTTP 接口一览：`POST /devices`、`POST /devices/{id}/rotate`、`POST /devices/{id}/revoke`、
`POST /telemetry`、`POST /telemetry/batch`、`GET /twin/{id}`、
`POST /twin/{id}/desired`、`GET /rejections`、`GET /rejections/counts`。
