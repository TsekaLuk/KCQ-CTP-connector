<!-- 用途：记录 CTP 第一阶段可执行契约，区分离线验证与上游验收。 -->
# CTP 连接与原始录制契约

## 1. Scope / Trigger

适用于原生回调、消费线程、SQLite、诊断 HTTP 之间的边界。
本阶段只提供只读连接、真实合约目录和原始快照录制；K 线 / SSE 不属于当前实现。
新增字段或改变生命周期时先更新本契约及对应测试。

## 2. Signatures

```text
python diagnose.py --sdk-only [--sdk-channel ctp|tts]
python diagnose.py [--config PATH] [--seconds 30]
python server.py [--config PATH]
GET /health
GET /diagnostics
GET /api/v1/market-data/sources/{sourceId}/probe
POST /api/v1/market-data/instruments/search
POST /api/v1/market-data/bars        -> UNSUPPORTED_CAPABILITY
GET /api/v1/market-data/sources/{sourceId}/stream -> UNSUPPORTED_CAPABILITY
```

`SqliteStore(path, profile, run_id)` 只创建新文件。数据库位于
`data_dir/<profile>/<runId>/recording.sqlite3`。

```sql
recording(run_id TEXT PRIMARY KEY, profile TEXT NOT NULL, schema_version INTEGER NOT NULL)
events(sequence INTEGER PRIMARY KEY, kind TEXT NOT NULL, channel TEXT NOT NULL,
       received_ns INTEGER NOT NULL, request_id INTEGER NOT NULL,
       is_last INTEGER NOT NULL, error_id INTEGER NOT NULL, payload TEXT NOT NULL)
```

## 3. Contracts

- `CTP_CONFIG` 或 `--config` 指定 TOML；默认 `config.toml`。
- `CTP_BROKER_ID`、`CTP_USER_ID`、`CTP_PASSWORD`、`CTP_APP_ID`、
  `CTP_AUTH_CODE` 仅覆盖各自字段，其他配置不根据失败自动调整。
- profile / sourceId 一一对应：`simnow` / `ctp-simnow`、
  `simnow-replay` / `ctp-simnow-replay`、`openctp-replay` / `ctp-openctp-replay`。
- 前两个 profile 使用 `ctp` 库；后者使用匹配版本的 `tts` 双库。
- Linux 在导入绑定前通过 `locale -a` 检查 `zh_CN.GB18030`，不修改进程或系统 locale。
- 依赖锁文件使用官方 PyPI 索引和制品 URL，不继承开发机的区域镜像。
- 搜索请求：`{sourceId: string, keyword: string, limit: 1..500, assetClasses?: string[]}`。
- V1 成功响应：`{data, requestId}`；失败响应：`{error:{code,message},requestId}`。
- 目录只含当前完整查询返回的可交易具体期货；没有旧目录回退。
- probe 始终 `degraded`，能力为 `{assetClasses:["futures"],liveBars:false}`，
  不声明 bars。`/health` 的 `chartReady` 始终 false。
- 回调先把基本字段复制为 JSON 再返回，不跨线程传 SDK 对象。
- `received_ns` 表示本机接收纳秒时间；原始交易日、自然日和上游墙钟不被改写。
- 只有每个配置合约均确认订阅并收到快照，行情诊断才通过；
  空订阅列表只检查登录和目录，`marketDataVerified` 仍为 false。
- 每个 run 的事件按接收顺序追加；回放时钟倒退也不覆盖旧事件。
- 配置、SDK flow、venv 和录制文件均不提交。

## 4. Validation & Error Matrix

| 条件 | 结果 |
| --- | --- |
| 无 TOML | `CONFIG_MISSING`；无上游网络线程，HTTP 可诊断 |
| 无效 TOML / profile / 认证字段 | `CONFIG_INVALID`；只公开字段位置 |
| 标准库与 wheel RECORD 不符 / TTS 缓存不符 | `SDK_CHANNEL_MISMATCH`；不尝试连接 |
| 无可用 SDK 制品 | `SDK_UNAVAILABLE` |
| Linux 缺少 `zh_CN.GB18030` 或无法查询 locale | `SDK_UNAVAILABLE`，不导入原生绑定 |
| 本机 Mac 选择 TTS | `SDK_CHANNEL_MISMATCH`，明确未支持 |
| 原生请求发送返回非零 | `REQUEST_REJECTED`，保留数值码 |
| 异步上游错误 | `UPSTREAM_ERROR`，不公开原始文本 |
| 前置断线 | `DISCONNECTED`，停止录制并保存 GAP |
| 队列溢出 / 回调复制失败 | `QUEUE_OVERFLOW` / `CALLBACK_FAILURE`，停止录制并保存 GAP |
| 录制写入或提交失败 | `RECORDING_FAILURE`；不能保证失败磁盘还能写入 GAP |
| 配置合约不存在或原始代码有歧义 | `SYMBOL_UNAVAILABLE` |
| CLI 截止前未完成 | 非零退出，`DIAGNOSIS_TIMEOUT`，请求释放原生 API |
| API 释放未及时完成 | `STOP_TIMEOUT`，`resourcesReleased=false` |
| V1 当前来源或目录不可用 | HTTP 503，`UPSTREAM_UNAVAILABLE` |
| 不支持的 bars / stream | HTTP 400，`UNSUPPORTED_CAPABILITY` |
| V1 搜索体无效 | HTTP 422，`INVALID_REQUEST`，不回显请求体 |

## 5. Good / Base / Bad Cases

- Good：有账号且合约有效，完整目录发布后订阅，接收快照并提交后公开诊断。
- Base：无账号，SDK 离线自检和所有边界测试可运行；HTTP 明确降级。
- Bad：只改 TTS 前置、未准备动态库；必须拒绝，不能将标准 CTP 加载成功当作 TTS 验收。

## 6. Tests Required

`python -m unittest discover -s tests -v` 覆盖真实配置解析、错误脱敏、
SDK 默认响应复制、溢出标志、真实 SQLite 重开、ASGI 请求与本机原生连接超时释放。
本机失败连接只使用关闭的 loopback 端口，不伪造市场行情。
CI 在 Linux / Mac / Windows 执行标准 SDK 自检及这些边界测试，不提供上游凭据。
真实目录、订阅和行情以及 TTS 登录须由有账号的贡献者人工验收。

## 7. Wrong vs Correct

错误：`/health` 返回成功就把来源标成 online；用任意生成行情验证聚合。

正确：独立呈现 SDK、登录、目录、订阅和真实快照状态；未实现的 bars 明确拒绝，
等待真实录制样本再开发和验收聚合。
