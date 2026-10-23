<!-- 用途：说明本阶段可运行能力、凭据配置、人工检查和平台限制。 -->
# KCQ CTP connector

当前交付阶段：**真实连接诊断 + 只读具体期货合约目录 + 原始快照 SQLite 录制**。
尚未提供 K 线或 SSE，不会在 KCQ 来源列表中伪装成可用行情源。
交易前置仅用于认证、登录、查询合约；不提供报单、撤单、账户和结算确认。

## 安装与离线自检

需要 uv 和 Python 3.12。仓库位于 KCQ 同级 `KCQ-CTP-connector/`。
源码仓库：[TsekaLuk/KCQ-CTP-connector](https://github.com/TsekaLuk/KCQ-CTP-connector)。
没有 SimNow / TTS 账号也可以完成下面的离线检查；实际登录与市场行情仍待有账号的贡献者验收。

```sh
uv sync --frozen --link-mode copy
uv run --frozen --no-sync python diagnose.py --sdk-only
uv run --frozen --no-sync python -m unittest discover -s tests
```

固定 `openctp-ctp==6.7.7.1`。标准库启动时对照 wheel RECORD 核验两份原生库的 SHA256。
离线自检实际创建、释放 Md/Td API；它不连接前置，也不能证明账号可登录。

## SimNow 实连

复制 `config.example.toml` 为 `config.toml`，填写所选环境的当前行情/交易前置、BrokerID、
账号和密码。账号只保存在本地；`config.toml` 已忽略，不要把密码发到聊天或提交 Git。
也支持 `CTP_CONFIG` 指定文件，及 `CTP_BROKER_ID`、`CTP_USER_ID`、`CTP_PASSWORD`、
`CTP_APP_ID`、`CTP_AUTH_CODE` 覆盖对应字段。

`auth_required` 按环境明确配置；设为 true 时必须填写 AppID/AuthCode。
失败时不会自动跳过认证或改连其他环境。

先保留 `recording_symbols = []`，查询当前真实合约：

```sh
uv run --frozen --no-sync python diagnose.py --seconds 30
```

成功需完成 Md 登录、Td 登录和整份合约查询。输出 `instruments` 是真实可交易的具体期货；
没有配置录制合约时，`marketDataVerified` 仍为 false。

从目录中选择原始 `raw_id` 填到 `recording_symbols`，重新运行诊断。
每个指定合约均订阅成功且收到至少一个快照才通过；休市可能超时。
输出保留 TradingDay、ActionDay、UpdateTime、UpdateMillisec，以及累计成交量/额。
这些是行情快照，不能称为逐笔成交。

持续运行：

```sh
uv run --frozen --no-sync python server.py
# 也可在 KCQ 目录运行：
pnpm connector ctp
pnpm dev -c ctp
```

默认 `127.0.0.1:8093`。缺配置也能启动诊断服务，但不连接上游。
`ctp` 不进入 `all`。KCQ 配套接入 PR 将本仓库登记到 `pnpm setup:backends` 克隆清单；
克隆后仍需在本目录执行 `uv sync --frozen --link-mode copy` 准备独立环境。

## OpenCTP TTS 回放

配置 `profile = "openctp-replay"`、`sdk_channel = "tts"`，并填写 TTS 7x24 环境的前置及账号。
不能只修改 IP：TTS 需要匹配版本的兼容 Md/Td 原生库。
普通 TTS 仿真不等于 7x24 行情回放；本阶段仅安排 7x24。

在独立的 Linux x86_64 或 Windows venv 中准备，**先停止所有服务**：

```sh
uv sync --frozen --extra tts --link-mode copy
uv run --frozen --no-sync openctp-channels switch tts
uv run --frozen --no-sync python diagnose.py --sdk-only --sdk-channel tts
uv run --frozen --no-sync python diagnose.py --seconds 30
```

通道工具可能以退出码 0 报告切换失败，所以必须运行本项目自检。
自检逐一比较当前两份动态库和通道工具 `_chan_tts` 缓存的 SHA256；没有缓存或指纹不匹配就拒绝启动。
服务代码不会实例化通道切换器或修改 site-packages。
不要在已切换环境随意重建/同步依赖；重建后重新准备并核验。
Mac TTS 尚未验证并明确拒绝；Mac 标准 CTP 的支持不代表 TTS 可用。
Windows/Linux TTS 实际登录也尚待人工验证。

## 诊断与存储

- `GET /health`：进程状态；阶段一始终 `chartReady=false`。
- `GET /diagnostics`：脱敏版本、登录阶段、目录数量、订阅确认和最近原始快照。
- `GET /api/v1/market-data/sources/{sourceId}/probe`：V1 probe，阶段一为 degraded。
- `POST /api/v1/market-data/instruments/search`：V1 目录搜索，请求含 sourceId/keyword/limit。
- bars/stream 返回 `UNSUPPORTED_CAPABILITY`，不返回伪造或静默填充的 K 线。

来源身份为 `ctp-simnow`、`ctp-simnow-replay`、`ctp-openctp-replay`，不跨环境消费目录。
每次运行独立保存 `data/<profile>/<runId>/recording.sqlite3`，包含目录响应、原始快照及接收纳秒时间。
所有事件按接收顺序追加，即使回放时钟倒退也不会覆盖已有循环。
SDK 流目录可能有账号相关原生缓存，整个 data 目录均忽略。
录制不依赖浏览器；每个进程只启动一个 worker。

回调仅复制字段进入有界队列。队列溢出、回调复制失败、上游错误、断线或写库失败会停止采集并公开错误；
断线不会被掩盖为连续历史，阶段一需人工重启，不做未经验证的自动恢复。
不要把当前记录视为完整交易所历史。下一阶段用实际录制数据验证分钟桶、累计量差分和夜盘日期，
再接 KCQ 的 HTTP K 线及 SSE。

## 人工验收

先完成一套真实环境：核对目录、订阅确认和快照时间；至少选一个日盘和一个夜盘合约，
在对应交易时段录制样本。再断开网络确认失败和缺口可见。
保留原始记录在本地，聚合开发只使用获确认的真实样本。

API、环境变量、SQLite 与失败行为的可执行契约见 [docs/contract.md](docs/contract.md)。
