# OpenClaw Probe (Rust)

OpenClaw Host Agent — 健康探测、自动重启、Token 用量扫描、远程 API 管理。

这是 Python 版 `openclaw-probe` 的完整 Rust 重写，API 端点和响应格式完全兼容，前端 Dashboard 可无缝切换。

## 功能

- **健康探测** — 定时探活 OpenClaw Gateway 实例，带重试和失败阈值
- **自动重启** — 宕机实例自动重启，带速率限制（窗口内最大次数）
- **Token 扫描** — 增量解析 JSONL session 文件，跟踪 token 用量和成本
- **系统指标** — macOS 系统指标采集（CPU、内存、磁盘、swap）
- **配置采集** — 读取 OpenClaw 配置文件中的 agents/channels/bindings/crons 信息
- **远程 API** — 完整的 REST API，支持 Token 认证，供中央 Dashboard 拉取
- **备份** — 创建 OpenClaw 数据目录的 tar.gz 备份
- **维护模式** — 暂停自动操作

## 编译

### 前置要求

- Rust 1.75+ (推荐用 [rustup](https://rustup.rs/) 安装)

### 编译 release 二进制

```bash
cargo build --release
```

产出位于 `target/release/openclaw-probe`，约 10-15MB 的单个静态二进制。

### 交叉编译 (例如在 Mac 上编译 Linux)

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

## 部署

### 1. 准备目录

```bash
mkdir -p /opt/openclaw-probe/data /opt/openclaw-probe/logs
cp target/release/openclaw-probe /opt/openclaw-probe/
cp config.json /opt/openclaw-probe/
```

### 2. 配置 config.json

```json
{
  "instances": [
    {
      "name": "主助手",
      "port": 18789,
      "profile": null,
      "openclaw_home": "~/.openclaw",
      "enabled": true
    }
  ],
  "probe": {
    "interval_seconds": 30,
    "timeout_seconds": 5,
    "failure_threshold": 2,
    "retry_delay_seconds": 10
  },
  "restart": {
    "enabled": true,
    "max_restarts_per_window": 3,
    "window_minutes": 10,
    "wait_after_restart_seconds": 20
  },
  "token_scan": {
    "interval_minutes": 5
  },
  "agent": {
    "name": "my-host",
    "site": "",
    "api_token": ""
  },
  "server": {
    "host": "0.0.0.0",
    "port": 8000
  },
  "operations": {
    "backup_dir": "~/openclaw-agent-backups",
    "max_backups": 10
  }
}
```

### 3. 直接运行

```bash
cd /opt/openclaw-probe
./openclaw-probe
```

服务启动后监听 `http://0.0.0.0:8000`。

### 4. macOS launchd (开机自启)

复制 plist 文件并加载：

```bash
cp launchd/ai.openclaw.host-agent.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/ai.openclaw.host-agent.plist
```

查看状态：

```bash
launchctl list | grep openclaw
```

停止：

```bash
launchctl unload ~/Library/LaunchAgents/ai.openclaw.host-agent.plist
```

### 5. Docker

```bash
docker build -t openclaw-probe .
docker run -d \
  --name openclaw-probe \
  --network host \
  -v $(pwd)/config.json:/app/config.json \
  -v $(pwd)/data:/app/data \
  -v ~/.openclaw:/root/.openclaw:ro \
  -e TZ=Asia/Shanghai \
  openclaw-probe
```

或使用 docker-compose：

```bash
docker compose up -d
```

## API 端点

所有端点前缀为 `/api`，与 Python 版完全兼容。

### 公开端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/instances` | 所有实例状态 |
| GET | `/api/instances/{name}` | 单个实例详情 |
| GET | `/api/events` | 事件日志 |
| GET | `/api/tokens/summary` | Token 用量汇总 |
| GET | `/api/tokens/trend` | Token 用量趋势 |
| GET | `/api/tokens/agent-heatmap` | Agent 活跃热力图 |
| GET | `/api/tokens/pricing` | 模型定价表 |

### 认证端点 (需要 `X-Agent-Token` 或 `Bearer`)

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/agent/health` | Agent 健康状态 |
| GET | `/api/agent/snapshot` | 完整快照（Dashboard 用） |
| POST | `/api/agent/instances/{name}/restart` | 重启实例 |
| POST | `/api/agent/instances/{name}/start` | 启动实例 |
| POST | `/api/agent/instances/{name}/stop` | 停止实例 |
| POST | `/api/agent/instances/{name}/desired-state` | 设置期望状态 |
| GET | `/api/agent/instances/{name}/lifecycle` | 生命周期状态 |
| POST | `/api/agent/maintenance/enter` | 进入维护模式 |
| POST | `/api/agent/maintenance/exit` | 退出维护模式 |
| GET | `/api/config` | 读取配置 |
| PUT | `/api/config` | 更新配置 |
| GET | `/api/agent/openclaw/version` | OpenClaw 版本信息 |
| GET | `/api/agent/openclaw/latest-release` | 最新 Release |
| GET | `/api/agent/tasks` | 任务列表 |
| GET | `/api/agent/tasks/{task_id}` | 任务详情 |
| GET | `/api/agent/backups` | 备份列表 |
| GET | `/api/agent/backups/{backup_id}` | 备份详情 |
| POST | `/api/agent/backups/create` | 创建备份 |

## 从 Python 版迁移

1. 编译 Rust 版二进制
2. 复制现有 `config.json` 和 `data/probe.db` 到新目录
3. 停止 Python 版服务
4. 启动 Rust 版二进制
5. Dashboard 无需任何修改

数据库格式完全兼容，SQLite 文件可以直接复用。

## 与 Python 版的差异

- **升级/恢复** — 简化实现，升级操作返回提示信息
- **性能** — 内存占用降低约 90%，启动时间从秒级降到毫秒级
- **部署** — 单二进制，无需 Python 环境
