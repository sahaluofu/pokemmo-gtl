# PokeMMO GTL Market Research & Price Forecasting Workbench (`pokemmo-gtl`)

一个严格合规、纯外部只读的 **PokeMMO 全球交易行（GTL）市场研究、量化建模与价格预测本地工作台**。

> **合规声明**：本项目仅面向外部只读行情分析与量化研究，数据来源限定为 PokeMMO 官方只读 Auctions API（`https://api.pokemmo.com/api/auctions/items`）、开源静态物品词典（`PokeMMO-Tools/pokemmo-data`）、用户手工/CSV 导入以及本地确定性仿真器。本项目**不包含且严禁任何**游戏客户端交互、内存读取、抓包、自动化点击或第三方站点爬虫代码。

---

## 核心特性

- **双库物理隔离（`live` / `sim`）**：
  - `--env live` 对应 `data/gtl_live.db`，`--env sim` 对应 `data/gtl_sim.db`。
  - 硬性隔离：在 `live` 环境下运行 `simulate`（CLI 或 Web `/api/admin/trigger`）会被直接拦截（CLI 非零退出，Web 返回 `403 Forbidden`）。
- **官方只读 API 与跨进程令牌桶限流**：
  - 支持游标分页采集、`follow_redirects=False`（`3xx/401/403` 立即中止鉴权错误）与指数退避重试。
  - 基于 `data/api_limiter.db` 实现跨进程持久化滚动限流（`28,000 tokens / 305s`）与单飞采集租约锁。
- **反钓鱼盘口聚合与隐含成交估计**：
  - 前缀去钓鱼滤波识别异常低价挂单，计算稳健最优卖价（`robust_best_ask`）、盘口深度与卖压。
  - 三档价格条件快照差分估计隐含成交量，物化 `15m / 4h / 1d` 多周期 K 线，并按分块事务定期清理过期明细。
- **精确多槽位 `Decimal` 税费引擎**：
  - 完整建模 PokeMMO GTL 5% 挂单费（道具最低 100 / 宝可梦最低 1,000 / 单槽位封顶 25,000、按 `max_stack_size` 拆槽、不可退还费与重挂率），精确求解最小整数保本价。
- **零未来泄露因果特征与三档冷启动预测**：
  - 所有特征严格对齐 `close_ts`，通过三重未来扰动与未公告突袭事件零泄露测试。
  - 按历史 K 线样本量自动路由：Tier 0 规则基线 → Tier 1 Ridge 统计模型 → Tier 2 LightGBM + CQR（分位数区间 `Q10 / Q50 / Q90` + 扣税净 ROI + Top-5 驱动因子）。
  - 全局 Purged Walk-Forward 扣税回测引擎（严格消除前瞻泄露与重叠持仓）。
- **离线 ECharts 5.6 本地 Web 工作台**：
  - 12 个标准 REST 接口 + 离线打包 `echarts.min.js`（零外链 CDN 依赖）。
  - 默认仅绑定 `127.0.0.1`（非回环地址需显式 `--allow-lan`），内置 `Host`/`Origin` 防护、统一错误信封、`NaN/±Inf -> null` 严格 JSON 清洗、有界流式 CSV 导入（≤5 MiB / 50,000 行）与防 Excel 公式注入 CSV 导出。

---

## 快速开始

### 1. 环境准备（Python 3.12+）

```powershell
python -m venv .venv
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

> Windows PowerShell 建议直接调用 `.\.venv\Scripts\python.exe -m ...`，避免脚本编码或执行策略问题。

### 2. 离线仿真模式快速体验（无需官方 Token）

```powershell
$env:PYTHONUTF8 = "1"
# 1) 初始化仿真库并加载离线词典（43 种核心道具与节日日历）
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli init-db --env sim
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli sync-catalog --env sim --offline-only

# 2) 生成 90 天确定性仿真行情并聚合多周期 K 线
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli simulate --days 90 --seed 42 --env sim

# 3) 生成 24h / 72h / 168h 多周期预测与 72h Purged Walk-Forward 回测
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli forecast --env sim --horizons 24,72,168
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli backtest --env sim --horizon 72 --models naive,statistical,lgbm

# 4) 启动本地 Web 研究工作台（http://127.0.0.1:8000）
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli serve --env sim --host 127.0.0.1 --port 8000
```

### 3. 实盘只读模式（`--env live`）

1. 将 `.env.example` 复制为 `.env`，填入你在 PokeMMO 官方申请的 `Private-Token`：
   ```dotenv
   POKEMMO_API_TOKEN=your_64_char_hex_token_here
   ```
2. 初始化实盘库、同步词典并采集行情：
   ```powershell
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli init-db --env live
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli sync-catalog --env live
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli collect --env live --once
   # 或按 15 分钟定频采集：
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli collect --env live --daemon --interval 900
   ```
3. 生成预测并启动实盘看板：
   ```powershell
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli forecast --env live --horizons 24,72,168
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli serve --env live --host 127.0.0.1 --port 8000
   ```

---

## 开发测试与质量检查

```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest -v
```

当前全量测试套件共 **371 项**（覆盖数据库 `STRICT` 约束、多槽位 `Decimal` 税费、官方 API 与限流器、反钓鱼聚合与分块清理、零未来泄露特征、冷启动模型与 Purged 回测、全部 Web REST 安全契约与 CLI 端到端链路）。

---

## 目录结构

```text
config/                  # 配置文件、节日日历、中文别名与离线种子词典
docs/                    # 架构总规范、分阶段任务单与验收报告
src/pokemmo_gtl/
  cli.py                 # Typer 统一命令行入口
  config.py              # YAML 配置与环境隔离加载器
  db/                    # SQLite STRICT 迁移与 BEGIN IMMEDIATE 事务封装
  ingestion/             # 官方只读 API、跨进程限流、词典同步、CSV 导入与仿真器
  pipeline/              # 反钓鱼聚合、三档价格条件差分与分块数据清理
  quant/                 # Decimal 税费、因果特征、三档预测模型与 Purged 回测器
  web/                   # FastAPI 安全服务、Pydantic 契约与离线 ECharts 前端
tests/                   # 371 项自动化单元测试与端到端测试
```

## 文档参考

- 架构与接口总规范：[docs/ARCHITECTURE_PLAN.md](docs/ARCHITECTURE_PLAN.md)
- 分阶段验收清单：[docs/CODEX_TASKS.md](docs/CODEX_TASKS.md)
- 交付与实测报告：[docs/PROGRESS_CHECKPOINT.md](docs/PROGRESS_CHECKPOINT.md)
