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
- **双时态发布序列与反钓鱼盘口聚合（v2）**：
  - 引入 `publish_events(publish_seq)`、`item_bar_revisions` 与 `inferred_flow_revisions`，支持乱序迟到帧按名义时间重算邻对差分并级联相邻 `15m / 4h / 1d` 桶，保留历史 `as_of` 视角不可篡改。
  - 前缀去钓鱼滤波识别异常低价挂单，区分 `stocked / mixed_ended_empty / empty_confirmed` 盘口状态，隔离未覆盖分类（`api_covered = 0`）与空盘历史价；分块清理过期明细时严格校验逐帧留存。
- **版本化 `Decimal` 税费引擎、整批入场测算与分组可重放账本（v2）**：
  - 完整建模 PokeMMO GTL 5% 挂单费（支持 `per_slot` / `per_unit` 版本化配置、`verified` 核验门禁、离散改价/重挂费用与整批总成本精确最小整数保本价）。
  - 提供 `entry_calc`（逐档扫货 / 整挂子集 DP）、`ledger`（按 `arm_id` 独立可重放事件流、`DIRECT_EDIT` / `RELIST` / 跨组合领 `batch_claim_id` 分摊校验与期初基准累计损失预算硬门禁）及 `trial_report`（阶段二预登记人工窗口对照与可复算双红线门禁）。
- **零未来泄露因果特征、三档冷启动预测与冻结日历回测（v2）**：
  - 所有特征严格对齐 `close_ts`（含逐行有效权重归一的上游特征、带 TTL 与空盘屏障的 RP 特征，以及双写 `item_bar_revisions` 的普通手工/CSV 报价），通过未来扰动与未公告突袭事件零泄露测试。
  - 按历史 K 线样本量自动路由：Tier 0 规则基线 → Tier 1 Ridge 统计模型 → Tier 2 LightGBM + CQR（分位数区间 `Q10 / Q50 / Q90` + 扣税净 ROI + Top-5 驱动因子）。
  - 冻结前向步长日历与三类截止时间分离的因果 Purged Walk-Forward 扣税回测引擎（保留未成熟决策与未平仓持仓）。
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

# 2) 生成 30 天确定性仿真行情并聚合多周期 K 线（重叠时间窗重跑需显式 --reset）
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli simulate --days 30 --seed 42 --env sim --reset

# 3) 生成 24h / 72h / 168h 多周期预测与 72h Purged Walk-Forward 回测
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli forecast --env sim --horizons 24,72,168
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli backtest --env sim --horizon 72 --models naive,statistical,lgbm

# 4) 重放本人账本流水并核对各组余额与在途仓位
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli ledger-replay --env live

# 5) 启动本地 Web 研究工作台（http://127.0.0.1:8000）
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

当前全量测试套件共 **838 项**（在 v1 基线 371 项 100% 兼容基础上，新增 467 项 v2 专项测试，覆盖 001→002 增量无损迁移、双时态发布序列与迟到帧邻桶差分、版本化费用、整批入场测算、分组可重放账本与累计损失预算、阶段二预登记双红线门禁、上游防泄漏特征、冻结日历因果回测与 Web 显式投影契约）。

---

## 目录结构

```text
config/                  # 配置文件、版本化税费、阶段二冻结试验协议、节日日历与离线词典
docs/                    # 架构总规范、v2 决策辅助实施计划与审查交接文档
src/pokemmo_gtl/
  cli.py                 # Typer 统一命令行入口（含 ledger-replay、simulate --reset）
  config.py              # YAML 配置与环境隔离加载器
  db/                    # SQLite STRICT 迁移（001/002）与 BEGIN IMMEDIATE 事务封装
  ingestion/             # 官方只读 API、跨进程限流、词典同步、双写 CSV 导入与仿真器
  pipeline/              # 双时态修订、反钓鱼聚合、邻桶差分与留存门禁清理
  quant/                 # 版本化税费、入场测算、分组账本、试验报告、因果特征与回测器
  web/                   # FastAPI 显式投影安全服务、Pydantic 契约与离线 ECharts 前端
tests/                   # 838 项自动化单元测试、对抗反例测试与端到端测试
```

## 文档参考

- 架构与接口总规范：[docs/ARCHITECTURE_PLAN.md](docs/ARCHITECTURE_PLAN.md)
- v2 决策引擎实施计划（终审闭环版）：[docs/V2_DECISION_ENGINE_PLAN_R2.md](docs/V2_DECISION_ENGINE_PLAN_R2.md)
- v2 实施交付与审查交接报告：[docs/V2_IMPLEMENTATION_REVIEW_HANDOFF.md](docs/V2_IMPLEMENTATION_REVIEW_HANDOFF.md)
- 分阶段验收清单：[docs/CODEX_TASKS.md](docs/CODEX_TASKS.md)
- 交付与实测报告：[docs/PROGRESS_CHECKPOINT.md](docs/PROGRESS_CHECKPOINT.md)
