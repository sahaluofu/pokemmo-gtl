# Codex CLI 分阶段执行任务单（v2.0 联审定稿：M0 – M6）

> **执行铁律**：每次仅执行 **一个里程碑（M0 → M1 → ... → M6）**。完成单阶段编码与全部验收命令后，**立即停止并汇报验收输出**，由架构师（Antigravity）完成代码审阅与结果核验后，再启动下一里程碑。

---

## 全局硬性守则（所有阶段必须遵守）

1. **以 `docs/ARCHITECTURE_PLAN.md`（v2.0）为唯一事实标准**，不得删减字段、约束或更改接口签名。
2. **禁止使用 `Activate.ps1`**：在 Windows PowerShell 中统一使用 `.\.venv\Scripts\python.exe -m ...` 执行 `pip`、`ruff`、`pytest` 与 `pokemmo_gtl.cli`，每次运行前确保 `$env:PYTHONUTF8="1"`。
3. **保护系统盘空间与特殊路径安全**：测试临时目录与缓存必须落在仓库内（`.pytest_tmp`、`.ruff_cache`），不得写入系统临时目录。
4. **合规与安全红线**：仅在仓库目录内操作；严禁触碰游戏客户端、抓包或爬取任何第三方网页；严禁硬编码或打印 `POKEMMO_API_TOKEN`。

---

## Milestone M0：`.venv`、工程脚手架、配置系统与 `STRICT` SQLite 双库

### 任务目标
使用 Python 3.12 创建 `.venv`，搭建完整工程骨架、YAML 配置加载器、幂等 `STRICT` SQLite 迁移引擎与 `BEGIN IMMEDIATE` 安全事务封装。

### 需创建的文件
- `.gitignore`（包含 `.venv/`、`data/*.db*`、`.env`、`.pytest_tmp/`、`.ruff_cache/`、`__pycache__/`、`*.egg-info/`）
- `.env.example`
- `pyproject.toml`：
  - 依赖：`httpx>=0.27`, `pydantic>=2.7`, `pyyaml>=6.0`, `typer>=0.12`, `pandas>=2.2`, `numpy>=1.26`, `scikit-learn>=1.5`, `lightgbm>=4.5`, `statsmodels>=0.14`, `fastapi>=0.115`, `uvicorn[standard]>=0.30`, `python-multipart>=0.0.9`
  - 开发依赖：`pytest>=8.2`, `ruff>=0.6`, `respx>=0.21`
  - 脚本入口：`gtl = "pokemmo_gtl.cli:app"`
  - 包含 package-data（`db/migrations/*.sql`, `web/static/**/*`），并配置 `basetemp = ".pytest_tmp"` 与 `cache-dir = ".ruff_cache"`
- `config/settings.yaml`、`config/fees.yaml`、`config/events.yaml`、`config/item_aliases_zh.yaml`、`config/item_relations.yaml`（严格按 `ARCHITECTURE_PLAN.md` §3.3 填充真实合理的默认配置，涵盖万圣节/圣诞节/元旦/春节窗口）
- `src/pokemmo_gtl/__init__.py`、`src/pokemmo_gtl/config.py`、`src/pokemmo_gtl/cli.py`（实现 `gtl init-db --env live|sim`）
- `src/pokemmo_gtl/db/connection.py`（短生命周期连接工厂 + 每次打开自动设置 `WAL / NORMAL / foreign_keys=ON / busy_timeout=30000` + `write_transaction` 显式 `BEGIN IMMEDIATE`）
- `src/pokemmo_gtl/db/migrations/001_init.sql`（严格按 `ARCHITECTURE_PLAN.md` §3.2 全部 10 张 `STRICT` 表与 9 个索引创建）
- `src/pokemmo_gtl/db/repository.py`
- `tests/test_db.py`

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
if (-not (Test-Path ".\.venv\Scripts\python.exe")) { python -m venv .venv }
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_db.py -v
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli init-db --env sim
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli init-db --env live
```
- `test_db.py` 必须验证：`--env sim` 与 `--env live` 物理文件分离、WAL 与 `busy_timeout=30000` 生效、`STRICT` 表拒绝向 `INTEGER` 时间戳列写入 ISO 字符串、多线程并发读+`BEGIN IMMEDIATE` 写零 `SQLITE_BUSY` 报错、重复迁移幂等。

---

## Milestone M1：词典同步、安全手工/CSV 导入、固定锚点仿真器与多槽位 `Decimal` 税费

### 任务目标
实现离线兜底词典、防覆盖手工/CSV 录入、确定性时间锚点市场仿真器、以及多槽位精确 `Decimal` GTL 税费与保本二分求解器。

### 需创建/更新的文件
- `config/seed_catalog_minimal.json`（30~50 个核心道具，正确区分可堆叠道具 `max_stack_size=99` 与不可堆叠时装/精灵分类 `max_stack_size=1`，包含 100/500/1000 RP 点券、护符金币、PP 果、精灵球、节日礼袋及 `>=900000` 精灵素材分类）
- `src/pokemmo_gtl/ingestion/catalog_sync.py`（支持 GitHub `PokeMMO-Tools/pokemmo-data` 拉取、本地目录加载、合并 `item_aliases_zh.yaml` 与 `seed_catalog_minimal.json` 离线兜底）
- `src/pokemmo_gtl/ingestion/manual_import.py`（`utf-8-sig` 流式 CSV 限制 `<=5MB / <=50,000` 行；写入 `manual_quotes` 并向 `item_bars` `4h`/`1d` 桶 upsert，带 `WHERE item_bars.data_source != 'snapshot'` 保护）
- `src/pokemmo_gtl/ingestion/simulator.py`（**仅允许 `env == "sim"` 运行，`env == "live"` 直接抛错拒绝**；默认固定 `end_ts = 1768435200` 即 `2026-01-15T00:00:00Z`，生成包含周内周期、万圣/圣诞崩盘与节后爬升、RP 通胀漂移、盘口卖压收窄先导信号、1 元钓鱼单及高位撤单潮的确定性快照）
- `src/pokemmo_gtl/quant/fees.py`（严格使用 `decimal.Decimal(str(x))` 实现 `divmod(qty, max_stack_size)` 多槽位挂单费、净利与动态倍增上界整数二分 `breakeven_sell_price`）
- 更新 `src/pokemmo_gtl/cli.py`（新增 `sync-catalog`, `import-csv`, `simulate`）
- `tests/test_fees.py`、`tests/test_simulator.py`

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_fees.py tests/test_simulator.py -v
```
- `test_fees.py` 必须覆盖：①低价单 100 下限；②常规 5% 区间；③单槽 99 件超 50 万封顶 25,000；④250 件树果拆 3 槽（`99+99+52`）计费；⑤5 只高价精灵/时装拆 5 槽（最高 `125,000`）；⑥浮点边界 `relist_rate=0.10, fee=100` 及低价宠 `buy_price=50, min_fee=1000` 下 `net_profit(breakeven) >= 0` 且 `net_profit(breakeven - 1) < 0`。
- `test_simulator.py` 必须验证：同 `seed` 两次运行生成的数据库内容逐字节/逐行完全相等（不随真实时钟变化），且对 `env="live"` 调用抛异常。

---

## Milestone M2：跨进程持久化令牌桶、单飞锁与官方 Auctions API 采集器

### 任务目标
严格按 `ARCHITECTURE_PLAN.md` §2 实现官方 API 采集器与 `data/api_limiter.db` 跨进程限流和互斥保护。

### 需创建/更新的文件
- `src/pokemmo_gtl/ingestion/rate_limiter.py`（SQLite 持久化 `api_rate_ledger` 305s/28,000 tokens 滑动窗口 + `collector_locks` 单飞互斥锁 + 可注入 `clock`/`sleeper` 测试桩）
- `src/pokemmo_gtl/ingestion/official_api.py`（`follow_redirects=False`、超时配置、全 `3xx`/`401`/`403` 抛脱敏 `ApiAuthError`、`429`/`5xx`/网络超时退避重试、显式 `count=1000`、仅在 `[]` 或 `min_id <= 1` 停止、游标递减断言、内存 `auction_id` 去重、解析 `Date` 头 `server_started_at`、两阶段无锁网络拉取 + 原子落库）
- 更新 `src/pokemmo_gtl/cli.py`（新增 `collect --once` 与 `collect --daemon --interval 900` start-to-start 调度）
- `tests/test_rate_limiter.py`、`tests/test_official_api.py`（全 `respx` Mock，零真实外网依赖）

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_rate_limiter.py tests/test_official_api.py -v
```
- 测试覆盖：两次独立进程/实例调用共享 `api_limiter.db` 配额、并发采集触发 `CollectorBusyError`、全 `3xx` 拦截且异常不含明文 Token、首页无 `skip`、绝不发 `skip<=0`、跨页重复 `auction_id` 去重、中途第 2 页失败时 `raw_listings` 零残留且 `snapshot_runs.status == 'failed'`。

---

## Milestone M3：前缀去钓鱼聚合、价格条件差分与分块热表清理

### 任务目标
实现 `ARCHITECTURE_PLAN.md` §4.1–§4.2 的抗钓鱼盘口指标、三档条件权重差分、两阶段 `15m -> 4h/1d` 聚合与 20 帧分块 `retention.py`。

### 需创建/更新的文件
- `src/pokemmo_gtl/pipeline/snapshot_diff.py`
- `src/pokemmo_gtl/pipeline/aggregator.py`
- `src/pokemmo_gtl/pipeline/retention.py`
- 将差分、两阶段聚合与清理串联进 `collect` 与 `simulate`
- `tests/test_snapshot_diff.py`、`tests/test_aggregator_and_retention.py`

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_snapshot_diff.py tests/test_aggregator_and_retention.py -v
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli sync-catalog --env sim --offline-only
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli simulate --days 90 --seed 42 --env sim
```
- 测试必须验证：①100,000 正常盘口混入 1 件 1 元钓鱼单时，`robust_best_ask`、`vwap_top5_qty` 与 `depth_within_5pct` 完全不受污染，同时 `best_ask_min == 1`；②高于清盘价 `+20%` 的撤单不计入 `est_sold_qty`（`alpha_far == 0`）；③`4h`/`1d` 桶的 `total_listed_qty` 等于桶末值而非多帧累加；④手工报价绝不覆盖 `'snapshot'` 桶；⑤未知 `reference_id` 先补录 `items_catalog` 再写明细不触发外键错误；⑥`retention` 分批清理后保留最新至少 2 帧 `completed` 快照。

---

## Milestone M4：`close_ts` 因果特征工程与全方位防泄露不变量测试

### 任务目标
实现规则 `4h` 网格重索引、`close_ts` 严格向后 `merge_asof`、`announced_at` 事件门控与执行摩擦感知的目标标签。

### 需创建的文件
- `src/pokemmo_gtl/quant/features.py`
- `tests/test_no_future_leakage.py`

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_no_future_leakage.py -v
```
- `test_no_future_leakage.py` 必须同时执行三重对抗扰动（选定阈值时刻 $T$）：
  1. 将**全部物品**（含目标物品、RP 点券、上游关联物品、`1d` 手工报价）在 `close_ts > T` 的价格与数量全部乘以 `100`；
  2. 插入一条突袭事件（`start_ts = T + 86400`, `announced_at = T + 3600`）；
  3. 断言所有物品在 `close_ts <= T` 的每一列特征值（含 `NaN` 位置）与扰动前 **逐元素完全一致**。

---

## Milestone M5：对数收益面板 LightGBM、Platt + CQR 校准与全局 Purged 回测

### 任务目标
实现 `Naive`、Tier 0/1 `Statistical`（含 `SimpleImputer`）、Tier 2 面板 `LightGBM`（对数收益 + 唯一样本权重 + Platt 概率校准 + CQR 共形分位区间）、Top-5 归因，以及全局 `close_ts` Purged Walk-Forward 组合扣税回测器。

### 需创建/更新的文件
- `src/pokemmo_gtl/quant/models/base.py`
- `src/pokemmo_gtl/quant/models/naive.py`
- `src/pokemmo_gtl/quant/models/statistical.py`
- `src/pokemmo_gtl/quant/models/lgbm_model.py`
- `src/pokemmo_gtl/quant/predictor.py`
- `src/pokemmo_gtl/quant/backtester.py`
- 更新 `src/pokemmo_gtl/cli.py`（新增 `forecast --env sim`、`backtest --env sim --horizon 72`）
- `tests/test_models_and_backtest.py`

### 验收命令与通过标准
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest tests/test_models_and_backtest.py -v
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli forecast --env sim
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli backtest --env sim --horizon 72
```
- 测试必须验证：①Tier 0 / Tier 1（含 `30d` 特征全 `NaN` 场景不崩溃）/ Tier 2 自动路由正常；②通胀创新高序列上树模型预测对数收益可正常给出高于现价的目标价；③`pred_q10 <= pred_q50 <= pred_q90` 100% 成立；④Purged 切分中任意训练样本均满足 `train.close_ts + horizon_sec <= test_start_ts` 且单标的绝无重叠持仓；⑤在 90 天仿真集上 LightGBM 的 Pinball Loss 低于 Naive、ROC-AUC 高于 Naive、且扣税累计净收益率 `> 0%`。

---

## Milestone M6：FastAPI 安全服务、本地 ECharts 5.6 打包与量化工作台

### 任务目标
严格按 `ARCHITECTURE_PLAN.md` §5 实现全部 12 个标准 REST 接口、安全防护中间件、离线 `echarts.min.js` 打包及 4 页签可视化工作台，并通过非阻塞生命周期冒烟测试。

### 需创建/更新的文件
- 确定性获取真实官方 `echarts.min.js`（严禁手写假 stub）：
  ```powershell
  cmd /c "npm pack echarts@5.6.0 --pack-destination .tmp"
  tar -xzf ".tmp\echarts-5.6.0.tgz" -C ".tmp"
  New-Item -ItemType Directory -Force -Path "src\pokemmo_gtl\web\static\vendor" | Out-Null
  Copy-Item -Force ".tmp\package\dist\echarts.min.js" "src\pokemmo_gtl\web\static\vendor\echarts.min.js"
  ```
- `src/pokemmo_gtl/web/schemas.py`
- `src/pokemmo_gtl/web/app.py`（实现 §5.1–§5.3 全部安全规则、错误信封与 12 个 REST 路由）
- `src/pokemmo_gtl/web/static/index.html`、`styles.css`、`app.js`（4 个 Tab + 明暗主题 + K线/折线 + CQR 阴影预测带 + 卖压/成交副图 + 多槽位扣税计算器 + 回测净值图 + CSV 导入导出）
- 更新 `src/pokemmo_gtl/cli.py`（新增 `serve --env sim --host 127.0.0.1 --port 8000`）
- `tests/test_web_api.py`

### 验收命令与通过标准（有界不挂起！）
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m ruff check src tests
.\.venv\Scripts\python.exe -m pytest -v

# 有界生命周期冒烟测试（自动拉起 -> 探活 -> 强制关闭，绝不挂起终端）
$proc = Start-Process -FilePath ".\.venv\Scripts\python.exe" -ArgumentList "-m pokemmo_gtl.cli serve --env sim --host 127.0.0.1 --port 18080" -PassThru -WindowStyle Hidden
try {
    $ok = $false
    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep -Milliseconds 500
        try {
            $h = Invoke-RestMethod -Uri "http://127.0.0.1:18080/api/health" -TimeoutSec 2
            if ($h.env -eq "sim") { $ok = $true; break }
        } catch {}
    }
    if (-not $ok) { throw "Server health check timed out" }
    $html = Invoke-WebRequest -Uri "http://127.0.0.1:18080/" -UseBasicParsing -TimeoutSec 5
    if ($html.StatusCode -ne 200) { throw "Index page failed" }
    Write-Output "M6 Smoke Test PASSED: item_count=$($h.item_count), bar_count=$($h.bar_count)"
} finally {
    if ($proc -and -not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
}
```
- `test_web_api.py` 必须逐一覆盖全部 12 个 REST 接口，并断言：`echarts.min.js` 文件字节数 `> 500_000`、跨源伪造 `Origin: https://evil.example` 的 `POST` 请求被拒绝、在 `env="live"` 下调用 `POST /api/admin/trigger {"action": "simulate"}` 返回 `403`、含 `NaN` 特征序列返回合法 JSON `null`、导出含 `=CMD()` 备注的 CSV 自动转义为 `'=CMD()`。

---

## 每阶段审阅交接格式（Codex CLI 汇报模板）

每完成一个里程碑，立即停止并输出：
1. 本阶段新增/修改文件列表；
2. `ruff` 与 `pytest`（及 M6 冒烟脚本）完整终端输出；
3. 是否完全符合 `docs/ARCHITECTURE_PLAN.md`（如有任何细节调整请逐条说明）；
4. 等待架构师（Antigravity）审阅放行后再执行下一里程碑。
