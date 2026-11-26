# PokeMMO GTL 市场研究与价格预测系统 — 交付与验收报告（100% 完成）

> **完成时间**：2026-11-25 23:47 (UTC+8)  
> **最终状态**：✅ **M0 – M6 全部里程碑 100% 完成并通过端到端终验收**（`ruff check src tests` 0 错误；全量 `pytest` **371/371 passed**；90 天 `--env sim` 仿真 → 预测 → Purged Walk-Forward 回测 → M6 本地 FastAPI 真实服务冒烟全部通过）。

---

## 一、运行环境与工具链状态

| 组件 | 位置 / 配置 | 状态 |
| :--- | :--- | :--- |
| **Python 3.12** | 本地 Python `3.12+` 虚拟环境 `.venv`（已可编辑安装 `pokemmo-gtl` 及全部依赖） | ✅ 就绪 |
| **编码规范** | `PYTHONUTF8=1`、全量 `pathlib.Path` + UTF-8 显式编码 | ✅ 已验证 |
| **离线 ECharts** | `src/pokemmo_gtl/web/static/vendor/echarts.min.js`（官方 `echarts@5.6.0`，`1,034,102` 字节） | ✅ 已验证 |
| **本地仿真演示库** | `data/gtl_sim.db`（本地按需生成，已通过 `.gitignore` 隔离） | ✅ 就绪 |
| **官方 API Token** | 申请官方只读 `Private-Token` 后填入本地 `.env` 的 `POKEMMO_API_TOKEN` 即可运行 `--env live` | ⏳ 按需配置 |

---

## 二、各里程碑终验收明细（M0 – M6）

| 阶段 | 状态 | 核心交付文件 | 最终验收指标 |
| :--- | :--- | :--- | :--- |
| **规划与联审** | ✅ 完成 | `docs/ARCHITECTURE_PLAN.md`（v2.0）、`docs/CODEX_TASKS.md` | 双审阅子代理交叉严审，18 项高危架构隐患全部闭环 |
| **M0：脚手架与双库** | ✅ 通过 | `pyproject.toml`, `config/*.yaml`, `src/pokemmo_gtl/config.py`, `db/{connection,repository}.py`, `db/migrations/001_init.sql`, `tests/{conftest,test_db}.py` | 10 张 `STRICT` 表 + 9 索引 + `BEGIN IMMEDIATE` + 物理双库隔离 |
| **M1：词典/手工/仿真/税费** | ✅ 通过 | `config/seed_catalog_minimal.json`（43 条）, `ingestion/{catalog_sync,manual_import,simulator}.py`, `quant/fees.py`, `tests/{test_fees,test_simulator}.py` | 多槽位 `Decimal` 税费（5% / 单槽封顶 25,000 / 15% 重挂率）、精确最小整数保本价、UTF-8 BOM 有界导入 |
| **M2：官方 API 与限流** | ✅ 通过 | `ingestion/{rate_limiter,official_api}.py`, `tests/{test_rate_limiter,test_official_api}.py` | `data/api_limiter.db` 跨进程令牌桶（28,000 / 305s）+ 单飞租约锁 + `follow_redirects=False` |
| **M3：差分/聚合/清理** | ✅ 通过 | `pipeline/{snapshot_diff,aggregator,retention}.py`, `tests/{test_snapshot_diff,test_aggregator_and_retention}.py` | 前缀去钓鱼滤波 + 三档价格条件差分 + 两阶段 K 线聚合 + 分块清理 |
| **M4：因果特征防泄露** | ✅ 通过 | `quant/features.py`, `tests/test_no_future_leakage.py` | 严格 `close_ts` 因果特征 + 三重未来扰动/未公告事件零泄露测试通过 |
| **M5：分层模型与回测** | ✅ 通过 | `quant/models/{base,naive,statistical,lgbm_model}.py`, `quant/{predictor,backtester}.py`, `tests/{test_models_unit,test_models_and_backtest}.py` | 三档冷启动路由（规则/Ridge/LightGBM+CQR）+ 全局 Purged Walk-Forward 扣税回测 |
| **M6：Web 工作台与 CLI** | ✅ 通过 | `web/{schemas,app}.py`, `web/static/{index.html,styles.css,app.js,vendor/echarts.min.js}`, `cli.py`, `tests/{test_web_api,test_cli_e2e}.py` | 全部 12 个 REST 接口 + 离线 ECharts 看板 + `Host`/`Origin` 安全防护 + `live` 禁跑仿真 `403` + 全命令统一 CLI |

---

## 三、终验收实测数据

1. **静态检查与全量自动化测试**
   - `.\.venv\Scripts\python.exe -m ruff check src tests`：`All checks passed!`
   - `.\.venv\Scripts\python.exe -m pytest -v`：**371 passed, 0 failed**（含 179 项 Web API 与安全契约测试、10 项 CLI 端到端测试、14 项预测与回测测试、85 项官方 API 与限流测试等）

2. **90 天 `--env sim` 全链路实测**（`seed=42`，`8,640` 帧快照，累计 `1,708,088` 条挂单，分块清理 `1,671,745` 条过期明细，保留 `398,610` 条多周期 K 线，生成 `129` 条 24h/72h/168h 预测）
   - **72h 全局 Purged Walk-Forward 扣税回测对比**（含 5% 多槽位挂单费与 15% 重挂率）：
     - `naive`：ROC-AUC `0.4974`｜Pinball `0.039150`｜扣税净收益 `0.00%`｜最大回撤 `0.00%`｜交易 `0` 笔
     - `statistical`：ROC-AUC `0.7332`｜Pinball `0.063932`｜扣税净收益 `-7.17%`｜最大回撤 `15.47%`｜交易 `397` 笔
     - `lgbm`：**ROC-AUC `0.9318`**｜**Pinball `0.022829`**｜**扣税净收益 `+11.57%`**｜**最大回撤 `1.44%`**｜交易 `343` 笔

3. **M6 本地服务真实启动冒烟验收**（`serve --env sim --host 127.0.0.1 --port 18765` → 探活后自动优雅回收）
   - `GET /api/health`：`env=sim`, `item_count=43`, `bar_count=398610`
   - `GET /`：`HTTP 200`；`GET /static/vendor/echarts.min.js`：`1,034,102` 字节
   - `GET /api/overview?horizon=72`：`rp_index=45.536`, `top_bullish=10`, `top_movers_24h=10`
   - `GET /api/items/1192/bars?timeframe=4h&days=90`：`540` 根 4h K 线；`GET /api/items/1192/forecast?horizons=24,72,168`：`3` 个周期预测

---

## 四、日常使用指令速查

### 1. 立即启动仿真研究工作台（当前 `data/gtl_sim.db` 已就绪）
```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe -m pokemmo_gtl.cli serve --env sim --host 127.0.0.1 --port 8000
```
浏览器访问 `http://127.0.0.1:8000` 即可使用（总览、物品 K 线与 CQR 预测区间、扣税税费计算器、Purged Walk-Forward 回测、手工/CSV 录入与事件日历）。

### 2. 拿到官方 `Private-Token` 后切入实盘模式（`--env live`）
1. 复制 `.env.example` 为 `.env`，填入 `POKEMMO_API_TOKEN=你的官方Token`（严禁上传或提交 `.env`）。
2. 初始化实盘库并同步物品词典：
   ```powershell
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli init-db --env live
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli sync-catalog --env live
   ```
3. 单次验证采集或后台每 15 分钟定频采集：
   ```powershell
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli collect --env live --once
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli collect --env live --daemon --interval 900
   ```
4. 生成预测并启动实盘工作台：
   ```powershell
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli forecast --env live --horizons 24,72,168
   .\.venv\Scripts\python.exe -m pokemmo_gtl.cli serve --env live --host 127.0.0.1 --port 8000
   ```
