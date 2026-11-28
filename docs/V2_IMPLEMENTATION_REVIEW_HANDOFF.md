# PokeMMO GTL v2 实施交付与自审闭环报告（供外部独立审查）

> **审查基线**：Git `HEAD = 4c52753`（工作区未提交变更，基于 [V2_DECISION_ENGINE_PLAN_R2.md](./V2_DECISION_ENGINE_PLAN_R2.md) 终审闭环可执行版完整实现）
> **合规红线声明**：纯只读官方 Auctions API + 本地离线研究 + 用户手工记账；无游戏进程读写、无抓包、无模拟键鼠/宏、无自动交易、无第三方抓取；`data/gtl_live.db` 与 `data/gtl_sim.db` 物理隔离。

---

## 一、总体交付规模与验证结果

| 维度 | 实测结果 |
| :--- | :--- |
| **代码变更统计（相对 `4c52753`）** | 修改 **19** 个文件（`+4,349 / -889` 行）；新增 **5** 个生产/配置文件（`3,738` 行）+ **11** 个 v2 专项测试文件（`9,274` 行） |
| **静态检查（`ruff check src tests`）** | **All checks passed**（0 错误 / 0 告警） |
| **全库测试（`pytest`）** | **838 passed, 0 failed**（v1 基线 `371 passed` 100% 兼容 + 新增 **467** 条 v2 对抗与集成用例） |
| **现网库隔离状态（`data/gtl_live.db`）** | `migrations=['001_init']`，`snapshots=0, bars=0, bar_revs=0, preds=0`（完全未污染） |
| **v2 30 天仿真库（`data/gtl_sim.db`）** | `migrations=['001_init', '002_v2_ledger_and_observations']`；`snapshots=2,880`，`item_bars=132,870`，`item_bar_revisions=371,520`，`inferred_flow_revisions=123,797`，`orderbook_observations=123,840`，`predictions=129` |
| **Web 本地看板联调（`127.0.0.1:8000`）** | 浏览器实测 `/api/health`、`/api/overview`、`/api/events`、`/api/items`、`/api/items/*/bars`、`/api/items/*/forecast` 全 `200`，零 JS 控制台异常，K 线/预测扇形/盘口卖压/Top-5 驱动图均正常渲染（已停服释放端口） |

---

## 二、终审 9 项阻塞与补齐项闭环对照（含内部 3 路对抗审阅再修复）

| 终审编号 | 要求摘要 | 实现位置 | 对应专项测试 |
| :--- | :--- | :--- | :--- |
| **Final P1-1** | 杜绝 `MIN(raw_retained)=1` 跳过 `NULL`；前驱裁剪/未知时禁止覆盖已物化结果或做半截修订 | `src/pokemmo_gtl/pipeline/retention.py` (`check_frames_retained`)<br>`src/pokemmo_gtl/pipeline/snapshot_diff.py` (`_split_replayable`)<br>`src/pokemmo_gtl/pipeline/aggregator.py` | `tests/test_bitemporal_pipeline.py`（覆盖 `[NULL,1,1]`、`[0,1,1]` 及前驱裁剪后迟到帧零写入） |
| **Final P1-2** | 消除同秒发布主键冲突；仿真重叠窗必须显式 `--reset` 且防并发绕过 | `src/pokemmo_gtl/db/migrations/002_v2_ledger_and_observations.sql` (`publish_events`, `item_bar_revisions` PK 含 `publish_seq`)<br>`src/pokemmo_gtl/ingestion/simulator.py`（路径锁 + 单事务重叠校验与 `running` 占位） | `tests/test_migration_002.py`<br>`tests/test_bitemporal_pipeline.py`<br>`tests/test_ingestion_v2.py` |
| **Final P1-3** | 迟到帧按名义 `(server_started_at, snapshot_id)` 重算前后邻对并级联相邻 `15m/4h/1d` 桶，保留旧 `as_of` 版本 | `src/pokemmo_gtl/pipeline/snapshot_diff.py`<br>`src/pokemmo_gtl/pipeline/aggregator.py` (`query_bars_as_of`) | `tests/test_bitemporal_pipeline.py`（`12:10 -> 12:20 -> 迟到 12:14` 拆分 `6 -> 3+3`） |
| **Final P1-4** | 冻结前向日历切折、三类截止时间分离、默认路径也执行逐时点因果与尾部未成熟决策保留 | `src/pokemmo_gtl/quant/backtester.py` (`freeze_walk_forward_calendar`, `run_backtest`)<br>`src/pokemmo_gtl/quant/predictor.py` (`_load_feature_panel`) | `tests/test_backtester_v2.py`（覆盖删尾部不漂移 `split_ts`、迟到修订不泄漏、默认路径保留末尾未成熟决策） |
| **Final P1-5** | 按 `arm_id` 独立可重放账本、同费不同价 `DIRECT_EDIT`、重挂 `parent_order_id`、跨组合领 `batch_claim_id` 分摊校验 | `src/pokemmo_gtl/quant/ledger.py` (`append_ledger_events`, `replay_ledger`)<br>`src/pokemmo_gtl/cli.py` (`ledger-replay`) | `tests/test_ledger_and_risk.py`<br>`tests/test_upgrade_001_to_002_e2e.py` |
| **Final P1-6** | 001 有数据老库无损升级 002；显式投影消除 `SELECT *` + `extra_forbidden`；普通手工/CSV 报价双写 `item_bar_revisions` 并贯通特征/预测/回测 | `src/pokemmo_gtl/ingestion/manual_import.py`<br>`src/pokemmo_gtl/web/schemas.py` / `web/app.py` / `web/static/app.js` | `tests/test_manual_import_v2.py`<br>`tests/test_upgrade_001_to_002_e2e.py` |
| **Final P2-1** | `api_covered=0` 禁止精灵分类写 `empty_confirmed`；`mixed_ended_empty` 隔离历史 `close_price` 与当前报价/RP/上游特征；上游特征逐行按有效权重归一防 `1.0 -> 0.5` 漂移 | `src/pokemmo_gtl/ingestion/catalog_sync.py`<br>`src/pokemmo_gtl/pipeline/aggregator.py`<br>`src/pokemmo_gtl/quant/features.py`<br>`src/pokemmo_gtl/web/app.py` | `tests/test_ingestion_v2.py`<br>`tests/test_bitemporal_pipeline.py`<br>`tests/test_features_v2_leakage.py` |
| **Final P2-2** | 阶段二 B 组试单可复算门槛：以 `review_windows` 为分母、隔离 `AUDIT_SAMPLE` / `NO_ALERT_CONTROL`，同时校验 `min_client_confirmation_rate`（0.50）与 `max_expired_or_missed_rate`（0.35） | `config/trial_protocol.yaml`<br>`src/pokemmo_gtl/quant/trial_report.py` | `tests/test_trial_report.py`（含 `1 确认 + 99 及时但不达标` 拦截反例） |
| **Final P2-3** | 整批总成本 `989,000` / 99 件无整除截断保本与多价 `BUY` 归并；`per_unit` 扫货与 `bundle_only` 精确子集 DP（区分 `60+39` vs `50+49`）；期初基准统一累计损失预算 `check_open_loss_budget` | `config/fees.yaml`<br>`src/pokemmo_gtl/config.py`<br>`src/pokemmo_gtl/quant/fees.py`<br>`src/pokemmo_gtl/quant/entry_calc.py`<br>`src/pokemmo_gtl/quant/ledger.py` | `tests/test_fees_v2.py`<br>`tests/test_entry_calc.py`<br>`tests/test_ledger_and_risk.py` |

---

## 三、编码后内部 3 路对抗审阅发现并已修复的问题清单

在全量实现后，执行了 3 路独立代码对抗审阅（A1 流水线、A2 账本/试验、A3 特征/回测/Web），共抓出 6 个边界缺陷，均已完成修复并补齐回归测试：

1. **Web 删除事件路由笔误**（`web/app.py:875`）：`event_id=item_id` 导致 `NameError` → 已修正为 `event_id=event_id`。
2. **极端 Ridge 外推触发 SQLite 64 位整数溢出**（`quant/models/base.py`, `quant/predictor.py`）：`log_returns_to_prices` 裁剪上限 `2**62` 且 `predictions` 写入列显式转 `float` 对齐 `STRICT REAL`。
3. **前驱已裁剪时迟到帧触发部分后继改写**（`pipeline/snapshot_diff.py`）：在分配 `publish_seq` 之前增加 `_split_replayable` 全量前置门禁（顺向尾帧保留当前盘口发布，乱序插帧任一必需帧/已物化父桶未留存则零写入退出）。
4. **并发 `simulate(..., reset=False)` 重叠检查竞态**（`ingestion/simulator.py`）：加路径级锁并将过期清理、`completed` + 活跃 `running` 重叠检查、首帧 `running` 占位合并进同一写事务。
5. **阶段二门禁遗漏客户端达标确认率下限**（`config/trial_protocol.yaml`, `quant/trial_report.py`）：新增 `min_client_confirmation_rate: 0.50` 冻结红线并纳入 `gate_checks` 与 `b_passes_gate`。
6. **`run_backtest` 默认无参路径未启用 v2 因果与尾部不变切折**（`quant/backtester.py`）：全路径强制通过 `visible_panel` 隔离未来修订（无延迟修订库走等价切片快路径），凡含 v2 发布/水印的数据库默认采用固定 7 天前向步长并保留末尾未成熟决策（纯未发布 001 旧库保持兼容）。

---

## 四、已知待实盘人工核验与非阻塞观察项

1. **客户端实测待完成（工程外前提）**：`config/fees.yaml` 当前 `verified: false`、`POKEMMO_API_TOKEN` 尚未配置；需用户按计划 §2.1 在游戏客户端核对按槽位/按件计费、`Pending` 可见延迟与 PC 改价费用后再开启实盘核验标记（未核验前 v2 门禁默认阻断新买信号）。
2. **前端非阻塞排版观察**：浏览器核验发现 `单品分析` 页滚动至中部时，顶部粘性横幅会轻微遮挡图表工具栏，且窄视口（`796×794`）需滚动或切至宽视口（`1280×800`）点击标签切换，不影响数据准确性与接口契约。

---

## 五、变更文件清单（供重点抽查）

- **配置与迁移**：`config/fees.yaml`、`config/trial_protocol.yaml`、`src/pokemmo_gtl/db/migrations/002_v2_ledger_and_observations.sql`
- **采集与流水线**：`src/pokemmo_gtl/ingestion/{official_api,simulator,catalog_sync,manual_import}.py`、`src/pokemmo_gtl/pipeline/{snapshot_diff,aggregator,retention}.py`
- **量化、费用、账本与试验**：`src/pokemmo_gtl/config.py`、`src/pokemmo_gtl/quant/{fees,entry_calc,ledger,trial_report,features,predictor,backtester}.py`、`src/pokemmo_gtl/quant/models/base.py`
- **接口与前端**：`src/pokemmo_gtl/web/{schemas,app}.py`、`src/pokemmo_gtl/web/static/app.js`、`src/pokemmo_gtl/cli.py`
- **新增测试**：`tests/test_{migration_002,fees_v2,ledger_and_risk,entry_calc,trial_report,ingestion_v2,bitemporal_pipeline,manual_import_v2,features_v2_leakage,backtester_v2,upgrade_001_to_002_e2e}.py` 及 `tests/test_db.py`
