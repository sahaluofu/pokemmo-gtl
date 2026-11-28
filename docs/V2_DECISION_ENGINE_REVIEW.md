# PokeMMO GTL 项目现状与 v2.0 决策系统方案（Codex 联合审查文档）

> **文档目的**：将当前已构建并验收的 **v1.0 基线系统** 与新提出的 **v2.0 跨领域买卖决策引擎方案** 合并整理为一份自洽的技术审查材料，供 Codex 对架构合理性、量化建模严谨性、市场机制假设、工程复杂度与实施优先级进行**对抗式审查（Adversarial Review）**。
>
> **合规硬红线（不可违背）**：本项目严格限定为**外部只读官方 API 采集 + 离线研究 + 人工游戏内手操决策辅助**。严禁任何游戏进程读写、抓包、模拟键鼠、自动买卖或抓取第三方网站。

---

## 一、已构建基线：v1.0 现状清单（Verified Ground Truth）

- **代码仓库**：`d:\pokemmo脚本`（GitHub: `https://github.com/sahaluofu/pokemmo-gtl`，分支 `main`，基线提交 `4c52753`）
- **质量与验收状态**：
  - `ruff check src tests`：**0 errors**
  - `pytest -v`：**371 passed / 0 failed**（12 个测试模块，含严格防未来函数泄漏测试 `tests/test_no_future_leakage.py`）
  - 90 天 `--env sim` 全链路端到端实测通过（`data/gtl_sim.db`）：`8,640` 次快照、`1,708,088` 条挂单、`1,671,745` 条过期明细清理、`398,610` 条 K 线、`129` 条预测；72h Purged Walk-Forward 回测：`naive` (AUC `0.4974`, Net `0.00%`)、`statistical` (AUC `0.7332`, Net `-7.17%`)、`lgbm` (AUC `0.9318`, Pinball `0.022829`, Net `+11.57%`, MaxDD `1.44%`, 343 trades)。
  - **关键事实声明**：上述高分均来自**确定性合成仿真器**（`gtl_sim.db`）；真实官方实盘数据库 `gtl_live.db` 目前**尚未积累真实历史快照**。

### 1.1 现网代码架构与核心契约

| 层级 | 文件路径 | 已实现的核心能力与契约 |
| :--- | :--- | :--- |
| **配置层** | `config/settings.yaml`<br>`config/fees.yaml`<br>`config/item_relations.yaml` | 轮询间隔 900s、令牌桶 28,000/305s、原始挂单保留 48h、三层成交权重 (`alpha_near=0.75, alpha_mid=0.20, alpha_far=0.0`)；精确费用 (`rate=0.05`, `item_min_fee=100`, `pokemon_min_fee=1000`, `max_fee_per_slot=25000`, `default_relist_rate=0.15`)；RP 点券 100/500/1000 与 3 种树果（Leppa/Lum/Sitrus）上游种子权重 |
| **数据库层** | `src/pokemmo_gtl/db/migrations/001_init.sql`<br>`connection.py` / `repository.py` | 10 张 SQLite `STRICT` 表（WAL 模式）：`schema_migrations`, `items_catalog`, `snapshot_runs`, `raw_listings`, `inferred_flows`, `item_bars`, `manual_quotes`, `market_events`, `predictions`, `backtest_runs`。所有因果对齐以 `item_bars.close_ts` 为准 |
| **采集层** | `ingestion/official_api.py`<br>`rate_limiter.py` / `catalog_sync.py`<br>`simulator.py` / `manual_import.py` | 官方 GTL 分页拉取 + 限流器；商品目录同步；确定性市场仿真器；手工报价/CSV 导入（用于官方 API 未覆盖的精灵/素材资产） |
| **行情流水线** | `pipeline/snapshot_diff.py`<br>`pipeline/aggregator.py`<br>`pipeline/retention.py` | 相邻快照按 `auction_id` 差分计算 `new_listed_qty`, `partial_fill_qty`, `early_disappear_qty`, `expired_qty`, `est_sold_qty`, `est_sold_vwap`；聚合 `15m / 4h / 1d` 防钓鱼 K 线（`robust_best_ask`, `p10_price`, `depth_within_5pct`, `spread_top5_pct`）；48h 后清理 `raw_listings` |
| **费用与特征** | `quant/fees.py`<br>`quant/features.py` | `Decimal` 精确分槽位手续费、期望重挂费用、保本卖价 `breakeven_sell_price`；53 维严格因果特征 `FEATURE_COLUMNS`（价格/动量/波动、深度/推断成交、周期时间、公开事件距离、RP 宏观指数、上游种子成本差）；固定到期目标 `add_targets(horizon_hours=24|72|168)` |
| **预测与回测** | `quant/models/{base,naive,statistical,lgbm_model}.py`<br>`quant/predictor.py`<br>`quant/backtester.py` | 分位数回归（q10/q50/q90 + CQR 校准）+ Platt 概率校准；Tukey 25/50/25 期望卖价；三态信号 `BUY_WATCH` (`expected_net_roi >= 0.02 and prob_up >= 0.55`) / `NEUTRAL` / `SELL_CAUTION` (`< 0`)；Purged Walk-Forward 全局回测 |
| **CLI 与 Web** | `src/pokemmo_gtl/cli.py`<br>`src/pokemmo_gtl/web/{app,schemas}.py`<br>`web/static/{index.html,app.js,styles.css}` | 8 个 CLI 子命令（`init-db`, `sync-catalog`, `import-csv`, `simulate`, `collect`, `forecast`, `backtest`, `serve`）；14 个本地回环安全 API 端点；纯本地 ECharts 4-Tab 看板（市场总览 / 商品分析 / 回测评估 / 数据与事件） |

### 1.2 v1.0 面向真实买卖决策时的 6 大结构性缺口

1. **G1 — 零实盘数据校准**：当前所有量化指标均来自合成仿真数据，尚未经历真实 GTL 非平稳噪声、版本冲击与巨鲸砸盘的检验。
2. **G2 — 固定到期假设（`t + H`）脱离真实交易路径**：`features.py:653-760` 的 `add_targets()` 假设在 `t` 买入并在**精确时刻 `t + 24/72/168h`** 按 `min(close_price, p10_price)` 成交退出；无法刻画中途止盈、止损，也不评估挂在 `p10` 需要排多久才卖得掉。
3. **G3 — 缺失盘口阻力墙与压价速率持久化**：`raw_listings` 48h 后即被 `retention.py` 清理，历史盘口深度分布（某价位堆积数千个货的铁壁）与每小时压价频率（Undercut Velocity）未入库也未可视化。
4. **G4 — 只按单笔 `expected_net_roi` 排序，忽略资金周转率与槽位效率**：`models/base.py:61-67` 未考虑资金占用天数与 GTL 挂单槽位稀缺性，容易选出低流动滞销品。
5. **G5 — 缺失持仓与挂单状态机**：`001_init.sql` 没有 `positions` / `trade_journal` / `decision_tickets`，系统不知道用户持有何物、成本价、挂单到期剩余时间与前方压单量，因此无法给出真正的“抛售/撤单重挂/止损”指令。
6. **G6 — 跨商品关系库太窄**：`config/item_relations.yaml` 仅覆盖 3 种树果与 3 档 RP 券，缺少更广泛的结构性平价/成本底扫描。

---

## 二、问题重定义与跨领域最优解（First-Principles Reframing）

### 2.1 对原始需求的重构
- **原始问题**：“预测某件商品的涨跌程度，判断什么时候买、什么时候卖。”
- **诊断结论**：
  - **手段 vs 目标**：预测单品涨跌幅度（`pred_q50`）只是手段，不是目标；真正目标是**在不可退 5% 单边上架税（单槽 100~25,000 封顶）、单边卖单簿压价竞争、有限资金与 GTL 槽位约束下，最大化风险调整后的资金日化回报率（RoCD）与单槽位日收益（Yen / Slot-Day）**。
  - **症状 vs 原因**：价格涨跌是滞后症状；在 ~5.75%+ 摩擦成本市场上，纯价格序列外推信噪比低，真正可重复的 Alpha 来自**结构性供需失衡**（卖墙吸收、库销比恶化/收紧、活动日历供给周期、RP 货币体制变化、跨商品生产成本与平价偏离）。
  - **核心目标函数**：

$$\text{E}[\text{RoCD}] = \frac{P(\text{fill} \le T) \cdot \text{NetProfit}(P_{\text{buy}}, P_{\text{sell}}, Q, r_{\text{relist}}) - \big(1 - P(\text{fill} \le T)\big) \cdot \text{Loss}_{\text{exit}}}{P_{\text{buy}} \cdot Q \cdot \text{E}[\text{DaysToExit}]}$$

### 2.2 五大跨领域方法映射

1. **量化金融微观结构**：
   - **Triple-Barrier Labeling（三重障碍：止盈上轨 / 止损下轨 / 时间垂直轨）+ Meta-Labeling**（由结构性剧本决定方向，ML 仅估计扣费后先触达止盈线的校准概率并控制仓位）。
   - **Avellaneda-Stoikov 库存衰减定价**（持仓时间越长、仓位越重，挂单越贴近 `best_ask` 换取流动性）。
   - **四分之一凯利（Fractional Kelly）+ 流动性上限仓位控制**。
2. **电商动态定价与收益管理（Amazon FBA / Yield Management）**：
   - **库销比（Days-of-Supply, DoS）** = 盘口 5%/10% 深度 ÷ 日均推断成交量；
   - **Buy-Box 压价生存分析（Survival Analysis / Time-to-Fill, TTF）**：估计不同挂单价档位在 24h/48h 内成交 vs 被压价过期的概率；
   - **RoCD 与单槽位日化收益排序**。
3. **大宗商品与宏观经济**：
   - **生产成本底（Cost-of-Production Floor）**（种子→树果、RP 锚、礼袋期望值 EV）；
   - **季节性/活动周期体制识别**（常态 / 节前蓄水 / 节中抽水抛压 / 节后修复）。
4. **运筹学组合优化（Operations Research）**：
   - 带 4 重约束的多维分配问题：`总资金上限`、`可用 GTL 槽位上限`、`单品日均销量 15%~25% 流动性硬顶`、`同板块相关性敞口 <= 35%`，并显式利用**单槽 50 万货值以上 25,000 税费封顶**带来的有效税率折价红利。
5. **预测市场与决策论**：
   - 非对称损失评估（踏空成本 = 0，被套与重复缴纳 5% 重挂税是硬亏损）；
   - **纸面模拟盘（Paper Trading）+ 实盘 Brier / 滑点 / TTF 校准审计**，漂移超阈值自动降级为 `OBSERVE_ONLY`。

### 2.3 四类结构性交易剧本（替代“全市场统一预测”）

| 剧本 | 核心机制 | 典型持有期 | 核心入场逻辑 |
| :--- | :--- | :--- | :--- |
| **A. 事件/节日周期** | 活动前囤货 / 节中产出过剩砸盘 / 节后供给断流 | 7~45 天 | 处于活动窗口关键节点 + 价格较节前中枢显著回撤 + 新增卖盘增速拐头下降 |
| **B. 流动性冲击错杀** | 大户集中砸穿盘口，短时跌破均衡价 | 1~4 天 | `robust_best_ask` 跌破 7 日 `p10` 且 `z_score <= -2.0` + 上方无巨墙 + `DoS <= 2.5天` 证明需求活跃 |
| **C. 跨商品平价/成本底** | RP 券面额差价 / 种子-树果成本 / 礼袋 EV | 2~7 天 | 相对理论锚定价扣税后折价 >= 7% + 价差历史回归半衰期可控 |
| **D. 高周转刚需做市** | 跨时区潮汐 / 盘口前档真空裂口 | 6~36 小时 | 极高日均成交量 + `TTF < 12h` + 扣税净利 >= 3% 且 RoCD 全市场前 10% |

**卖出与压价重挂算术规则**：
- 分批阶梯挂单（50% 避墙快速周转档 `min(target, wall_price - 1)`，50% 目标溢价档）；
- 严禁在超过 2 天销量的卖盘阻力墙**上方**挂单；
- 仅当 `前方压单等待机会成本 + 继续下跌风险 > 单次重挂 5%（上限 25k）不可退税费` 时才撤单重挂（经验规则：前方压单 `< 0.5 天` 销量不重挂，`> 2 天` 且持续压价则果断重挂或止损）；
- 触发 **止盈 / 时间止损 / 上游逻辑证伪** 任一条件即按库存衰减定价执行退出。

---

## 三、拟议的 v2.0 工程演进蓝图（Proposed Changes）

按依赖分三阶段推进，完全向后兼容 `001_init.sql` 与现有 371 个测试：

### Phase 1：微观盘口、平价引擎与持仓/纸面模拟盘（零 ML 依赖，真实数据首日可用）
1. **`src/pokemmo_gtl/db/migrations/002_decision_and_portfolio.sql`**（新增 4 张 `STRICT` 表）：
   - `orderbook_walls`：每次快照在 `retention.py` 清理前持久化 `best_ask`, `wall_price`, `wall_qty`, `wall_days_of_supply`, `qty_ahead_p10`, `undercut_events_1h`, `undercut_velocity_pct_h`, `days_of_supply_5pct`, `days_of_supply_10pct`, `price_ladder_json`。
   - `positions`：支持 `mode IN ('paper', 'live')`、剧本标签、状态机（`OPEN_HOLDING | LISTED | PARTIAL_SOLD | CLOSED | STOPPED`）、成本/挂单价/到期时间/重挂次数/止盈止损与最大持有时限。
   - `trade_journal`：记录买入、挂单、重挂、成交、止损流水，支撑滑点与校准审计。
   - `decision_tickets`：持久化行动工单（`playbook`, `action`, `buy_zone_low/high`, `recommended_qty/slots`, `target_sell_price`, `stop_loss_price`, `expected_ttf_hours`, `fill_prob_48h`, `expected_net_roi`, `expected_rocd`, `expected_yen_per_slot_day`, `invalidation_rule`）。
2. **`src/pokemmo_gtl/quant/microstructure.py`**：从 `raw_listings` 提取阻力墙、压价速率、DoS 与单槽 50 万封顶税费最优批量。
3. **`src/pokemmo_gtl/quant/parity.py`** + 扩充 `config/item_relations.yaml`：RP 券交叉平价、生产配方成本底、礼袋期望值扫描。
4. **`src/pokemmo_gtl/quant/portfolio.py`**：持仓状态机、每次快照自动巡检压单量并输出 `HOLD / RELIST / TAKE_PROFIT / TIME_STOP_EXIT`、以及 `paper` 模式基于真实盘口穿价的自动结算与滑点统计。

### Phase 2：三重障碍 Meta-Labeling、存活时间估计与 RoCD 组合优化器
5. **`src/pokemmo_gtl/quant/triple_barrier.py`**：路径依赖三重障碍标签（15m/4h 路径 + 成交量吸收校验），仅在 4 类剧本触发样本上做 Meta-Labeling 概率校准（保留 `features.add_targets` 兼容）。
6. **`src/pokemmo_gtl/quant/survival.py`**：基于 `auction_id` 生命周期（冷启动用 DoS 启发式，满 14 天真实数据切换经验 Kaplan-Meier 分位生存表）估计 `fill_prob_24h`, `fill_prob_48h`, `expected_ttf_hours`, `expected_relists`。
7. **`src/pokemmo_gtl/quant/decision_engine.py`**：聚合剧本 + Meta 概率 + 存活时间 + 避墙价位，结合 `config/portfolio.yaml`（总资金、槽位上限、单品流动性帽、板块相关性帽）求解并生成按 RoCD / 单槽日收益排序的 `decision_tickets`。
8. **`src/pokemmo_gtl/quant/backtester.py`**：升级为路径依赖 + 多槽位组合回测，增加 `Realized RoCD`、`Capital/Slot Utilization`、`Relist Cost Ratio`、`Brier Score`，并强制与**纯剧本规则基线（无 ML）**同台对照。

### Phase 3：决策工作台 UI、CLI 与实盘准入门槛
9. **`web/app.py` + `web/static/*` + `cli.py`**：
   - Tab 1 升级为「行动工单台（Action Desk）」（宏观体制徽章、资金/槽位进度、RoCD 榜单、一键加入 Paper/Live 持仓）；
   - Tab 2 增加 ECharts 盘口深度墙阶梯图 + DoS/压价趋势；
   - 新增 Tab 5「持仓与卖出驾驶舱（Portfolio & Exit Desk）」与校准审计面板；
   - CLI 新增 `decide`, `position`, `audit`。
10. **14 天真实数据 + 纸面模拟盘准入门槛**：至少 7~14 天真实 `gtl_live.db` 数据 + ≥30 笔闭环纸面模拟交易，要求扣全税重挂后胜率 ≥ 58%、净 RoCD > 0、TTF 中位误差 < 40%、且 ML 跑赢纯规则基线后才建议投入主力资金。

---

## 四、给 Codex 的重点对抗式审查清单（请直接挑刺与重构建议）

请从**量化研究员 + 系统架构师 + PokeMMO 资深地精/游戏经济设计师**三个视角，重点审查并反驳以下问题：

1. **可辨识性与统计陷阱（Identifiability）**：
   - 在仅靠 15 分钟一次的单边卖单簿（Ask-Only）快照 + `auction_id` 差分的前提下，`survival.py` 试图区分“真实成交”与“卖家手动撤单/改价”，以及估算 `fill_prob_48h` / `expected_ttf_hours`，在统计上是否**严重不可辨识（censored / confounded）**？是否存在更简单、噪声更低的替代估计量？
   - 15 分钟轮询间隔内发生的“上架即秒买”或“高频压价战”会被漏采样，这对 `undercut_velocity_pct_h` 和剧本 D（高周转做市）的可靠性有多大杀伤力？剧本 D 是否应该直接砍掉或限制标的？
2. **费用与市场微观机制遗漏（PokeMMO GTL Mechanics）**：
   - 单槽 5% 上架税、`min_fee=100`、`max_fee_per_slot=25,000`（单槽货值 ≥ 50 万后有效税率单调下降）是一条**强非线性凹函数**：它是否会扭曲当前 `triple_barrier.py` 与 `decision_engine.py` 的仓位优化（例如拆槽 vs 并槽、高单价不可堆叠道具 vs 低单价 99 堆叠道具）？
   - PokeMMO 挂单有效期、搜索排序规则、买方单次扫货部分成交（`partial_fill_qty`）以及卖盘“假墙/自撤墙（Spoofing）”是否会让避墙挂单策略 `wall_price - 1` 反而被诱导降价？
3. **过拟合与过度工程化审查（YAGNI & Pragmatism）**：
   - 在 `gtl_live.db` 目前 0 天真实数据的情况下，Phase 1–3 的整体设计是否**过度设计**？哪些模块（如 `triple_barrier.py`、`survival.py`、运筹背包求解器）应该严格后置甚至简化为透明可解释的规则？
   - 相比再写一套复杂 ML（Meta-Labeling），把精力优先花在**真实数据探索（EDA）+ 剧本 A/B/C 的确定性规则 + 持仓压价监控**，长期期望收益是否更高？
4. **数据库、流水线耦合与数据泄漏审查（Engineering & Leakage）**：
   - `orderbook_walls` 与现有 `aggregator.py` / `retention.py` 的执行时序强耦合（必须在 `prune_raw_listings` 之前落库）；SQLite WAL 下多表写入事务、幂等重放（re-run / backfill）和 `002` 迁移设计是否有隐患？
   - `triple_barrier.py` 与 `backtester.py` 如果引入“路径内成交量吸收校验”和“纸面穿价撮合”，最容易在哪些环节引入**隐蔽的未来函数泄漏**或**乐观撮合偏差（Optimistic Fill Bias）**？
5. **更好的替代思路（Missing Alternatives）**：
   - 除了文档提到的 5 个跨领域方法与 4 类剧本，是否遗漏了更适合 PokeMMO GTL 经济体（例如：资金小体量 vs 大体量完全不同的最优策略、以 RP 为计价本位而非名义金币本位防通胀幻觉、或把“机会筛选”做成纯告警流而非复杂预测分数）？

> **期望 Codex 输出格式**：
> 1. 致命缺陷 / P0 必须修正的问题（附具体原因与修正方案）
> 2. 过度工程化 / 建议砍掉或延后的模块
> 3. 遗漏的关键机制或更优的数学/工程替代解法
> 4. 修订后的精简分阶段路线图建议
