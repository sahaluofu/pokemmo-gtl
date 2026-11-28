# PokeMMO GTL v2 决策辅助系统完整实施计划（终审闭环可执行版）

> **文档定位**：本文件 [V2_DECISION_ENGINE_PLAN_R2.md](./V2_DECISION_ENGINE_PLAN_R2.md) 已完整闭环首轮审计、二轮审阅及终审提出的全部 **6 项 P1 阻塞项与 3 项 P2 补齐项**，作为 Codex CLI 分模块并行编码与验收的唯一执行规范。
> **现网实况声明**：代码基线 `4c52753`（`pytest` **371 passed**）。本地 `data/gtl_live.db` 当前为 **0 次快照、0 根 K 线、0 条手工报价**，尚未配置 `POKEMMO_API_TOKEN`，现行费用与 `Pending` 行为仍待客户端实测核验（客户端核验与手工记账可与工程编码并行先行）。v1.0 中 LGBM 的 **+11.57%（343 笔）** 来自合成仿真库 `data/gtl_sim.db`，仅作测试工具，不代表任何实盘盈利预期。本计划不预设投入金额、持有期或实盘收益。
> **合规硬红线**：仅使用官方只读 Auctions API、用户手工记录与本地离线研究。买入、上架、改价、撤单和领取均由用户在客户端手操；严禁读写游戏进程、抓包、模拟键鼠、自动交易或抓取第三方站点。

## 目录

- [一、总体目标、首版交付与范围边界](#一总体目标首版交付与范围边界)
- [二、市场机制核验、整批算账与冻结日历回测规范](#二市场机制核验整批算账与冻结日历回测规范)
- [三、双时态发布序列、邻桶差分修订、混合空盘桶与上游修复](#三双时态发布序列邻桶差分修订混合空盘桶与上游修复)
- [四、增量迁移契约、手工双写、接口兼容与分组可重放账本](#四增量迁移契约手工双写接口兼容与分组可重放账本)
- [五、四阶段目标试验、可复算门槛与统一权益损失风控](#五四阶段目标试验可复算门槛与统一权益损失风控)
- [六、统一结算日核算、工程验收与后续投入条件](#六统一结算日核算工程验收与后续投入条件)
- [七、终审 9 项阻塞与补齐闭环核对表](#七终审-9-项阻塞与补齐闭环核对表)

---

## 一、总体目标、首版交付与范围边界

### 1.1 唯一评价目标与对照基准

在**相同初始金币、GTL 槽位、人工操作时段和损失预算**下，提高**固定结算日的净权益增量（$\Delta \text{NetEquity}$，整数金币计）**：
- 用 **持币不动** 衡量新买入是否值得；
- 用 **冻结的简单限价规则** 衡量额外入场信号（如流动性冲击 B）是否有增益；
- 现有库存的退出试验从可比库存出发，以 **固定挂价规则** 为对照；
- **资金日、槽位日与人工分钟**作为定位瓶颈的诊断指标，不以单笔比率指标（如 RoCD）作主优化目标。

### 1.2 首版交付与离线先行路径

首版交付**预登记人工窗口与机会观察表、可重放本人订单流水、规则对照报告**。客户端费用核验和手工记账无需等待 `POKEMMO_API_TOKEN` 或编码完成，可立即用独立受控表格起步；**硬性禁令**：严禁借用现网 `manual_import.py` / `manual_quotes` 录入观察记录（该通路会自动向 `item_bars` 物化 `4h/1d` K 线且仅支持单价）。官方 API 可用后先验收单次完整采集，再开启定时观察。

### 1.3 移出首版与后置清单

| 模块 / 原提案 | 处置 | 依据与重开条件 |
| :--- | :--- | :--- |
| 五页综合看板 | **移出首版** | 界面先复用现有工作台呈现核心动作与证据，按实操需要扩展 |
| 三重障碍 / Meta-Labeling / 凯利仓位 / 运筹优化 / 成交概率生存模型 | **后置评估** | 须先积累本人可核对订单结果，满足第六章 §6.3 瓶颈与样本外净金币增益条件 |
| 剧本 D（高频周转） | **后置排期** | 须待实测机会寿命覆盖采集跨度、提醒与人工操作延迟后再排期 |
| “14 天 + 30 笔纸单 + 58% 胜率”准入线 | **彻底删除** | 30 笔独立交易 18 胜的 95% Wilson 区间约 42%–75%，改用统一结算日分块净权益对照 |

---

## 二、市场机制核验、整批算账与冻结日历回测规范

### 2.1 费用、2026-11-19 `Pending` 阶段与改价机制核实

- **客户端逐项实测（未核实标的仅可观察、禁止试买）**：
  1. 同品同价预览 `1` 件与计划批量（如 `99` 件），区分逐件与逐槽计费（验算 99 件以 9,600 买、10,000 卖：总费 25,000 净赚 +14,600 vs 总费 49,500 净亏 -9,900），覆盖最低费、25,000 封顶、取整方向与不可堆叠物品；
  2. 分别实测整单未售直接改价（PC 开启）、部分售后直接改价、撤单重挂的追加费用、剩余量、到期时间与槽位变化；
  3. 实测 2026-11-19 更新引入的 `Pending` 阶段时长、`Pending` 期间撤单/改价费用退补规则，并确认官方 API 是否严格不含 `Pending` 挂单。
- **代码规范（`config/fees.yaml` + `quant/fees.py`）**：
  - 按已验证标的类型、操作类型与 **`effective_from` 生效时间**配置离散整数规则，输出与客户端预览及实扣收据逐笔 0 金币误差对齐；
  - **整批总成本保本入口 [终审 P2-3]**：提供 `breakeven_sell_price_for_cost(total_buy_cost: int, qty: int, ...)` 直接接受整数总成本（如 99 件共 `989,000` 金币，不可用单一整数单价表示），求满足 `sell_price * qty - discrete_fees >= total_buy_cost` 的最小整数单价；原 `breakeven_sell_price(buy_price, qty, ...)` 作为 `total_buy_cost = buy_price * qty` 的兼容薄封装。

### 2.2 逐单组购买限制（Lot-Level）、最高买入总价与非现金化回款

1. **逐单组（Lot）核验购买可行性**：
   - 记录客户端购买单位限制（`per_unit` 可拆买 vs `bundle_only` 仅整组买）。在 `bundle_only` 模式下，仅汇总同价总量无法判断是否可买：例如同价总量 99 件，`60 + 39` 可以整组买到 39 件，而 `50 + 49` 无法凑出 39 件。因此盘口留存与算账必须基于**每条挂单的单组数量（lots）**精确求解可行组合，同档库存只计一次。
   - 允许拆买时，1 件报 9,000 + 99 件报 10,000，买足 99 件实付总价为 $1 \times 9000 + 98 \times 10000 = 989000$（按 10,000 卖出扣费前毛差仅 1,000）。该整批总成本 `989,000` 直接贯通保本价计算与 §4.3 账本多价批次入账，比较不买、小批量与费用/堆叠边界可行批量。
2. **最高可接受买入总价公式**：
   $$\text{最高可接受买入总价} = \text{截止日前保守售出回款} + \text{剩余库存保守净估值} - \text{尚待支付且未计入回款或估值的费用} - \text{要求最低净利}$$
   预算同步预留当下应付费用；缺乏售出量、回款时间或尾仓价证据时，按预设的少售、部分售出、滞销情景输出区间。
3. **非现金化回款**：实盘严格区分已确认待领取款（`unclaimed_proceeds`）与实际可花现金（`available_cash`）；纸面回测保留部分售出与未售尾仓并防止多张纸单重复占用同一批推断流量。

### 2.3 冻结决策日历与三截止时间回测契约 [终审 P1-4]

彻底修正 `backtester.py:56-69, 376-378` 的前视筛选及未来行情改变折边界缺陷：

1. **冻结决策日历与各折边界 [终审 P1-4]**：
   - 废除在全量行情尾部上调用 `np.array_split(test_times, n_splits)` 的动态切分（实测删除决策点之后的数据会把同一决策所属折起点从第 14 格漂移到第 17 格，间接改写训练截止）；
   - `run_backtest()` 必须基于**预先冻结的固定步长评估日历或显式传入的 `calendar_folds`（固定每折 `(split_ts, test_start_ts, test_end_ts)`）**切分，折边界与数据库尾部是否存在未来行彻底无关；扰动防泄漏测试前后强制复用同一份冻结切分。
2. **训练截止（Train Label Maturity Cutoff：`label_available_at <= split_ts`）**：
   - 仅当训练样本的退出目标在冻结切分点 `split_ts` 之前已收盘且在 `split_ts` 时可见（`exit_close_ts <= split_ts` 且 `exit_bar_revision_available_at <= split_ts`），才允许进入 `train` 集拟合模型。
3. **测试出候选截止（Inference / Decision Cutoff：`decision_ts = T >= split_ts`）**：
   - 对测试区间 `[test_start_ts, test_end_ts]` 内每一决策时刻 $T$，固定读取边界 `(T, read_publish_seq)`，**仅读取在 $T$ 时刻已可见的特征版本（§3.1 不可变版本 + §3.4 无泄漏上游特征）**生成预测与候选，**严禁按未来标签是否存在过滤 `test` 行**。
4. **事后评分截止（Post-hoc Evaluation Cutoff：`eval_as_of`）**：
   - 仅当评估终时 `eval_as_of` 晚于某条预测的退出成熟时刻且退出行情可见时，才将其真实标签挂接计算 AUC / Brier / Pinball；**未成熟或缺退出报价的预测与交易必须完整留在决策记录与未结账本中**，评分汇总单独披露“已成熟可评分样本数”与“未成熟/无标签保留样本数”。
5. **强制验收测试**：
   - 复用同一份冻结日历与折边界：删除 `t + 24h`（及所有 `> T`）报价、补入 `quote_ts <= T` 但 `available_at > T` 的迟到帧、或补入 `> T` 才首次出现的上游原料报价，$T$ 所属折起点、训练样本集、特征、预测、候选列表、排序与预算状态 **100% 不变**；缺标签预测仍保留在决策记录中。

---

## 三、双时态发布序列、邻桶差分修订、混合空盘桶与上游修复

### 3.1 全局发布序列（`publish_seq`）、双时态 K 线版本与邻桶差分修订 [终审 P1-2 / P1-3]

**问题根因**：
1. 采集与手工写入时间戳精度为整秒，若同桶在同一秒发布两次版本，以 `(item_id, timeframe, bucket_ts, revision_available_at)` 为主键会撞键报错；
2. 现网 `snapshot_diff.py:67` 按 `snapshot_id < current` 找前驱。当 `12:10`(10件) 与 `12:20`(4件) 已先入库（`12:20` 记减量 `6`），随后补入迟到帧 `12:14`(7件) 时，迟到帧误取 `12:20` 为前驱导致时间间隔为负被跳过，且 `12:20` 仍错留减量 `6`。

**双时态发布序列与级联修订契约**：
1. **全局单调发布序号 `publish_seq` 与运行隔离 [终审 P1-2]**：
   - 新增单调表 `publish_events(publish_seq INTEGER PRIMARY KEY AUTOINCREMENT, available_at INTEGER NOT NULL, source_type TEXT NOT NULL, source_id INTEGER NOT NULL)`。每次快照完成提交或手工报价写入均在同一事务内申请唯一递增 `publish_seq`；
   - `item_bar_revisions` 主键改为 **`(item_id, timeframe, bucket_ts, publish_seq)`**（另存 `revision_available_at INTEGER NOT NULL`），彻底杜绝同秒撞主键；
   - 决策记录与观察表固定读取边界 **`(decision_ts, read_publish_seq)`**；点时查询读取已收盘 K 线时强制满足 **`close_ts <= decision_ts AND revision_available_at <= decision_ts AND (:read_publish_seq IS NULL OR publish_seq <= :read_publish_seq)`**，按 `(item_id, timeframe, bucket_ts)` 取 **`MAX(publish_seq)`** 对应版本；
   - **重复仿真运行隔离**：`--env sim` 禁止向已有重叠时间段的仿真库混写；若 `gtl_sim.db` 已存在时间重叠快照且未显式传入 `--reset`（清空重建）或独立输出库路径，`simulate` 直接报错拒绝。
2. **名义时间前驱/后继与双时态差分级联修订 [终审 P1-3]**：
   - 新增只增不改表 **`inferred_flow_revisions`**（主键 `(snapshot_id, item_id, publish_seq)`，含 `prev_snapshot_id`, `revision_available_at` 及全部流量列），同时保留 `inferred_flows` 为最新视图；
   - 快照名义全序键固定为 **`(server_started_at ASC, snapshot_id ASC)`**。当新快照 $S$（发布序号 `P`，可用时间 `avail_S`）提交时，按名义序定位最近前驱 $S_{\text{prev}}$ 与最近后继 $S_{\text{next}}$（仅迟到帧存在 $S_{\text{next}}$）：
     1. 校验 $S_{\text{prev}}$ 与 $S$ 均存在且 `raw_retained = 1` 后，计算 $S_{\text{prev}} \to S$ 差分，以 `publish_seq = P, revision_available_at = avail_S` 写入 `inferred_flow_revisions` 与 `inferred_flows`；
     2. 若存在后继 $S_{\text{next}}$ 且 $S$ 与 $S_{\text{next}}$ 均满足 `raw_retained = 1`，**在同一事务内以同一 `publish_seq = P, revision_available_at = avail_S` 重算后继差分 $S \to S_{\text{next}}$**，追加写入 `inferred_flow_revisions(snapshot_id=S_next, publish_seq=P)` 并刷新 `inferred_flows`（**`S_next` 此前已发布的旧差分版本完整保留不动**）；若 $S_{\text{next}}$ 已清理则拒绝覆写其既有差分；
     3. **级联追加相邻桶 K 线版本**：基于 `publish_seq <= P` 的最新帧与最新差分，分别向 `item_bar_revisions` 追加 **$S$ 所在 `15m` 桶**、**$S_{\text{next}}$ 所在相邻 `15m` 桶（若跨桶）**及所有受影响 `4h / 1d` 父桶的新版本（`publish_seq = P, revision_available_at = avail_S`），并刷新 `item_bars` 最新视图。
3. **必过验收用例**：
   - （a）单帧跨边界：`12:14` 开始、`12:17` 完成 → `12:15` K 线在 `12:16` 不可见、`12:17` 可见；
   - （b）同桶准时+迟到帧与未收盘门槛：首帧 `12:01`(`12:02` 可用) 入库后补入 `12:14`(`12:17` 可用)，`12:10` 不可见、`12:16` 恒定读到首帧版本、`12:17` 读到两帧合并版；
   - （c）同秒双版本写入不撞主键，且固定 `read_publish_seq` 可精确复现第一个版本；重复仿真重叠未加 `--reset` 被拒绝；
   - （d）**跨桶迟到差分修订**：`12:10`(10件, `12:11` 可用) 与 `12:20`(4件, `12:21` 可用) 入库后，`as_of=12:22` 查 `[12:15, 12:30)` 桶减量为 `6`；补入迟到帧 `12:14`(7件, `12:25` 可用) 后，`as_of=12:22` 查 `[12:15, 12:30)` 减量**仍为 `6`**，`as_of=12:25` 查 `[12:00, 12:15)` 减量为 `3`、`[12:15, 12:30)` 减量为 `3`。

### 3.2 采集覆盖前提、混合空盘桶与当前报价分离 [终审 P2-1]

1. **空盘判定严格以官方接口采集覆盖为前提**：
   - `items_catalog` 新增 **`api_covered INTEGER NOT NULL DEFAULT 1`**（官方道具接口未覆盖的本地精灵素材分类 `asset_class = 'pokemon_category'` 或 `item_id >= 900000` 置 **`0`**）；
   - `process_snapshot()` 的持续空盘扩展集合严格限定为 **`affected_items = 本帧有挂单标的 ∪ 本帧差分标的 ∪ (api_covered = 1 的活跃监控/持仓标的)`**。收藏或持有 `api_covered = 0` 的精灵分类**绝不写入 `empty_confirmed`**。
2. **精确定义桶状态、混合桶汇总与当前报价分离**：
   - `book_state` 取值 **`'stocked' | 'mixed_ended_empty' | 'empty_confirmed'`**（无完成快照桶特征层标 `'collection_missing'`）：
     - **`stocked`**：全价量非空；
     - **`empty_confirmed`**（全桶空盘）：全部价格列 `NULL`，库存列 `0`，流量独立汇总；
     - **`mixed_ended_empty`**（先有货后空盘）：历史 OHLC/`best_ask_min` 保留有货帧轨迹，**末帧当前报价列（`robust_best_ask / p10_price / spread_top5_pct`）为 `NULL`**，末帧库存列为 **`0`**，流量全桶汇总。
   - **历史 `close_price` 与当前有效报价强制分离**：`web/app.py:276` 总览/列表查询的 `current_price` 改为 `CASE WHEN b.book_state = 'stocked' THEN b.close_price ELSE NULL END`；`features.py` 的 RP 与上游特征同样仅采信 `book_state = 'stocked'` 的当前有效报价，彻底杜绝总览或 RP 指数继续读取空盘前的旧 `close_price`。
3. **按最新覆盖帧阻断信号**：仅最新覆盖帧为新鲜完整的 `stocked`（且 `total_listed_qty > 0`）才允许触发入场候选；末帧为空盘（`empty_confirmed` 或 `mixed_ended_empty`）或超 TTL 一律阻断。同标的同轮连续跳价按 `opportunity_group_id` 合并计为 1 个独立机会。

### 3.3 48 小时清理前逐单组留痕与防 `NULL` 绕过的留存校验 [终审 P1-1]

1. **逐单组留痕与决策溯源**：48h 清理前，`orderbook_observations.ladder_json` 保存逐条挂单单组明细（区分 `60+39` 与 `50+49`），同时保存 `purchase_mode`、`coverage_hours`、`wall_persist_frames`；`opportunity_observations` 持久化 **`source_snapshot_id`、`read_publish_seq`、`rule_version` 与 `fee_rule_version`**。
2. **杜绝 `MIN(raw_retained)` 放过 `NULL` 旧帧 [终审 P1-1]**：
   - `prune_raw_listings()` 删除明细与更新 `snapshot_runs.raw_retained = 0` 同事务提交；
   - 重放前**不带 `JOIN raw_listings` 直接查 `snapshot_runs`**，校验当前帧、前驱/后继帧及被重算整个 `15m/4h/1d` 桶全部完成快照满足：**所需记录均存在（`COUNT(*) > 0`）且 `SUM(CASE WHEN raw_retained = 1 THEN 0 ELSE 1 END) = 0`**（严禁使用会忽略 `NULL` 导致 `[NULL, 1, 1]` 误判为真的 `MIN(raw_retained) = 1`）。任一帧为 `0` 或 `NULL` 时，保留既有历史结果拒绝覆写。
   - **必测用例**：(1) 仅删同桶第 1 帧（`[0, 1, 1]`）；(2) **升级后旧帧为 `NULL` 的混合桶（`[NULL, 1, 1]`）**，两者重放均拒绝覆写既有结果。

### 3.4 上游特征与 RP 时效修复（本轮回测阻塞项）

1. 改造 `features.py:_add_upstream_features()`：对上游成分做带 `tolerance` 的后向因果对齐（过滤非 `stocked` 与超期报价），**逐行（逐时刻 `t`）仅对当前有效非过期成分累加权重分母 `sum_valid_abs_w(t)`**；
2. `features.py:_add_rp_features()` 与 `web/app.py` 统一折算 **`金币/RP`**、仅采信 `book_state = 'stocked'` 且带 `tolerance = max_rp_quote_age_sec`；
3. **必测用例**：直接调用 `build_feature_frame()` 与 `run_backtest()`（固定冻结日历），仅补入决策日 `T` 之后首次出现的一种原料报价，`T` 当天 `upstream_ret_24h`（不从 `1.0` 变 `0.5`）与回测决策 100% 不变；
4. 策略 C 上线前补全带 `effective_from` 版本的完整配方成本（缺项标 `NULL`）；策略 A 按 `announced_at <= decision_ts` 计算活动信号。

---

## 四、增量迁移契约、手工双写、接口兼容与分组可重放账本

增量迁移 `src/pokemmo_gtl/db/migrations/002_v2_ledger_and_observations.sql` 保持 SQLite `STRICT` + WAL + `BEGIN IMMEDIATE`，不提前创建后置 ML 或组合优化表。

### 4.1 既有表扩列、普通手工报价双写与接口兼容 [终审 P1-6]

1. **既有表扩列**：
   - `items_catalog` 新增 `api_covered INTEGER NOT NULL DEFAULT 1`（`pokemon_category` / `item_id >= 900000` 置 `0`）、`purchase_mode TEXT NOT NULL DEFAULT 'unknown'`、`fee_verified INTEGER NOT NULL DEFAULT 0`；
   - `snapshot_runs` 新增 `last_page_server_at INTEGER`、`available_at INTEGER`、`publish_seq INTEGER`、`raw_retained INTEGER`（新帧 `1` / 清理后 `0` / 旧行 `NULL`）、`raw_pruned_at INTEGER`；
   - `item_bars`（事务内重建保留索引）：放宽价格列可空，新增 `book_state TEXT NOT NULL DEFAULT 'stocked'`、`bar_available_at INTEGER`（旧行 `NULL`）、`latest_publish_seq INTEGER`；
   - `inferred_flows` 新增 `observed_undercut_count INTEGER NOT NULL DEFAULT 0`、`latest_publish_seq INTEGER`；
   - **`manual_quotes` 新增 `ingested_at INTEGER`**（普通手工/CSV 报价的系统实际录入时间，历史旧行留 `NULL`）。
2. **普通手工报价双写 `item_bar_revisions` [终审 P1-6]**：
   - 明确区分两条通路：普通手工报价（`manual_import.py` / `POST /api/quotes/manual` / `import-csv`）专用于官方 API 未覆盖的精灵/素材资产；机会观察表独立走 `review_windows` + `opportunity_observations` 绝不混用；
   - `manual_import.py:_materialize_bars()` 在受 `data_source != 'snapshot'` 保护写 `item_bars` 的**同一事务**内，申请单调 `publish_seq`，令 **`revision_available_at = MAX(recorded_at, ingested_at)`**（历史补录报价绝不在实际录入系统前被下游提前看见），同步将 `4h` / `1d` 手工 K 线版本追加写入 `item_bar_revisions(data_source='manual', ...)` 并刷新 `item_bars.bar_available_at`。
3. **接口与全链路升级兼容**：
   - 消除 `web/app.py:591` 等全部 `SELECT *` 改为显式列投影，`web/app.py:276` 的 `current_price` 仅对 `book_state = 'stocked'` 返回非空价格；同步升级 `schemas.py`（显式声明 `book_state`、`bar_available_at` 及空盘可空价格）与 `app.js` 断点绘制；
   - **升级全链路验收**：在**含历史数据的 `001` 库**执行 `002` 升级 → 回归 `/api/overview`、`/api/items`、`/api/items/{id}/bars`、`forecast` 与 `backtest` → 再经 `import-csv` / `POST /api/quotes/manual` 录入一条新精灵报价，验证在 `as_of < revision_available_at` 不可见、在 `as_of >= revision_available_at` 被特征提取、`forecast` 与 `backtest` **实际读入并改变特征/预测输出**。

### 4.2 新增最小表清单（共 9 张表：发布序 + 双时态桶/差分 + 留痕 + 窗口 + 观察 + 三张账本表）

| 表名 | 核心字段与关键约束 |
| :--- | :--- |
| **`publish_events`** [终审 P1-2] | `publish_seq INTEGER PRIMARY KEY AUTOINCREMENT`, `available_at INTEGER NOT NULL`, `source_type`（`'snapshot' \| 'manual_quote'`）, `source_id INTEGER NOT NULL` |
| **`item_bar_revisions`** [终审 P1-2] | 主键 **`(item_id, timeframe, bucket_ts, publish_seq)`**；另含 `close_ts`, `revision_available_at`, `data_source`, `book_state`, OHLC, 末帧价量与流量列，同秒不撞键 |
| **`inferred_flow_revisions`** [终审 P1-3] | 主键 **`(snapshot_id, item_id, publish_seq)`**；含 `prev_snapshot_id`, `revision_available_at`, `captured_at`, `interval_sec`, 全部挂牌变化与估算流量列，支持迟到帧重算邻桶并保留旧版本 |
| **`orderbook_observations`** | 主键 `(snapshot_id, item_id)`；48h 清理前保存 `available_at`, `book_state`, `purchase_mode`, `coverage_hours`, `wall_price`, `wall_qty`, `wall_persist_frames`, `ladder_json`（逐条挂单单组明细，区分 `60+39` 与 `50+49`） |
| **`review_windows`** | 预登记人工窗口主表（报告分母）：`window_id PRIMARY KEY`, `pair_block_id`, `planned_start_at`, `planned_end_at`, `planned_review_min`, `item_pool_version`, `rule_version`, `assigned_arm`, `window_status`（`'COMPLETED' \| 'ZERO_OPPORTUNITY' \| 'MISSED_BY_USER' \| 'ABORTED'`）, `actual_start_at`, `actual_end_at`, `actual_review_min`, `miss_or_abort_reason` |
| **`opportunity_observations`** | 外键 `window_id` 关联 `review_windows`：`obs_id`, `opportunity_group_id`, `arm`（`'SIMPLE_LIMIT' \| 'B_SHOCK' \| 'AUDIT_SAMPLE' \| 'NO_ALERT_CONTROL'`）, `rule_version`, `fee_rule_version`, `source_snapshot_id`, **`read_publish_seq`**, `item_id`, `collect_start_at`, `available_at`, `quote_expires_at`, `rank_order`, `planned_qty`, `max_total_buy_cost`, `alert_seen_at`, `client_checked_at`, `client_fillable_qty`, `client_total_cost`, `outcome_reason` |
| **`user_positions`** | 批次投影表：`position_id`, **`arm_id`**, `item_id`, `is_legacy_inventory`, `opened_at`, `initial_qty`, `remaining_qty`, `total_cost`（整数金币总成本，旧仓可 `NULL`）, `unknown_cost_reason`, `settlement_due_at`, `status`（`'OPEN' \| 'CLOSED'`） |
| **`user_orders`** | 挂单投影表：`order_id`, `parent_order_id`, **`arm_id`**, `position_id`, `item_id`, `exit_arm`, `submitted_at`, `public_visible_at`, `slot_closed_at`, `listed_price`, `initial_qty`, `sold_qty`, `remaining_qty`, `first_fill_at`, `listing_fee_paid`, `edit_fee_paid`, `edit_count`, `expires_at`, `status` |
| **`user_ledger_events`** [终审 P1-5 / P2-3] | 唯一真相源：`event_id`, `idempotency_key TEXT NOT NULL UNIQUE`, **`arm_id TEXT NOT NULL`**, `event_ts`, `event_order INTEGER NOT NULL DEFAULT 0`（全序键 `(event_ts, event_order, event_id)`）, `event_type`, `item_id`, `position_id`, `order_id`, `parent_order_id`, **`settlement_due_at`**, **`batch_claim_id`**, `new_listed_price`, `new_expires_at`, `new_public_visible_at`, **`total_amount INTEGER`**（整批整数总金额）, `unit_price`（可选辅助）, `qty_delta`, `available_cash_delta`, `unclaimed_delta`, `fee_paid`, `slot_delta`, `fills_detail_json`（多价成交分档明细）, `voucher_note`, `unknown_reason` |

### 4.3 分组可重放事件账本的状态机与事务边界 [终审 P1-5 / P2-3]

1. **按 `arm_id` 独立建账与事件必填字段矩阵**：
   - `ACCOUNT_OPEN`（**按每个 `arm_id` 分别建账**，必填 `arm_id`, `event_ts`, `available_cash_delta >= 0`, `unclaimed_delta >= 0`，并记录该组初始槽位上限与损失预算 $L_{\max}$）；
   - `LEGACY_POSITION_INIT` / `LEGACY_ORDER_INIT`（必填 `arm_id`, `item_id`, `position_id`, `qty_delta > 0`, 可选 `settlement_due_at`；旧仓 `total_amount` / `opened_at` 未知可 `NULL + unknown_reason`；旧挂单必填 `order_id`, `new_listed_price > 0`, `slot_delta = +1`）；
   - **`BUY`（支持多价成交归入同一批次）[终审 P2-3]**：必填 `arm_id`, `item_id`, `position_id`, `qty_delta > 0`, **`total_amount > 0`**, `available_cash_delta = -total_amount`, `settlement_due_at`（可选 `unit_price` 与 `fills_detail_json`）。多价扫货（如 `1×9,000 + 98×10,000 = 989,000`）既可按同 `position_id` 写多条 `BUY` 分档事件，也可写单条 `BUY(qty_delta=99, total_amount=989000)`，`user_positions` 精确累加整数 `total_cost = 989000` 不截断均值；
   - `LIST`（必填 `arm_id`, `item_id`, `position_id`, `order_id`, `qty_delta > 0`, `new_listed_price > 0`, `fee_paid >= 0`, `available_cash_delta = -fee_paid`, `slot_delta = +1`, `new_expires_at`；重挂必填 `parent_order_id`）；
   - `ORDER_PUBLIC`（必填 `arm_id`, `order_id`, `new_public_visible_at`）；
   - `DIRECT_EDIT`（必填 `arm_id`, `order_id`, `new_listed_price > 0`, `fee_paid >= 0`, `available_cash_delta = -fee_paid`, `slot_delta = 0`，可选 `new_expires_at`）；
   - `SALE_CONFIRMED`（必填 `arm_id`, `order_id`, `position_id`, `qty_delta < 0`, `total_amount > 0`, `unclaimed_delta = +total_amount`；剩余归 0 转 `SOLD_OUT` 且 `slot_delta = -1`）；
   - **`CLAIM_CASH` 与跨组合领分摊 [终审 P1-5]**：必填 `arm_id`, `unclaimed_delta < 0`, `available_cash_delta = -unclaimed_delta`。若客户端一次合领 A、B 多组待领取款，必须在**同一 `BEGIN IMMEDIATE` 事务**内凭同一 **`batch_claim_id`** 为每个涉及 `arm_id` 各写一条 `CLAIM_CASH` 分摊事件，强校验每组分摊额 $\le$ 该组当前 `unclaimed_proceeds` 且各组分摊之和精确等于客户端实领总金币；
   - `CANCEL` / `EXPIRE`（必填 `arm_id`, `order_id`, `slot_delta = -1`）；撤单重挂在同一事务按序写入 `CANCEL(old_order_id)` + `LIST(new_order_id, parent_order_id=old_order_id)`；
   - `EXTERNAL_TRANSFER`（必填 `arm_id`, `available_cash_delta != 0`）。
2. **事务边界与分组重放**：单条或批量事件导入在单一 `BEGIN IMMEDIATE` 事务内执行幂等校验（同 `idempotency_key` 同载荷跳过、异载荷整批报错回滚）→ 写事件流 → 校验组内现金/待领取款/库存/状态转移合法性 → 更新投影表。`replay_ledger(events, as_of_ts, arm_id=None)` 可独立还原**任意 `arm_id` 及全局汇总**在任意结算日的可复投现金、待领取款、批次总成本、挂单挂价、槽位与剩余损失额度。

---

## 五、四阶段目标试验、可复算门槛与统一权益损失风控

### 5.1 阶段一：核对市场机制，建立可复核记录
- 完成客户端费用、`Pending`、PC 直接改价及购买单位核验；以独立受控表格/事件表记录期初状态、观察与本人流水。
- **验收**：费用计算与客户端预览及实扣 0 误差对齐；收费未明标的禁入新买；用 `1×9,000 + 98×10,000 = 989,000` 验证整批买入扣款、保本价与重放余额 0 误差一致；`replay_ledger` 从期初完整重建含“两次同费不同价改价、撤单重挂关联、一次跨组合领分摊、重复回执、导入中断回滚”的测试集，与分组及全局四项余额 100% 一致。

### 5.2 阶段二：用同样的人工窗口筛查入场与可复算门槛 [终审 P2-2]
- 冻结**简单限价规则**（按拟买总价低于上限比例排序）与 **剧本 B**（优先冲击候选，覆盖不足回退简单排序）。
- 每个人工窗口开始前写入 `review_windows` 冻结配对区组与随机指定的 `assigned_arm`，两组获得等长复查分钟数，未复查组保留排序，并抽查少量低排名（`AUDIT_SAMPLE`）与无告警时点（`NO_ALERT_CONTROL`）。
- **可复算入围门槛（试验前冻结于 `config/trial_protocol.yaml`）**：
  - 报告以 `review_windows` 为分母 `LEFT JOIN opportunity_observations`；
  - 对每个已完成配对区组 $k \in \{1..K\}$，主指标 $Y_{k,B}$ 与 $Y_{k,S}$ **严格只统计预分配给该窗口策略（`arm == review_windows.assigned_arm`）且经客户端确认足量、总价达标的独立机会数（按 `opportunity_group_id` 去重）**；**`AUDIT_SAMPLE` 与 `NO_ALERT_CONTROL` 抽查结果单独汇总报告，绝不计入 $Y_{k,B}$ 或 $Y_{k,S}$**；
  - 定义配对净增益 $d_k = Y_{k,B} - Y_{k,S}$、平均增益 $\bar{d} = \frac{1}{K}\sum_{k=1}^K d_k$；
  - **B 进入阶段四小额试单的冻结条件**：完成预登记最少区组数 $K \ge K_{\min}$ 后，同时满足（1）$\bar{d} \ge \Delta_{\min}$（预冻结最小实用每窗增量）、（2）预登记配对区间下界（如配对 Bootstrap 90% 下界）$> 0$、（3）B 组客户端复查达标率与过期/错过率不劣于预登记红线；不达标则保留简单规则或继续限额观察。各策略前置证据：A 须跨独立活动窗口；C 须核实可落地兑换/生产/退出路径；D 须待机会寿命实测覆盖全链路延迟。

### 5.3 阶段三：用真实订单验证退出
- 先用可比现有库存对照**固定挂价**与**预定时点复查改价**（已上架旧仓按已持续时长匹配，严禁同商品自家挂单同时段争抢同一买方需求；跨时段占用配置独立额度或完整持有窗口），再以新买批次验证完整净结果。
- 动作比较只计未来净回收额（`回款 + 尾仓净值 − 尚未计入两者的应付费用`）；**仅当提前腾出槽位可服务可信下一笔机会时才计入机会价值**。从 `public_visible_at` 起报告首件售出耗时、截止日前售出件数占比与整单售罄比例。

### 5.4 阶段四：小额完整试单与统一权益损失预算风控 [P1-3]

**修正开仓损失预算检查（杜绝累计亏损突破上限）**：
- 设本轮试验（或独立分组）期初同口径净权益为 $E_0$，期间净外部转入为 $\Delta E_{\text{ext}}$，调整后基准权益 $E_{\text{base}} = E_0 + \Delta E_{\text{ext}}$，预先冻结的总最大可承受损失上限为 $L_{\max}$。
- **任意新单开仓前**，按与结算日完全一致的保守情景，计算**纳入该笔新单后的保守期末净权益下界**：
  $$E_{\text{cons,post}} = (\text{当前可用金币} - \text{新单买入总价} - \text{当下应付费用}) + \text{已确认待领取款} + \text{全部在途仓位保守净估值} + \text{新单保守净估值} - \text{尚待支付且未计入估值的费用}$$
- 强制硬门禁（以期初基准与剩余损失额度统一校验）：
  $$\text{ProjectedTotalLoss} = \max\big(0,\; E_{\text{base}} - E_{\text{cons,post}}\big) \le L_{\max}$$
  （等价于：**已实现累计亏损 + 在途仓位保守亏损 + 新单保守最大亏损 $\le L_{\max}$**，即 `新单保守最大亏损 <= 剩余损失额度 L_rem`）。
  *反例校验*：预算 $L_{\max} = 100{,}000$、已实现亏损 $80{,}000$（$L_{\text{rem}} = 20{,}000$）、当前无仓时，一笔保守可能再亏 $50{,}000$ 的新单算出 $\text{ProjectedTotalLoss} = 130{,}000 > 100{,}000$，**直接硬拒绝**，不再放行。
- **分组、熔断与扩量**：B 与简单规则共用同一退出规则并分配独立金币/槽位额度（无法隔离则用完整持有窗口）；严格按预登记分组统计并保留漏单、拒绝与未售尾仓。触及累计损失上限（当前保守累计亏损 $\ge L_{\max}$）、关键机制/费率变更、数据过期或账本失配时立即熔断停开新仓。仅在预先保留、未参与调参的独立时段内，账本对齐且在合理尾仓损失与费用压力情景下，简单规则优于持币、B 同时优于简单规则与持币时，方可在实测验证容量内扩量。

---

## 六、统一结算日核算、工程验收与后续投入条件

### 6.1 统一结算日账户核算与分块披露口径

1. **策略净权益增量**：
   $$\text{期末净权益}_{T_{\text{settle}}} = \text{可用金币（available\_cash）} + \text{已确认待领取款（unclaimed\_proceeds）} + \text{尾仓净估值}$$
   $$\Delta \text{NetEquity} = \text{期末净权益}_{T_{\text{settle}}} - (\text{期初同口径净权益}_{T_0} + \text{期间净外部转入})$$
   该口径与 §5.4 开仓损失预算检查完全同源，避免实盘风控与期末核算出现两套账。
2. **尾仓情景与分项披露**：尾仓按统一的保守、中性假设计价并扣除尚需承担的处置费；缺乏可靠处置价时单列更低回收额的压力情景。分别展示实收现金、已付费用、延迟待领取金币及预计回收区间；按日期或独立活动分块汇总，报告样本量、窗口差异、最大资金占用、最大损失与有效窗口数。资金日、槽位日（从 `submitted_at` 至 `slot_closed_at`）与人工分钟用于瓶颈定位。

### 6.2 工程实施六大模块与全量硬验收清单

| 模块优先级 | 工程改动与强制验收用例 |
| :--- | :--- |
| **1. 双时态发布序、邻桶差分与混合空盘桶** | 引入 `publish_events`（单调 `publish_seq`）、`item_bar_revisions` 与 `inferred_flow_revisions`；按 `(server_started_at, snapshot_id)` 定前驱/后继并在迟到帧到达时级联重算后继差分及相邻桶；仅对 `api_covered = 1` 标的写空盘，分离历史 `close_price` 与当前有效报价；仿真重叠需 `--reset` 隔离。<br>**必测**：(1) `12:14→12:17` 使 `12:15` K 线在 `12:16` 不可见、`12:17` 可见；(2) 同桶首帧 `12:01`(`12:02`) + 迟到帧 `12:14`(`12:17`)，`12:10` 不可见、`12:16` 读首帧不变；(3) 同秒发布两版本不撞主键；(4) `12:10`(10件)+`12:20`(4件) 后补 `12:14`(7件)，补前 `as_of` 读减量 `6` 不变、补后分为 `3` 与 `3`；(5) `pokemon_category` 收藏/持仓不写空盘，混合桶总览与 RP 指数不读空盘前旧 `close_price`。 |
| **2. 留痕、防 `NULL` 留存校验与窗口表** | 48h 清理前保存逐单组 `ladder_json`、`source_snapshot_id`、`read_publish_seq` 与规则版本；清理与 `raw_retained = 0` 同事务；重放前直查 `snapshot_runs` 校验 `COUNT(*) > 0 AND SUM(CASE WHEN raw_retained = 1 THEN 0 ELSE 1 END) = 0`；建立 `review_windows` 分母表与可复算配对门槛报告。<br>**必测**：(1) 同桶 `[0, 1, 1]` 与 **`[NULL, 1, 1]`** 重放均拒绝覆写；(2) 整组买 `60+39` 与 `50+49` 买 39 件区分；(3) 双零窗口入分母且低排名/无告警抽查不混入主指标。 |
| **3. 迁移兼容、手工双写与整批费用** | 增量迁移 `002_v2_ledger_and_observations.sql` 保留既有数据；消除 `SELECT *` 同步升级 `schemas.py` 与前端；`manual_import.py` 双写 `item_bar_revisions`（`revision_available_at = MAX(recorded_at, ingested_at)`）；`fees.py` 实现离散生效时间费率与 `breakeven_sell_price_for_cost(total_buy_cost, qty)`。<br>**必测**：(1) **含历史数据的 `001` 库升级 `002`** 后，`/api/overview`、`/api/items`、`/api/items/{id}/bars`、`forecast`、`backtest` 全绿；(2) **升级后新录手工/CSV 报价**在 `as_of < available_at` 不可见、`as_of >= available_at` 实际进入下游特征与预测；(3) 99 件 `989,000` 总成本保本价验证及客户端预览 0 误差对齐。 |
| **4. 分组可重放事件账本与损失风控** | `user_ledger_events` 按 `arm_id` 独立建账并记录 `settlement_due_at`、`total_amount`、`new_listed_price`、`parent_order_id`、`batch_claim_id` 与全序键 `(event_ts, event_order, event_id)`；跨组合领单事务按组拆分并校验总和；开仓按每组期初调整基准净权益校验 $\text{ProjectedTotalLoss} \le L_{\max}$。<br>**必测**：(1) 从期初重建含两次同费不同价改价、重挂关联、**A/B 跨组合领分摊**、**99 件 `989,000` 多价批次**、同秒事件、重复回执与导入中断回滚；(2) 预算 10 万、已亏 8 万、无仓时拒绝保守再亏 5 万新单。 |
| **5. 冻结日历回测与上游特征修复** | 冻结 `calendar_folds` 固定折边界（废除尾部依赖 `np.array_split`）；本轮同步修复 `features.py:458` 逐时刻有效权重归一化与 RP 仅采信 `stocked` 时效过滤；严格分离训练截止、测试出候选与事后评分；缺退出报价留余货占资金，防纸单重复占流。<br>**必测**：(1) 复用同一冻结切分，删除 `> T` 报价、补入迟到帧或补入 `> T` 首次出现的原料报价，`T` 所属折边界、`upstream_ret_24h`（不从 `1.0` 变 `0.5`）、候选与预算 100% 不变；(2) 缺标签预测保留在决策记录中。 |
| **6. 策略专属前置** | 策略 C 上线前补全带 `effective_from` 的完整生产配方成本（缺项标 `NULL`）与实测退出路径；策略 A 按 `announced_at <= decision_ts` 计算活动信号。 |

**开工三步走**（仿真器保留作测试工具，重叠时间段需 `--reset` 隔离）：
1. **立即可先行**：客户端从现有库存挑少量熟悉道具，同步用独立表格开展费用、`Pending`/PC 改价核验与手工记账；
2. **并行编码与终验收**：按模块 1–5 拆分独立工作流并行编码并逐项跑通全部自动化单测与 001 历史库升级回归；官方 API 就绪后验收单次完整采集再开定时观察，开展阶段二入场对照与阶段三现有库存退出对照；
3. **小额闭环试单**：依据阶段二、三实测证据锁定阶段四规则与分组统一权益损失预算，到期后按结算日净权益增量与尾仓压力情景决策。

### 6.3 后续高阶功能的投入条件

- **成交概率与等待时间**：仅当账本证实挂价与等待为主要损失时，先按标的、批量与挂价差统计本人订单固定期限结果，样本充足后再分撤单/到期/售罄竞争事件估计分档成交率。
- **资金与槽位分配**：仅当金币、槽位或人工时间反复卡住正收益机会且简单分配长期造成可量化损失时，再建组合优化。
- **预测模型与扩展界面**：仅当冻结的样本外独立对照证明模型能比简单规则多赚净金币时，才接入排序；三重障碍、Meta-Labeling、凯利仓位与高频做市均留待后续评估。

---

## 七、终审 9 项阻塞与补齐闭环核对表

| 编号与等级 | 终审指出问题 | 本终稿闭环方案与章节 |
| :--- | :--- | :--- |
| **P1-1** | `MIN(raw_retained) = 1` 在 SQLite 会忽略 `NULL` 放过 `[NULL, 1, 1]` 旧帧 | 改为 `COUNT(*) > 0 AND SUM(CASE WHEN raw_retained = 1 THEN 0 ELSE 1 END) = 0` 并补测 `[NULL, 1, 1]`（§3.3、§6.2） |
| **P1-2** | 整秒时间戳下同秒发布两版撞主键；重复仿真覆盖同时间段缺少隔离 | 新增 `publish_events(publish_seq)` 作为主键维度并在决策记录固定 `read_publish_seq`；`simulate` 禁止重叠混写须 `--reset`（§3.1、§4.2、§6.2） |
| **P1-3** | 迟到帧按 `snapshot_id` 找前驱致间隔为负，且相邻桶差分未拆分为 3+3 | 按 `(server_started_at, snapshot_id)` 定位前驱/后继，新增 `inferred_flow_revisions`，迟到帧同 `publish_seq` 重算后继差分与相邻桶并保留旧版本（§3.1、§4.2、§6.2） |
| **P1-4** | `np.array_split` 依赖全量尾部，删未来行情导致所属折起点从第 14 格移到第 17 格 | 恢复冻结决策日历 `calendar_folds` 固定各折边界，扰动测试复用同一份切分（§2.3、§6.2） |
| **P1-5** | 事件流缺分组字段、结算日来源及跨组合领分摊，无法重放各组可复投现金与剩余额度 | `user_ledger_events` 增加必填 `arm_id`、`settlement_due_at` 与 `batch_claim_id`，按组 `ACCOUNT_OPEN`，合领单事务分摊校验总和并验收跨组合领（§4.2–§4.3、§6.2） |
| **P1-6** | 因果查询读 `item_bar_revisions`，而 `manual_import.py` 只写 `item_bars` 漏掉新录报价 | `manual_quotes` 增加 `ingested_at`，`manual_import.py` 同事务双写 `item_bar_revisions` 并验收从导入到下游特征/预测全链路（§4.1、§6.2） |
| **P2-1** | 精灵素材分类未覆盖却可能写空盘；混合桶旧 `close_price` 仍被总览和 RP 特征读取 | `items_catalog.api_covered` 过滤空盘扩展；`web/app.py:276` 与 `features.py` 仅对 `book_state = 'stocked'` 读取当前有效报价（§3.2、§4.1、§6.2） |
| **P2-2** | 剧本 B 试单门槛缺少可复算规则与抽查隔离口径 | 冻结 `trial_protocol.yaml` 中的 $K_{\min}, \Delta_{\min}$ 与配对差值下界；主指标只计预分配策略确认机会，抽查单列统计（§5.2、§6.2） |
| **P2-3** | 99 件共 989,000 金币无法用整数单价表达，需贯通保本计算与账本多价批次 | 新增 `breakeven_sell_price_for_cost(total_buy_cost, qty)`；`BUY` 用整数 `total_amount` 支持多价归入同一批次并用 989,000 端到端验收（§2.1–§2.2、§4.3、§5.1、§6.2） |
