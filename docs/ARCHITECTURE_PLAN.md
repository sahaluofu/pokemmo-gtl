# PokeMMO GTL 市场研究与价格预测系统 — 架构总规范（v2.0 多代理联审定稿）

> **工作区**：`<repo-root>`  
> **角色分工**：架构师与审阅者负责规范制定、逐里程碑代码审阅与结果验收；执行者严格按本规范与 `docs/CODEX_TASKS.md` 逐里程碑实现并自测。  
> **合规红线**：纯外部只读市场研究工具。仅对接官方只读 Auctions API（`https://api.pokemmo.com/api/auctions/items`）、开源静态词典（`PokeMMO-Tools/pokemmo-data`）、用户手工/CSV 导入与本地仿真器；**严禁编写任何触碰游戏客户端、抓包、自动化点击或爬取第三方站点（含 `tool.lzpoke.com`）的代码**。

---

## 1. 本地运行环境、磁盘保护与编码硬约束

1. **推荐工具链**：
   - Python 3.12（`>=3.12`，建议使用项目本地虚拟环境 `.venv`）
2. **保护系统盘与规避特殊字符路径陷阱**：
   - 兼容含空格、单引号或非 ASCII 字符的本地路径。
   - `pyproject.toml` 必须配置 `[tool.pytest.ini_options] basetemp = ".pytest_tmp"` 与 `[tool.ruff] cache-dir = ".ruff_cache"`，确保测试临时文件与缓存 100% 留在仓库目录内。
   - **严禁依赖 `.\.venv\Scripts\Activate.ps1`**（PowerShell 5.1 对无 BOM UTF-8 脚本在中文路径下会产生 GBK 乱码且子进程不继承激活态）。所有命令一律显式调用 `.\.venv\Scripts\python.exe -m ...`。
   - 所有文件 I/O 必须使用 `pathlib.Path` 并显式声明 `encoding="utf-8"`（CSV 导入使用 `encoding="utf-8-sig"` 兼容 Excel BOM）。
3. **物理环境隔离**：
   - `--env live`（默认）操作 `data/gtl_live.db`；
   - `--env sim` 操作 `data/gtl_sim.db`；
   - **硬性禁令**：当 `env == "live"` 时，CLI `gtl simulate` 与 Web `/api/admin/trigger` 的 `simulate` 动作必须直接拒绝执行（CLI 退出码非 0，Web 返回 `403 Forbidden`）。
4. **密钥安全**：
   - `POKEMMO_API_TOKEN` 仅从 `.env` 或环境变量读取；任何日志、异常消息、HTTP 响应严禁输出 Token 原文（异常格式化前必须脱敏请求头）。

---

## 2. 官方 PokeMMO Auctions API 与跨进程限流契约

### 2.1 HTTP 采集契约（`ingestion/official_api.py`）
- **Endpoint**：`GET https://api.pokemmo.com/api/auctions/items`
- **必填请求头**：
  - `Private-Token: <64-char hex token>`
  - `Accept: application/json`
- **连接与错误处理**：
  - `httpx.Client(follow_redirects=False, timeout=httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0))`
  - 收到任何 **`3xx`（`300..399`）**、`401`、`403`：立即抛出 `ApiAuthError`，绝不跟随重定向、绝不重试。
  - 收到 `429`、`5xx` 或 `httpx.TransportError` / `TimeoutException`：按带抖动的指数退避重试（最多 3 次，若响应含 `Retry-After` 头则优先遵守）。
  - 校验响应体必须为 JSON `list`，每项包含正整数 `auction_id`, `reference_id`, `price`, `quantity`, `expires_at`（若 `expires_at > 10^11` 则整除 `1000` 归一化为 UTC 秒）。
- **游标分页与去重**：
  - 每次请求显式携带 `count=1000`；首页不传 `skip`。
  - 终止条件：**仅当返回空数组 `[]` 或 `min_auction_id <= 1` 时停止**（绝不以 `len(page) < count` 提前终止，也绝不发送 `skip <= 0`）。
  - 严格断言游标单调递减 `next_skip < prev_skip` 且硬上限 `max_pages = 100`，防止服务端异常导致死循环。
  - 在内存中按 `auction_id` 去重（保留首次出现的最新记录）。
- **服务端时钟提取**：
  - 解析首页响应头 `Date`（`email.utils.parsedate_to_datetime`）获取服务端 UTC 秒 `server_started_at`（缺失时回退本机 `int(time.time())`），消除本机时钟漂移对流拍判定的影响。

### 2.2 跨进程持久化令牌桶与单飞采集锁（`ingestion/rate_limiter.py`）
- 官方限流为滚动 300s / 30,000 tokens（消耗量 = 请求参数 `count`）。
- 使用独立共享 SQLite 文件 `data/api_limiter.db`（亦支持注入内存后端 + `clock`/`sleeper` 供单元测试）：
  - `api_rate_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT, req_ts REAL NOT NULL, tokens INTEGER NOT NULL) STRICT`：每次请求前在 `BEGIN IMMEDIATE` 中清理 `req_ts < now - 310` 的旧记录，若窗口内已用 tokens + `count` > `28000`（305s 窗口），释放锁并睡眠至最早一笔释放，再重新获取锁扣减。
  - `collector_locks(lock_name TEXT PRIMARY KEY, owner_pid INTEGER NOT NULL, acquired_at INTEGER NOT NULL, heartbeat_at INTEGER NOT NULL) STRICT`：防止 `gtl collect --once`、`--daemon` 与 Web 手动触发同时运行（锁心跳超时 600s 自动失效，活跃冲突抛出 `CollectorBusyError`）。
- `--daemon --interval 900` 采用 **start-to-start** 调度：`sleep(max(0, interval - elapsed))`。

---

## 3. SQLite `STRICT` 数据库架构与两阶段并发模型

### 3.1 连接与事务规范（`db/connection.py`）
- 严禁跨线程共享单一 `sqlite3.Connection`。每次操作/每个请求通过短生命周期工厂打开连接（`isolation_level=None`），打开时立即执行：
  ```sql
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA foreign_keys = ON;
  PRAGMA busy_timeout = 30000;
  ```
- 所有写操作必须包裹在 `write_transaction(conn)` 上下文管理器中，显式发出 `BEGIN IMMEDIATE` ... `COMMIT` / `ROLLBACK`，杜绝 `DEFERRED` 事务读写锁升级死锁（`SQLITE_BUSY`）。
- **两阶段快照持久化（网络 I/O 零数据库写锁）**：
  1. **阶段 1（`<10ms` `BEGIN IMMEDIATE`）**：将超过 30 分钟仍处于 `running` 的陈旧记录置为 `failed` 并清理其残留 `raw_listings`；插入新 `snapshot_runs(status='running', started_at=...)`。
  2. **网络拉取阶段（无活跃 DB 事务）**：分页拉取全部挂单至 Python 内存列表并去重。
  3. **阶段 2（`<200ms` `BEGIN IMMEDIATE`）**：先将本批未知 `reference_id` 批量 `INSERT OR IGNORE INTO items_catalog`（`name_en='Unknown Item #'||id, category='unknown', max_stack_size=99`）→ 批量写入 `raw_listings` → 计算 `inferred_flows` → 执行两阶段 `item_bars` 聚合 → 更新 `snapshot_runs(status='completed', server_started_at=..., completed_at=...)`。若中途抛任何异常（含 `KeyboardInterrupt`），在独立短事务中置 `status='failed'`，确保库中零残缺 `raw_listings`。

### 3.2 完整建表契约（`db/migrations/001_init.sql`）
所有时间戳字段均为 **UTC Unix epoch 秒（`INTEGER`）**，全部采用 SQLite `STRICT` 表：

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS items_catalog (
    item_id        INTEGER PRIMARY KEY,         -- 官方 reference_id；精灵素材分类使用 >= 900000
    asset_class    TEXT NOT NULL DEFAULT 'item', -- 'item' | 'cosmetic' | 'berry' | 'pokemon_category'
    name_en        TEXT NOT NULL,
    name_zh        TEXT,
    category       TEXT NOT NULL DEFAULT 'other',
    max_stack_size INTEGER NOT NULL DEFAULT 99,  -- 可堆叠道具 99；精灵与不可堆叠时装/装备为 1
    description    TEXT,
    is_watched     INTEGER NOT NULL DEFAULT 0,
    updated_at     INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS snapshot_runs (
    snapshot_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    source            TEXT NOT NULL,            -- 'official_api' | 'simulator'
    started_at        INTEGER NOT NULL,
    server_started_at INTEGER NOT NULL,
    completed_at      INTEGER,
    status            TEXT NOT NULL,            -- 'running' | 'completed' | 'failed'
    total_listings    INTEGER NOT NULL DEFAULT 0,
    pages_fetched     INTEGER NOT NULL DEFAULT 0,
    error_message     TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS idx_snapshot_runs_status_ts ON snapshot_runs(status, completed_at DESC, snapshot_id DESC);

CREATE TABLE IF NOT EXISTS raw_listings (
    snapshot_id INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    auction_id  INTEGER NOT NULL,
    item_id     INTEGER NOT NULL REFERENCES items_catalog(item_id),
    price       INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, auction_id)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_raw_listings_snap_item_price ON raw_listings(snapshot_id, item_id, price ASC, auction_id ASC);

CREATE TABLE IF NOT EXISTS inferred_flows (
    snapshot_id         INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    prev_snapshot_id    INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    item_id             INTEGER NOT NULL REFERENCES items_catalog(item_id),
    captured_at         INTEGER NOT NULL,
    interval_sec        INTEGER NOT NULL,
    new_listing_count   INTEGER NOT NULL,
    new_listed_qty      INTEGER NOT NULL,
    partial_fill_qty    INTEGER NOT NULL,
    early_disappear_qty INTEGER NOT NULL,
    expired_qty         INTEGER NOT NULL,
    est_sold_qty        REAL NOT NULL,
    est_sold_vwap       REAL,
    gap_skipped         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (snapshot_id, item_id)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_inferred_flows_item_ts ON inferred_flows(item_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_inferred_flows_prev_snap ON inferred_flows(prev_snapshot_id);

CREATE TABLE IF NOT EXISTS item_bars (
    item_id             INTEGER NOT NULL REFERENCES items_catalog(item_id),
    timeframe           TEXT NOT NULL,          -- '15m' | '4h' | '1d'
    bucket_ts           INTEGER NOT NULL,       -- 桶起始 UTC 秒 (ts - ts % tf_sec)
    close_ts            INTEGER NOT NULL,       -- 桶结束 UTC 秒 (bucket_ts + tf_sec)，所有因果对齐以此为准
    data_source         TEXT NOT NULL DEFAULT 'snapshot', -- 'snapshot' | 'manual'
    open_price          REAL NOT NULL,
    high_price          REAL NOT NULL,
    low_price           REAL NOT NULL,
    close_price         REAL NOT NULL,
    robust_best_ask     INTEGER NOT NULL,
    best_ask_min        INTEGER NOT NULL,
    p10_price           REAL,
    total_listed_qty    INTEGER,
    listing_count       INTEGER,
    depth_within_5pct   INTEGER,
    spread_top5_pct     REAL,
    est_sold_qty_sum    REAL,
    expired_qty_sum     INTEGER,
    new_listed_qty_sum  INTEGER,
    sample_count        INTEGER NOT NULL DEFAULT 1,
    valid_flow_samples  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, timeframe, bucket_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS idx_item_bars_tf_ts ON item_bars(timeframe, close_ts DESC);

CREATE TABLE IF NOT EXISTS manual_quotes (
    quote_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id     INTEGER NOT NULL REFERENCES items_catalog(item_id),
    recorded_at INTEGER NOT NULL,
    price       INTEGER NOT NULL,
    listed_qty  INTEGER,
    note        TEXT,
    source_tag  TEXT NOT NULL DEFAULT 'manual'
) STRICT;
CREATE INDEX IF NOT EXISTS idx_manual_quotes_item_ts ON manual_quotes(item_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS market_events (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    announced_at INTEGER NOT NULL,              -- 事件公开时间 UTC 秒（仅 close_ts >= announced_at 时可见）
    start_ts     INTEGER NOT NULL,
    end_ts       INTEGER NOT NULL,
    impact_scope TEXT NOT NULL DEFAULT 'all',
    notes        TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS idx_market_events_window ON market_events(start_ts, end_ts);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at     INTEGER NOT NULL,
    item_id          INTEGER NOT NULL REFERENCES items_catalog(item_id),
    horizon_hours    INTEGER NOT NULL,          -- 24 | 72 | 168
    model_name       TEXT NOT NULL,
    current_price    REAL NOT NULL,
    ref_qty          INTEGER NOT NULL,
    prob_up          REAL NOT NULL,
    pred_q10         REAL NOT NULL,
    pred_q50         REAL NOT NULL,
    pred_q90         REAL NOT NULL,
    expected_net_roi REAL NOT NULL,
    signal           TEXT NOT NULL,             -- 'BUY_WATCH' | 'NEUTRAL' | 'SELL_CAUTION'
    regime_note      TEXT NOT NULL,
    top_drivers_json TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS idx_predictions_lookup ON predictions(item_id, horizon_hours, generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_rank ON predictions(horizon_hours, generated_at DESC, expected_net_roi DESC);

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at        INTEGER NOT NULL,
    item_scope        TEXT NOT NULL,
    horizon_hours     INTEGER NOT NULL,
    model_name        TEXT NOT NULL,
    config_json       TEXT NOT NULL,
    metrics_json      TEXT NOT NULL,
    equity_curve_json TEXT NOT NULL
) STRICT;
```

### 3.3 配置文件 YAML 最小键规范（`config/*.yaml`）
- `settings.yaml`: `poll_interval_sec: 900`, `rate_limit_tokens: 28000`, `rate_limit_window_sec: 305`, `retention_hours: 48`, `expiry_grace_sec: 180`, `max_diff_gap_sec: 5400`, `alpha_near: 0.75`, `alpha_mid: 0.20`, `alpha_far: 0.0`, `ref_notional_yen: 500000`, `simulation_default_end_ts: 1768435200`（`2026-01-15T00:00:00Z`）。
- `fees.yaml`: `rate: "0.05"`, `item_min_fee: 100`, `pokemon_min_fee: 1000`, `max_fee_per_slot: 25000`, `default_relist_rate: "0.15"`.
- `events.yaml`: `events: [{name, event_type, announced_at, start_ts, end_ts, impact_scope, notes}]`（固定日历节日 `announced_at` 可设为 `start_ts - 90*86400`；突发补丁 `announced_at = start_ts`）。
- `item_aliases_zh.yaml`: `items: [{item_id, name_zh, category, asset_class, max_stack_size, is_watched}]`。
- `item_relations.yaml`: `rp_vouchers: [{item_id, denomination}]`, `upstream_relations: [{target_item_id, upstream_item_id, weight}]`。

---

## 4. 核心算法与数学规范

### 4.1 前缀去钓鱼滤波与两阶段 K 线聚合（`pipeline/aggregator.py`）
1. **单帧盘口统计**（对 `total_qty > 0` 的某物品挂单按 `price ASC, auction_id ASC` 排序；`total_qty == 0` 跳过不生成快照点）：
   - `best_ask_min = listings[0].price`
   - 定义数量加权经验分位数 $Q_w(p)$：累积数量首次达到 $\lceil p \cdot \text{total\_qty} \rceil$ 所在挂单的 `price`；令 `p10_price = float(Q_w(0.10))`。
   - **低尾钓鱼单剔除**：当 `total_qty >= 3` 时，锚点 $P_{\text{anchor}} = Q_w(0.10)$（若 `total_qty >= 10`）否则 $Q_w(0.50)$；从头部依次剔除满足 `price < 0.50 * P_anchor` 的异常低价单，但最多剔除数量上限为 `max(1, floor(0.05 * total_qty) - 1)` 件（保留真实整体暴跌）。
   - 剔除后的首件价格记为 `robust_best_ask`；在剔除后簿上取前 $K = \min(\text{trimmed\_qty}, 20, \max(5, \lfloor 0.05 \times \text{trimmed\_qty} \rfloor))$ 件计算数量加权均价，记为 **`vwap_top5_qty`**。
   - `depth_within_5pct` = 剔除后簿中 `price <= robust_best_ask * 1.05` 的挂单总件数；
   - `spread_top5_pct = (vwap_top5_qty - robust_best_ask) / max(robust_best_ask, 1)`。
2. **两阶段聚合（`15m` → `4h` / `1d`）**：
   - 第 1 阶段：将快照按 `server_started_at` 归入 `15m`（`900s`）桶，桶内 `open/high/low/close` 取 `vwap_top5_qty` 的首/高/低/末，盘口存量列（`robust_best_ask`, `p10_price`, `total_listed_qty`, `listing_count`, `depth_within_5pct`, `spread_top5_pct`）取桶内**最后一帧**，`best_ask_min` 取 `MIN`，流量列汇总该桶内非断档（`gap_skipped == 0`）的 `inferred_flows`，`valid_flow_samples` 记录有效差分帧数。
   - 第 2 阶段：由已落库的 `15m` `'snapshot'` 桶向上聚合生成 `4h`（`14400s`）和 `1d`（`86400s`）桶：OHLC 按 `bucket_ts ASC` 取 `FIRST(open) / MAX(high) / MIN(low) / LAST(close)`，**所有盘口存量列严格取桶内最后一个 `15m` 桶的值（严禁 `SUM`）**，流量列与 `sample_count`、`valid_flow_samples` 取 `SUM`。
   - **手工报价物化保护**：`manual_import.py` 向 `item_bars`（`4h` 与 `1d`）upsert 时必须附带 `ON CONFLICT(item_id, timeframe, bucket_ts) DO UPDATE SET ... WHERE item_bars.data_source != 'snapshot'`，绝不覆盖 `'snapshot'` 数据。

### 4.2 价格条件快照差分与分块清理（`snapshot_diff.py` & `retention.py`）
- 仅对比相邻两条 `status = 'completed'` 快照（首帧无前驱不写 `inferred_flows`）。
- 间隔 `interval_sec = curr.server_started_at - prev.server_started_at`：若 `< 60` 或 `> 5400`，置 `gap_skipped = 1, est_sold_qty = 0.0, est_sold_vwap = NULL`。
- 消失挂单判定（使用 `curr.server_started_at + 180` 宽限期）：
  - `prev.expires_at <= curr.server_started_at + 180` → `EXPIRED`；否则 → `EARLY_DISAPPEAR`。
- **价格条件成交折算**（清盘参考价 $P_{\text{clear}} = \max(\text{prev\_vwap\_top5}, \text{curr\_robust\_best\_ask})$）：
  - 近盘 `prev.price <= 1.03 * P_clear`：先扣减同档压价重挂量 `eff_near = max(0.0, early_near_qty - 0.5 * new_near_below_prev_qty)`，乘 `alpha_near = 0.75`；
  - 中盘 `1.03 * P_clear < prev.price <= 1.10 * P_clear`：乘 `alpha_mid = 0.20`；
  - 远盘 `prev.price > 1.10 * P_clear`：乘 `alpha_far = 0.0`（视为卖家撤单）；
  - `est_sold_qty = partial_fill_qty + 0.75 * eff_near + 0.20 * early_mid_qty`；`est_sold_vwap` 为对应加权成交均价（`est_sold_qty == 0` 时为 `NULL`）。
- **分块热表清理（`retention.py`）**：接受 `now_ts: int | None = None`（默认取库内 `MAX(completed_at)`，兼容历史仿真锚点），强制保留最新至少 2 帧 `completed` 快照，对其余早于 `now_ts - 48*3600` 的 `snapshot_id` 按**每批 20 个 `snapshot_id`** 分批在 `BEGIN IMMEDIATE` 中删除 `raw_listings`，最后执行 `PRAGMA wal_checkpoint(PASSIVE)`。

### 4.3 多槽位 `Decimal` 税费与精确保本价（`quant/fees.py`）
- 全部金额与费率运算使用 `decimal.Decimal(str(val))`：
  - 按 `max_stack_size` 拆槽：`full_slots, rem = divmod(qty, max_stack_size)`；
  - 单槽挂单费 `slot_fee(slot_qty) = min(25000, max(min_fee, int(Decimal(sell_price * slot_qty) * rate)))`（向下取整）；
  - 总挂单费 `listing_fee = full_slots * slot_fee(max_stack_size) + (slot_fee(rem) if rem > 0 else 0)`；
  - 期望有效税费 `effective_fee = Decimal(listing_fee) * (Decimal("1") + relist_rate)`；
  - `net_profit = Decimal((sell_price - buy_price) * qty) - effective_fee`。
- `breakeven_sell_price`：初始化 `lo = buy_price`，`hi = max(buy_price + 2000, buy_price * 2)`，循环 `while net_profit(hi) < 0: hi *= 2`，随后在 `[lo, hi]` 上整数二分求最小整数使 `net_profit >= Decimal("0")`。

### 4.4 严格 `close_ts` 因果特征、校准模型与全局 Purged 回测（`quant/*`）
1. **规则网格与严格因果连接（`features.py`）**：
   - 将每个物品重索引到连续 `4h` UTC 网格（`close_ts` 步长 `14400s`），价格列最多前向填充 `limit = 6`（纯 `'manual'` 序列 `limit = 18`），输出陈旧度特征 `bars_since_last_obs`，缺失流量列保持 `NaN`（若 24h 内 `coverage = valid_flow_samples / max(sample_count, 1) >= 0.5` 则将流量和乘以 `1 / coverage` 修正断档，否则置 `NaN`）。
   - 所有跨表/跨物品合并（`rp_index`、上游物品 `upstream_ret_24h`、`1d` 手工桶）必须使用 `pandas.merge_asof(..., on="close_ts", direction="backward")`（严格 `right.close_ts <= left.close_ts`）。
   - `market_events` 仅筛选 `announced_at <= close_ts` 的事件计算 `is_in_event`, `days_to_next_event`, `days_since_event_end`（无已知未来事件时 `days_to_next_event = -1`）。
   - 明确有界公式（$\epsilon = 1$）：
     - `rp_index`：`item_relations.yaml` 中各 RP 点券 `close_price / denomination` 的中位数；
     - `flow_imbalance_24h = (est_sold_24h - new_listed_24h) / (est_sold_24h + new_listed_24h + 1)`；
     - `expiry_pressure_24h = expired_24h / (est_sold_24h + expired_24h + 1)`；
     - `depth_ratio_24h = depth_within_5pct / (rolling_mean_24h(depth_within_5pct) + 1)`。
2. **执行摩擦感知的训练标签**：
   - Taker 买入价 $P_{\text{entry}, t} = \text{close\_price}_t$；Maker 卖出价 $P_{\text{exit}, t+H} = \min(\text{close\_price}_{t+H}, \text{p10\_price}_{t+H})$（`p10_price` 为空时取 `close_price`）。
   - 参考仓位 $Q_{\text{ref}} = \min(\text{max\_stack\_size}, \max(1, K_t), \max(1, \lfloor 500000 / \max(P_{\text{entry}, t}, 1) \rfloor))$。
   - 回归目标（尺度无关对数收益）：$y_{\text{reg}} = \ln(P_{\text{exit}, t+H} / P_{\text{entry}, t})$；预测分位数还原价格 $\hat{P}_\alpha = \max(1, \text{round}(P_{\text{entry}, t} \cdot \exp(\hat{q}_\alpha^r)))$。
   - 分类目标（扣税净盈利）：$y_{\text{cls}} = \mathbb{I}[\text{net\_profit}(P_{\text{entry}, t}, P_{\text{exit}, t+H}, Q_{\text{ref}}, \dots) > 0]$。
   - 预期净收益率：以三均值 $\hat{P}_{\text{exp}} = \text{round}(0.25\hat{P}_{10} + 0.50\hat{P}_{50} + 0.25\hat{P}_{90})$ 代入 `net_profit / (P_entry * Q_ref)`；当 `expected_net_roi >= 0.02` 且 `prob_up >= 0.55` 输出 `'BUY_WATCH'`，`expected_net_roi < 0` 输出 `'SELL_CAUTION'`，其余 `'NEUTRAL'`。
3. **冷启动分级模型与概率/区间校准（`models/*`）**：
   - `Naive`：`pred_q50 = current_price`，`pred_q10/q90` 取历史对数收益 10%/90% 分位，`prob_up` 取训练集正例率，信号恒为 `'NEUTRAL'`（0 交易）。
   - 单品有效 `4h` K 线 `< 84` 根 → **Tier 0**（盘口卖压 + 经验波动率正态残差区间）；
   - `84 ~ 179` 根 → **Tier 1**（`Pipeline([SimpleImputer(strategy="median", keep_empty_features=True), StandardScaler(), Ridge(alpha=10.0)])` + 经验残差分位）；
   - `>= 180` 根 → **Tier 2**（多物品面板 `LightGBM`，禁止 `class_weight='balanced'`，设重叠唯一样本权重 $w_t = \Delta / H$、`max_depth=4, num_leaves=15, min_child_samples=30`；留出时序尾部经 Purge 隔离的 15% 验证窗做 **Platt/Sigmoid 概率校准** 与 **Split Conformal Quantile Regression (CQR)**：`np.sort` 后上下拓宽共形残差 $c_{80}$）。
4. **全局 `close_ts` Purged Walk-Forward 组合回测（`backtester.py`）**：
   - 折切分严格按全局 UTC `close_ts` 对所有物品同步切分，训练集满足 **`train.close_ts + horizon_sec <= T_test_start`**。
   - 组合账户模拟：初始资金 `10,000,000`，**同一 `item_id` 最多 1 笔在途持仓**（锁定至 $t+H$ 平仓），单笔上限当前净值 `10%` 且数量上限 $\min(Q_{\text{ref}}, \max(1, \lfloor 0.20 \times \text{est\_sold\_24h} \rfloor))$（快照模式下 `total_listed_qty < 3` 或 `est_sold_24h < 1` 跳过开仓）。

---

## 5. FastAPI 安全服务与 12 个标准 REST 接口契约（`web/*`）

### 5.1 服务安全与并发守则
- 默认绑定 `127.0.0.1:8000`（非回环地址需显式传 `--allow-lan` 才放行）。
- 不启用通配符 CORS；中间件校验 `Host` 头，且对所有 `POST`/`DELETE` 请求若携带 `Origin` 头则必须匹配本机源（防本地网页 CSRF / DNS Rebinding）。
- 耗时任务（`backtest`, `simulate`, `collect`, `sync-catalog`, `forecast`）一律通过 `asyncio.to_thread` 执行，并受全局非阻塞任务互斥锁保护（运行中重复触发返回 `409 Conflict`）。
- JSON 序列化前统一递归将 `NaN` / `±Inf` 清洗为 `null`。

### 5.2 统一错误响应信封
所有非 2xx 错误统一返回：`{"error": {"code": str, "message": str, "details": Any}}`（严禁暴露堆栈或环境变量）。

### 5.3 全部 12 个标准 REST 接口（M6 必须逐一实现并测试）
1. `GET /api/health` → `{env, db_path, last_snapshot_at, token_configured, rate_budget_remaining, active_job, item_count, bar_count}`
2. `GET /api/overview?horizon=72` → `{rp_index, next_event, top_bullish, top_thinning_depth, top_movers_24h, watchlist}`
3. `GET /api/items?q=&category=&watched_only=false&limit=100` → 物品列表（含现价、24h涨跌、最新信号）
4. `POST /api/items/{item_id}/watch`（Body: `{is_watched: bool}`）→ `{item_id, is_watched}`
5. `GET /api/items/{item_id}/bars?timeframe=4h&days=90` → `{item, bars, events}`（含 OHLC、`robust_best_ask`、`best_ask_min`、卖压、隐含成交）
6. `GET /api/items/{item_id}/forecast?horizons=24,72,168` → 多周期预测（概率、CQR Q10/Q50/Q90、扣税净 ROI、档位说明、Top-5 驱动因子）
7. `POST /api/fees/simulate`（Body: `{item_id?, asset_class?, max_stack_size?, buy_price, sell_price, qty, relist_rate?}`）→ `{slots_used, listing_fee, effective_fee, net_profit, net_roi, breakeven_sell_price}`
8. `POST /api/backtest/run`（Body: `{item_id?, category?, horizon_hours, models}`）→ `{run_id, comparison_table, equity_curves}`
9. `POST /api/quotes/manual`（Body: `{item_id, price, listed_qty?, recorded_at?, note?}`）→ 写入 `manual_quotes` 并安全物化至 `item_bars`
10. `POST /api/quotes/import-csv`（Multipart `file`：`utf-8-sig` 流式限制 `<= 5MB` / `<= 50,000` 行，校验正整数价格）→ `{imported_rows, skipped_snapshot_bars, errors}`
11. `GET /api/items/{item_id}/export.csv?timeframe=4h` → `text/csv; charset=utf-8`（自由文本首字符为 `=,+,-,@,\t,\r` 时自动加 `'` 前缀防 Excel 公式注入）
12. `GET /api/events`（查询事件列表）及配套管理路由：`POST /api/events`、`DELETE /api/events/{event_id}`、`POST /api/admin/trigger`（Body: `{action: "simulate"|"collect"|"sync_catalog"|"forecast"}`；**当 `env == "live"` 且 `action == "simulate"` 时必须返回 `403 Forbidden`**）。
