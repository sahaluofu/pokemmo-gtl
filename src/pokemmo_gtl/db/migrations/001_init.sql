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
