ALTER TABLE items_catalog ADD COLUMN api_covered INTEGER NOT NULL DEFAULT 1;
ALTER TABLE items_catalog ADD COLUMN purchase_mode TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE items_catalog ADD COLUMN fee_verified INTEGER NOT NULL DEFAULT 0;

UPDATE items_catalog
SET api_covered = 0
WHERE asset_class = 'pokemon_category' OR item_id >= 900000;

ALTER TABLE snapshot_runs ADD COLUMN last_page_server_at INTEGER;
ALTER TABLE snapshot_runs ADD COLUMN available_at INTEGER;
ALTER TABLE snapshot_runs ADD COLUMN publish_seq INTEGER;
ALTER TABLE snapshot_runs ADD COLUMN raw_retained INTEGER DEFAULT 1;
UPDATE snapshot_runs SET raw_retained = NULL;
ALTER TABLE snapshot_runs ADD COLUMN raw_pruned_at INTEGER;

CREATE TABLE item_bars_v2 (
    item_id             INTEGER NOT NULL REFERENCES items_catalog(item_id),
    timeframe           TEXT NOT NULL,
    bucket_ts           INTEGER NOT NULL,
    close_ts            INTEGER NOT NULL,
    data_source         TEXT NOT NULL DEFAULT 'snapshot',
    book_state          TEXT NOT NULL DEFAULT 'stocked',
    bar_available_at     INTEGER,
    latest_publish_seq  INTEGER,
    open_price          REAL,
    high_price          REAL,
    low_price           REAL,
    close_price         REAL,
    robust_best_ask     INTEGER,
    best_ask_min        INTEGER,
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

INSERT INTO item_bars_v2 (
    item_id,
    timeframe,
    bucket_ts,
    close_ts,
    data_source,
    book_state,
    bar_available_at,
    latest_publish_seq,
    open_price,
    high_price,
    low_price,
    close_price,
    robust_best_ask,
    best_ask_min,
    p10_price,
    total_listed_qty,
    listing_count,
    depth_within_5pct,
    spread_top5_pct,
    est_sold_qty_sum,
    expired_qty_sum,
    new_listed_qty_sum,
    sample_count,
    valid_flow_samples
)
SELECT
    item_id,
    timeframe,
    bucket_ts,
    close_ts,
    data_source,
    'stocked',
    NULL,
    NULL,
    open_price,
    high_price,
    low_price,
    close_price,
    robust_best_ask,
    best_ask_min,
    p10_price,
    total_listed_qty,
    listing_count,
    depth_within_5pct,
    spread_top5_pct,
    est_sold_qty_sum,
    expired_qty_sum,
    new_listed_qty_sum,
    sample_count,
    valid_flow_samples
FROM item_bars;

DROP TABLE item_bars;
ALTER TABLE item_bars_v2 RENAME TO item_bars;

CREATE INDEX idx_item_bars_tf_ts
ON item_bars(timeframe, close_ts DESC);

CREATE INDEX idx_item_bars_avail
ON item_bars(timeframe, bar_available_at DESC, close_ts DESC);

ALTER TABLE inferred_flows
ADD COLUMN observed_undercut_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inferred_flows ADD COLUMN latest_publish_seq INTEGER;

ALTER TABLE manual_quotes ADD COLUMN ingested_at INTEGER;
ALTER TABLE manual_quotes ADD COLUMN publish_seq INTEGER;

CREATE TABLE publish_events (
    publish_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    available_at INTEGER NOT NULL,
    source_type  TEXT NOT NULL,
    source_id    INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_publish_events_avail
ON publish_events(available_at ASC, publish_seq ASC);

CREATE TABLE item_bar_revisions (
    item_id               INTEGER NOT NULL REFERENCES items_catalog(item_id),
    timeframe             TEXT NOT NULL,
    bucket_ts             INTEGER NOT NULL,
    publish_seq           INTEGER NOT NULL REFERENCES publish_events(publish_seq),
    close_ts              INTEGER NOT NULL,
    revision_available_at INTEGER NOT NULL,
    data_source           TEXT NOT NULL DEFAULT 'snapshot',
    book_state            TEXT NOT NULL DEFAULT 'stocked',
    open_price            REAL,
    high_price            REAL,
    low_price             REAL,
    close_price           REAL,
    robust_best_ask       INTEGER,
    best_ask_min          INTEGER,
    p10_price             REAL,
    total_listed_qty      INTEGER,
    listing_count         INTEGER,
    depth_within_5pct     INTEGER,
    spread_top5_pct       REAL,
    est_sold_qty_sum      REAL,
    expired_qty_sum       INTEGER,
    new_listed_qty_sum    INTEGER,
    sample_count          INTEGER NOT NULL DEFAULT 1,
    valid_flow_samples    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, timeframe, bucket_ts, publish_seq)
) STRICT;

CREATE INDEX idx_bar_rev_lookup
ON item_bar_revisions(timeframe, close_ts, revision_available_at, publish_seq);

CREATE INDEX idx_bar_rev_item_lookup
ON item_bar_revisions(item_id, timeframe, close_ts, revision_available_at, publish_seq);

CREATE TABLE inferred_flow_revisions (
    snapshot_id             INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    item_id                 INTEGER NOT NULL REFERENCES items_catalog(item_id),
    publish_seq             INTEGER NOT NULL REFERENCES publish_events(publish_seq),
    prev_snapshot_id        INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    revision_available_at   INTEGER NOT NULL,
    captured_at             INTEGER NOT NULL,
    interval_sec            INTEGER NOT NULL,
    new_listing_count       INTEGER NOT NULL,
    new_listed_qty          INTEGER NOT NULL,
    partial_fill_qty        INTEGER NOT NULL,
    early_disappear_qty     INTEGER NOT NULL,
    expired_qty             INTEGER NOT NULL,
    est_sold_qty            REAL NOT NULL,
    est_sold_vwap           REAL,
    observed_undercut_count INTEGER NOT NULL DEFAULT 0,
    gap_skipped             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (snapshot_id, item_id, publish_seq)
) STRICT;

CREATE INDEX idx_flow_rev_item_ts
ON inferred_flow_revisions(item_id, captured_at, revision_available_at, publish_seq);

CREATE TABLE orderbook_observations (
    snapshot_id         INTEGER NOT NULL REFERENCES snapshot_runs(snapshot_id),
    item_id             INTEGER NOT NULL REFERENCES items_catalog(item_id),
    available_at        INTEGER NOT NULL,
    publish_seq         INTEGER REFERENCES publish_events(publish_seq),
    book_state          TEXT NOT NULL,
    purchase_mode       TEXT NOT NULL DEFAULT 'unknown',
    coverage_hours      REAL,
    same_id_qty_drop     INTEGER NOT NULL DEFAULT 0,
    full_disappear_qty  INTEGER NOT NULL DEFAULT 0,
    visible_lower_qty   INTEGER,
    wall_price          INTEGER,
    wall_qty            INTEGER,
    wall_persist_frames INTEGER NOT NULL DEFAULT 0,
    ladder_json         TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, item_id)
) STRICT;

CREATE TABLE review_windows (
    window_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_block_id        TEXT NOT NULL,
    planned_start_at     INTEGER NOT NULL,
    planned_end_at       INTEGER NOT NULL,
    planned_review_min   REAL NOT NULL,
    item_pool_version    TEXT NOT NULL,
    rule_version         TEXT NOT NULL,
    assigned_arm         TEXT NOT NULL,
    window_status        TEXT NOT NULL DEFAULT 'PLANNED',
    actual_start_at      INTEGER,
    actual_end_at        INTEGER,
    actual_review_min    REAL,
    miss_or_abort_reason TEXT
) STRICT;

CREATE INDEX idx_review_windows_block
ON review_windows(pair_block_id, window_id);

CREATE TABLE opportunity_observations (
    obs_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    window_id            INTEGER NOT NULL REFERENCES review_windows(window_id),
    opportunity_group_id TEXT NOT NULL,
    arm                  TEXT NOT NULL,
    rule_version         TEXT NOT NULL,
    fee_rule_version     TEXT NOT NULL,
    source_snapshot_id   INTEGER REFERENCES snapshot_runs(snapshot_id),
    read_publish_seq     INTEGER,
    item_id              INTEGER NOT NULL REFERENCES items_catalog(item_id),
    collect_start_at     INTEGER,
    available_at         INTEGER NOT NULL,
    quote_expires_at     INTEGER NOT NULL,
    rank_order           INTEGER NOT NULL,
    planned_qty          INTEGER NOT NULL,
    max_total_buy_cost   INTEGER NOT NULL,
    snapshot_ladder_json TEXT NOT NULL,
    alert_seen_at        INTEGER,
    client_checked_at    INTEGER,
    client_fillable_qty  INTEGER,
    client_total_cost    INTEGER,
    outcome_reason       TEXT NOT NULL DEFAULT 'PENDING_REVIEW'
) STRICT;

CREATE INDEX idx_opp_obs_window_arm
ON opportunity_observations(window_id, arm, rank_order);

CREATE TABLE user_positions (
    position_id         INTEGER PRIMARY KEY,
    arm_id              TEXT NOT NULL,
    item_id             INTEGER NOT NULL REFERENCES items_catalog(item_id),
    is_legacy_inventory INTEGER NOT NULL DEFAULT 0,
    opened_at           INTEGER,
    initial_qty         INTEGER NOT NULL,
    remaining_qty       INTEGER NOT NULL,
    unallocated_qty     INTEGER NOT NULL,
    total_cost          INTEGER,
    remaining_cost      INTEGER,
    unknown_cost_reason TEXT,
    settlement_due_at   INTEGER,
    status              TEXT NOT NULL
) STRICT;

CREATE INDEX idx_user_positions_arm_status
ON user_positions(arm_id, status, item_id);

CREATE TABLE user_orders (
    order_id          INTEGER PRIMARY KEY,
    parent_order_id   INTEGER,
    arm_id            TEXT NOT NULL,
    position_id       INTEGER NOT NULL REFERENCES user_positions(position_id),
    item_id           INTEGER NOT NULL REFERENCES items_catalog(item_id),
    exit_arm          TEXT,
    submitted_at      INTEGER,
    public_visible_at INTEGER,
    slot_closed_at    INTEGER,
    listed_price      INTEGER NOT NULL,
    initial_qty       INTEGER NOT NULL,
    sold_qty          INTEGER NOT NULL DEFAULT 0,
    remaining_qty     INTEGER NOT NULL,
    first_fill_at     INTEGER,
    listing_fee_paid  INTEGER NOT NULL DEFAULT 0,
    edit_fee_paid     INTEGER NOT NULL DEFAULT 0,
    edit_count        INTEGER NOT NULL DEFAULT 0,
    expires_at        INTEGER,
    status            TEXT NOT NULL
) STRICT;

CREATE INDEX idx_user_orders_arm_status
ON user_orders(arm_id, status, item_id);

CREATE TABLE user_ledger_events (
    event_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key       TEXT NOT NULL UNIQUE,
    payload_sha256        TEXT NOT NULL,
    arm_id                TEXT NOT NULL,
    event_ts              INTEGER,
    event_order           INTEGER NOT NULL DEFAULT 0,
    event_type            TEXT NOT NULL,
    item_id               INTEGER REFERENCES items_catalog(item_id),
    position_id           INTEGER,
    order_id              INTEGER,
    parent_order_id       INTEGER,
    exit_arm              TEXT,
    settlement_due_at     INTEGER,
    batch_claim_id        TEXT,
    slot_limit            INTEGER,
    max_loss_budget       INTEGER,
    new_listed_price      INTEGER,
    new_expires_at        INTEGER,
    new_public_visible_at INTEGER,
    total_amount          INTEGER,
    unit_price            INTEGER,
    qty_delta             INTEGER NOT NULL DEFAULT 0,
    available_cash_delta  INTEGER NOT NULL DEFAULT 0,
    unclaimed_delta       INTEGER NOT NULL DEFAULT 0,
    fee_paid              INTEGER NOT NULL DEFAULT 0,
    slot_delta            INTEGER NOT NULL DEFAULT 0,
    fills_detail_json     TEXT,
    voucher_note          TEXT,
    unknown_reason        TEXT
) STRICT;

CREATE INDEX idx_ledger_events_arm_order
ON user_ledger_events(arm_id, event_ts ASC, event_order ASC, event_id ASC);
