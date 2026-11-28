"""Migration 002 contracts for populated v1 databases and strict v2 tables."""

import shutil
import sqlite3

import pytest

from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import MIGRATIONS_DIR, migrate

BASE_TS = 1_768_435_200
MIGRATION_VERSION = "002_v2_ledger_and_observations"

LEGACY_TABLE_ORDER = {
    "items_catalog": "item_id",
    "snapshot_runs": "snapshot_id",
    "raw_listings": "snapshot_id, auction_id",
    "item_bars": "item_id, timeframe, bucket_ts",
    "inferred_flows": "snapshot_id, item_id",
    "manual_quotes": "quote_id",
}

PRICE_COLUMNS = (
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "robust_best_ask",
    "best_ask_min",
    "p10_price",
    "spread_top5_pct",
)

NEW_TABLE_ROWS = {
    "publish_events": {
        "publish_seq": 101,
        "available_at": BASE_TS + 1000,
        "source_type": "snapshot",
        "source_id": 2,
    },
    "item_bar_revisions": {
        "item_id": 138,
        "timeframe": "15m",
        "bucket_ts": BASE_TS,
        "publish_seq": 1,
        "close_ts": BASE_TS + 900,
        "revision_available_at": BASE_TS + 1000,
        "close_price": 10_000.0,
    },
    "inferred_flow_revisions": {
        "snapshot_id": 2,
        "item_id": 138,
        "publish_seq": 1,
        "prev_snapshot_id": 1,
        "revision_available_at": BASE_TS + 1000,
        "captured_at": BASE_TS + 600,
        "interval_sec": 540,
        "new_listing_count": 1,
        "new_listed_qty": 2,
        "partial_fill_qty": 1,
        "early_disappear_qty": 5,
        "expired_qty": 0,
        "est_sold_qty": 6.0,
    },
    "orderbook_observations": {
        "snapshot_id": 2,
        "item_id": 138,
        "available_at": BASE_TS + 1000,
        "publish_seq": 1,
        "book_state": "stocked",
        "ladder_json": '[{"auction_id":101,"price":10000,"quantity":4}]',
    },
    "review_windows": {
        "window_id": 101,
        "pair_block_id": "block-1",
        "planned_start_at": BASE_TS + 1000,
        "planned_end_at": BASE_TS + 1600,
        "planned_review_min": 10.0,
        "item_pool_version": "pool-v1",
        "rule_version": "rule-v1",
        "assigned_arm": "SIMPLE_LIMIT",
    },
    "opportunity_observations": {
        "obs_id": 101,
        "window_id": 1,
        "opportunity_group_id": "opportunity-1",
        "arm": "SIMPLE_LIMIT",
        "rule_version": "rule-v1",
        "fee_rule_version": "fees-v1",
        "source_snapshot_id": 2,
        "read_publish_seq": 1,
        "item_id": 138,
        "available_at": BASE_TS + 1000,
        "quote_expires_at": BASE_TS + 1300,
        "rank_order": 1,
        "planned_qty": 99,
        "max_total_buy_cost": 989_000,
        "snapshot_ladder_json": (
            '[{"auction_id":101,"price":9000,"quantity":1},'
            '{"auction_id":102,"price":10000,"quantity":99}]'
        ),
    },
    "user_positions": {
        "position_id": 101,
        "arm_id": "SIMPLE_LIMIT",
        "item_id": 138,
        "initial_qty": 99,
        "remaining_qty": 99,
        "unallocated_qty": 99,
        "total_cost": 989_000,
        "remaining_cost": 989_000,
        "status": "OPEN",
    },
    "user_orders": {
        "order_id": 101,
        "arm_id": "SIMPLE_LIMIT",
        "position_id": 1,
        "item_id": 138,
        "listed_price": 10_000,
        "initial_qty": 99,
        "remaining_qty": 99,
        "status": "PENDING",
    },
    "user_ledger_events": {
        "event_id": 101,
        "idempotency_key": "migration-002-account-open",
        "payload_sha256": "0" * 64,
        "arm_id": "SIMPLE_LIMIT",
        "event_ts": BASE_TS + 1000,
        "event_type": "ACCOUNT_OPEN",
        "available_cash_delta": 1_000_000,
        "slot_limit": 5,
        "max_loss_budget": 100_000,
    },
}

EXPECTED_PRIMARY_KEYS = {
    "item_bars": ("item_id", "timeframe", "bucket_ts"),
    "publish_events": ("publish_seq",),
    "item_bar_revisions": ("item_id", "timeframe", "bucket_ts", "publish_seq"),
    "inferred_flow_revisions": ("snapshot_id", "item_id", "publish_seq"),
    "orderbook_observations": ("snapshot_id", "item_id"),
    "review_windows": ("window_id",),
    "opportunity_observations": ("obs_id",),
    "user_positions": ("position_id",),
    "user_orders": ("order_id",),
    "user_ledger_events": ("event_id",),
}

EXPECTED_INDEX_COLUMNS = {
    "idx_item_bars_tf_ts": (("timeframe", 0), ("close_ts", 1)),
    "idx_item_bars_avail": (
        ("timeframe", 0), ("bar_available_at", 1), ("close_ts", 1),
    ),
    "idx_publish_events_avail": (("available_at", 0), ("publish_seq", 0)),
    "idx_bar_rev_lookup": (
        ("timeframe", 0), ("close_ts", 0),
        ("revision_available_at", 0), ("publish_seq", 0),
    ),
    "idx_bar_rev_item_lookup": (
        ("item_id", 0), ("timeframe", 0), ("close_ts", 0),
        ("revision_available_at", 0), ("publish_seq", 0),
    ),
    "idx_flow_rev_item_ts": (
        ("item_id", 0), ("captured_at", 0),
        ("revision_available_at", 0), ("publish_seq", 0),
    ),
    "idx_review_windows_block": (("pair_block_id", 0), ("window_id", 0)),
    "idx_opp_obs_window_arm": (
        ("window_id", 0), ("arm", 0), ("rank_order", 0),
    ),
    "idx_user_positions_arm_status": (
        ("arm_id", 0), ("status", 0), ("item_id", 0),
    ),
    "idx_user_orders_arm_status": (
        ("arm_id", 0), ("status", 0), ("item_id", 0),
    ),
    "idx_ledger_events_arm_order": (
        ("arm_id", 0), ("event_ts", 0), ("event_order", 0), ("event_id", 0),
    ),
}

EXPECTED_FOREIGN_KEYS = {
    "item_bars": {
        ("item_id", "items_catalog", "item_id"),
    },
    "publish_events": set(),
    "item_bar_revisions": {
        ("item_id", "items_catalog", "item_id"),
        ("publish_seq", "publish_events", "publish_seq"),
    },
    "inferred_flow_revisions": {
        ("snapshot_id", "snapshot_runs", "snapshot_id"),
        ("item_id", "items_catalog", "item_id"),
        ("publish_seq", "publish_events", "publish_seq"),
        ("prev_snapshot_id", "snapshot_runs", "snapshot_id"),
    },
    "orderbook_observations": {
        ("snapshot_id", "snapshot_runs", "snapshot_id"),
        ("item_id", "items_catalog", "item_id"),
        ("publish_seq", "publish_events", "publish_seq"),
    },
    "review_windows": set(),
    "opportunity_observations": {
        ("window_id", "review_windows", "window_id"),
        ("source_snapshot_id", "snapshot_runs", "snapshot_id"),
        ("item_id", "items_catalog", "item_id"),
    },
    "user_positions": {
        ("item_id", "items_catalog", "item_id"),
    },
    "user_orders": {
        ("position_id", "user_positions", "position_id"),
        ("item_id", "items_catalog", "item_id"),
    },
    "user_ledger_events": {
        ("item_id", "items_catalog", "item_id"),
    },
}


def _insert_row(conn, table, values):
    # All identifiers come from constants in this test module.
    columns = ", ".join(values)
    placeholders = ", ".join("?" for _ in values)
    conn.execute(
        f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
        tuple(values.values()),
    )


def _legacy_rows(conn):
    return {
        table: [
            dict(row)
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order_by}")
        ]
        for table, order_by in LEGACY_TABLE_ORDER.items()
    }


def _schema_rows(conn):
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name"
        )
    ]


def _seed_legacy_rows(conn):
    conn.executemany(
        """
        INSERT INTO items_catalog (
            item_id, asset_class, name_en, name_zh, category,
            max_stack_size, description, is_watched, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                138, "item", "Normal item", "普通道具", "medicine",
                99, "Existing snapshot item", 1, BASE_TS,
            ),
            (
                900001, "pokemon_category", "Synthetic category", "精灵分类",
                "pokemon", 1, "Existing manual asset", 1, BASE_TS + 1,
            ),
            (
                139, "pokemon_category", "Low-ID category", "低编号分类",
                "pokemon", 1, None, 0, BASE_TS + 2,
            ),
            (
                900002, "item", "High-ID synthetic item", "高编号道具",
                "other", 99, None, 0, BASE_TS + 3,
            ),
        ],
    )
    conn.executemany(
        """
        INSERT INTO snapshot_runs (
            snapshot_id, source, started_at, server_started_at, completed_at,
            status, total_listings, pages_fetched, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                1, "official_api", BASE_TS + 61, BASE_TS + 60,
                BASE_TS + 65, "completed", 1, 1, None,
            ),
            (
                2, "official_api", BASE_TS + 601, BASE_TS + 600,
                BASE_TS + 605, "completed", 1, 1, None,
            ),
        ],
    )
    conn.executemany(
        """
        INSERT INTO raw_listings (
            snapshot_id, auction_id, item_id, price, quantity, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (1, 101, 138, 10_000, 10, BASE_TS + 86_400),
            (2, 101, 138, 10_000, 4, BASE_TS + 86_400),
        ],
    )
    snapshot_bar = {
        "item_id": 138,
        "timeframe": "15m",
        "bucket_ts": BASE_TS,
        "close_ts": BASE_TS + 900,
        "data_source": "snapshot",
        "open_price": 10_000.5,
        "high_price": 10_100.75,
        "low_price": 9900.25,
        "close_price": 10_020.5,
        "robust_best_ask": 10_021,
        "best_ask_min": 9900,
        "p10_price": 9950.5,
        "total_listed_qty": 14,
        "listing_count": 3,
        "depth_within_5pct": 8,
        "spread_top5_pct": 1.25,
        "est_sold_qty_sum": 6.5,
        "expired_qty_sum": 2,
        "new_listed_qty_sum": 4,
        "sample_count": 3,
        "valid_flow_samples": 2,
    }
    _insert_row(conn, "item_bars", snapshot_bar)
    _insert_row(
        conn,
        "item_bars",
        {
            **snapshot_bar,
            "item_id": 900001,
            "timeframe": "4h",
            "close_ts": BASE_TS + 14_400,
            "data_source": "manual",
            "open_price": 120_000.0,
            "high_price": 125_000.0,
            "low_price": 119_000.0,
            "close_price": 122_000.0,
            "robust_best_ask": 122_000,
            "best_ask_min": 119_000,
            "p10_price": None,
            "total_listed_qty": None,
            "listing_count": None,
            "depth_within_5pct": None,
            "spread_top5_pct": None,
            "est_sold_qty_sum": None,
            "expired_qty_sum": None,
            "new_listed_qty_sum": None,
            "sample_count": 1,
            "valid_flow_samples": 0,
        },
    )
    _insert_row(
        conn,
        "inferred_flows",
        {
            "snapshot_id": 2,
            "prev_snapshot_id": 1,
            "item_id": 138,
            "captured_at": BASE_TS + 600,
            "interval_sec": 540,
            "new_listing_count": 1,
            "new_listed_qty": 2,
            "partial_fill_qty": 1,
            "early_disappear_qty": 5,
            "expired_qty": 2,
            "est_sold_qty": 6.5,
            "est_sold_vwap": 9950.5,
            "gap_skipped": 0,
        },
    )
    _insert_row(
        conn,
        "manual_quotes",
        {
            "quote_id": 7,
            "item_id": 900001,
            "recorded_at": BASE_TS + 300,
            "price": 122_000,
            "listed_qty": 3,
            "note": "历史手工报价",
            "source_tag": "csv",
        },
    )


def _seed_new_dependencies(conn):
    for table, primary_key in (
        ("publish_events", "publish_seq"),
        ("review_windows", "window_id"),
        ("user_positions", "position_id"),
    ):
        values = {**NEW_TABLE_ROWS[table], primary_key: 1}
        _insert_row(conn, table, values)


@pytest.fixture
def legacy_database(tmp_path):
    migrations_dir = tmp_path / "v1_migrations"
    migrations_dir.mkdir()
    shutil.copyfile(MIGRATIONS_DIR / "001_init.sql", migrations_dir / "001_init.sql")
    path = tmp_path / "历史数据库.db"
    assert migrate(
        path, migrations_dir=migrations_dir, applied_at=BASE_TS - 60
    ) == ["001_init"]
    with open_connection(path) as conn, write_transaction(conn):
        _seed_legacy_rows(conn)
        before = _legacy_rows(conn)
    return path, before


@pytest.fixture
def upgraded_database(legacy_database):
    path, before = legacy_database
    assert migrate(path, applied_at=BASE_TS + 3600) == [MIGRATION_VERSION]
    return path, before


def test_upgrade_preserves_populated_v1_rows_and_unknown_history(upgraded_database):
    path, before = upgraded_database
    with open_connection(path) as conn:
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        for table, expected_rows in before.items():
            columns = ", ".join(expected_rows[0])
            order_by = LEGACY_TABLE_ORDER[table]
            actual_rows = [
                dict(row)
                for row in conn.execute(
                    f"SELECT {columns} FROM {table} ORDER BY {order_by}"
                )
            ]
            assert actual_rows == expected_rows, table

        for row in conn.execute(
            "SELECT book_state, bar_available_at, latest_publish_seq FROM item_bars"
        ):
            assert tuple(row) == ("stocked", None, None)

        for row in conn.execute(
            """
            SELECT last_page_server_at, available_at, publish_seq,
                   raw_retained, raw_pruned_at
            FROM snapshot_runs
            """
        ):
            assert tuple(row) == (None, None, None, None, None)

        coverage = {
            row["item_id"]: row["api_covered"]
            for row in conn.execute("SELECT item_id, api_covered FROM items_catalog")
        }
        assert coverage == {138: 1, 139: 0, 900001: 0, 900002: 0}
        for row in conn.execute("SELECT purchase_mode, fee_verified FROM items_catalog"):
            assert tuple(row) == ("unknown", 0)

        assert tuple(
            conn.execute(
                "SELECT observed_undercut_count, latest_publish_seq FROM inferred_flows"
            ).fetchone()
        ) == (0, None)
        assert tuple(
            conn.execute("SELECT ingested_at, publish_seq FROM manual_quotes").fetchone()
        ) == (None, None)

        # Historical rows cannot manufacture known publication or observation history.
        for table in NEW_TABLE_ROWS:
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0

        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT version, applied_at FROM schema_migrations ORDER BY version"
            )
        ] == [
            ("001_init", BASE_TS - 60),
            (MIGRATION_VERSION, BASE_TS + 3600),
        ]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_rebuilt_bars_have_exact_columns_and_nullable_prices(upgraded_database):
    path, _ = upgraded_database
    expected_columns = [
        ("item_id", "INTEGER"),
        ("timeframe", "TEXT"),
        ("bucket_ts", "INTEGER"),
        ("close_ts", "INTEGER"),
        ("data_source", "TEXT"),
        ("book_state", "TEXT"),
        ("bar_available_at", "INTEGER"),
        ("latest_publish_seq", "INTEGER"),
        ("open_price", "REAL"),
        ("high_price", "REAL"),
        ("low_price", "REAL"),
        ("close_price", "REAL"),
        ("robust_best_ask", "INTEGER"),
        ("best_ask_min", "INTEGER"),
        ("p10_price", "REAL"),
        ("total_listed_qty", "INTEGER"),
        ("listing_count", "INTEGER"),
        ("depth_within_5pct", "INTEGER"),
        ("spread_top5_pct", "REAL"),
        ("est_sold_qty_sum", "REAL"),
        ("expired_qty_sum", "INTEGER"),
        ("new_listed_qty_sum", "INTEGER"),
        ("sample_count", "INTEGER"),
        ("valid_flow_samples", "INTEGER"),
    ]
    with open_connection(path) as conn:
        info = conn.execute("PRAGMA table_info(item_bars)").fetchall()
        assert [(row["name"], row["type"]) for row in info] == expected_columns
        assert {row["name"] for row in info if row["notnull"]} == {
            "item_id", "timeframe", "bucket_ts", "close_ts",
            "data_source", "book_state", "sample_count", "valid_flow_samples",
        }
        columns = {row["name"]: row for row in info}
        assert columns["data_source"]["dflt_value"] == "'snapshot'"
        assert columns["book_state"]["dflt_value"] == "'stocked'"
        assert columns["sample_count"]["dflt_value"] == "1"
        assert columns["valid_flow_samples"]["dflt_value"] == "0"


def test_new_tables_keys_foreign_keys_and_index_order(upgraded_database):
    path, _ = upgraded_database
    with open_connection(path) as conn:
        tables = {
            row["name"]: row["strict"]
            for row in conn.execute("PRAGMA table_list")
        }
        assert "item_bars_v2" not in tables
        for table, expected_key in EXPECTED_PRIMARY_KEYS.items():
            assert tables[table] == 1, table
            info = conn.execute(f"PRAGMA table_info({table})").fetchall()
            actual_key = tuple(
                row["name"]
                for row in sorted(info, key=lambda row: row["pk"])
                if row["pk"]
            )
            assert actual_key == expected_key, table

        for table, expected_keys in EXPECTED_FOREIGN_KEYS.items():
            actual_keys = {
                (row["from"], row["table"], row["to"])
                for row in conn.execute(f"PRAGMA foreign_key_list({table})")
            }
            assert actual_keys == expected_keys, table

        for index, expected_columns in EXPECTED_INDEX_COLUMNS.items():
            actual_columns = tuple(
                (row["name"], row["desc"])
                for row in conn.execute(f"PRAGMA index_xinfo({index})")
                if row["key"]
            )
            assert actual_columns == expected_columns, index


def test_empty_confirmed_bar_accepts_null_prices_and_zero_inventory(upgraded_database):
    path, _ = upgraded_database
    with open_connection(path) as conn:
        with write_transaction(conn):
            _insert_row(
                conn,
                "item_bars",
                {
                    "item_id": 138,
                    "timeframe": "15m",
                    "bucket_ts": BASE_TS + 900,
                    "close_ts": BASE_TS + 1800,
                    "book_state": "empty_confirmed",
                    **dict.fromkeys(PRICE_COLUMNS),
                    "total_listed_qty": 0,
                    "listing_count": 0,
                    "depth_within_5pct": 0,
                    "est_sold_qty_sum": 6.0,
                    "expired_qty_sum": 0,
                    "new_listed_qty_sum": 0,
                },
            )
        row = conn.execute(
            """
            SELECT * FROM item_bars
            WHERE item_id = 138 AND timeframe = '15m' AND bucket_ts = ?
            """,
            (BASE_TS + 900,),
        ).fetchone()
        assert row is not None
        assert row["book_state"] == "empty_confirmed"
        assert all(row[column] is None for column in PRICE_COLUMNS)
        assert row["total_listed_qty"] == 0
        assert row["listing_count"] == 0
        assert row["depth_within_5pct"] == 0
        assert row["est_sold_qty_sum"] == 6.0
        assert row["data_source"] == "snapshot"
        assert row["sample_count"] == 1
        assert row["valid_flow_samples"] == 0
        assert row["bar_available_at"] is None
        assert row["latest_publish_seq"] is None


def test_same_second_revisions_have_distinct_publish_keys(upgraded_database):
    path, _ = upgraded_database
    available_at = BASE_TS + 1000
    published = []
    with open_connection(path) as conn:
        with write_transaction(conn):
            for source_id, price in ((1, 10_000.0), (2, 10_050.0)):
                cursor = conn.execute(
                    """
                    INSERT INTO publish_events (available_at, source_type, source_id)
                    VALUES (?, ?, ?)
                    """,
                    (available_at, "snapshot", source_id),
                )
                publish_seq = cursor.lastrowid
                published.append((publish_seq, price))
                _insert_row(
                    conn,
                    "item_bar_revisions",
                    {
                        "item_id": 138,
                        "timeframe": "15m",
                        "bucket_ts": BASE_TS,
                        "publish_seq": publish_seq,
                        "close_ts": BASE_TS + 900,
                        "revision_available_at": available_at,
                        "open_price": price,
                        "high_price": price,
                        "low_price": price,
                        "close_price": price,
                    },
                )

        assert published[0][0] < published[1][0]
        assert [
            tuple(row)
            for row in conn.execute(
                """
                SELECT publish_seq, revision_available_at, close_price
                FROM item_bar_revisions
                ORDER BY publish_seq
                """
            )
        ] == [
            (publish_seq, available_at, price)
            for publish_seq, price in published
        ]
        for publish_seq, expected_price in published:
            row = conn.execute(
                """
                SELECT close_price
                FROM item_bar_revisions
                WHERE item_id = 138 AND timeframe = '15m' AND bucket_ts = ?
                  AND close_ts <= ? AND revision_available_at <= ?
                  AND publish_seq <= ?
                ORDER BY publish_seq DESC
                LIMIT 1
                """,
                (BASE_TS, available_at, available_at, publish_seq),
            ).fetchone()
            assert row["close_price"] == expected_price

        with pytest.raises(sqlite3.IntegrityError), write_transaction(conn):
            _insert_row(
                conn,
                "item_bar_revisions",
                {
                    "item_id": 138,
                    "timeframe": "15m",
                    "bucket_ts": BASE_TS,
                    "publish_seq": published[1][0],
                    "close_ts": BASE_TS + 900,
                    "revision_available_at": available_at,
                },
            )


@pytest.mark.parametrize("table", tuple(NEW_TABLE_ROWS))
def test_every_integer_column_in_each_new_table_rejects_non_integer_text(
    upgraded_database, table
):
    path, _ = upgraded_database
    with open_connection(path) as conn:
        with write_transaction(conn):
            _seed_new_dependencies(conn)
        integer_columns = [
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table})")
            if row["type"] == "INTEGER"
        ]
        assert integer_columns, table
        for column in integer_columns:
            values = {**NEW_TABLE_ROWS[table], column: "not-an-integer"}
            with pytest.raises(
                sqlite3.IntegrityError,
                match=r"(cannot store TEXT value in INTEGER column|datatype mismatch)",
            ):
                with write_transaction(conn):
                    _insert_row(conn, table, values)
            assert not conn.in_transaction

        # The same payload with valid integer values must satisfy all other constraints.
        with write_transaction(conn):
            _insert_row(conn, table, NEW_TABLE_ROWS[table])
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_ledger_idempotency_key_is_unique(upgraded_database):
    path, _ = upgraded_database
    with open_connection(path) as conn:
        with write_transaction(conn):
            _insert_row(conn, "user_ledger_events", NEW_TABLE_ROWS["user_ledger_events"])
        duplicate = {**NEW_TABLE_ROWS["user_ledger_events"], "event_id": 102}
        with pytest.raises(
            sqlite3.IntegrityError, match="UNIQUE constraint failed"
        ), write_transaction(conn):
            _insert_row(conn, "user_ledger_events", duplicate)
        assert conn.execute(
            "SELECT count(*) FROM user_ledger_events"
        ).fetchone()[0] == 1


def test_second_migrate_is_idempotent_and_preserves_schema_and_data(upgraded_database):
    path, _ = upgraded_database
    with open_connection(path) as conn:
        schema_before = _schema_rows(conn)
        rows_before = _legacy_rows(conn)
        migrations_before = [
            tuple(row)
            for row in conn.execute(
                "SELECT version, applied_at FROM schema_migrations ORDER BY version"
            )
        ]

    assert migrate(path, applied_at=BASE_TS + 7200) == []

    with open_connection(path) as conn:
        assert _schema_rows(conn) == schema_before
        assert _legacy_rows(conn) == rows_before
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT version, applied_at FROM schema_migrations ORDER BY version"
            )
        ] == migrations_before


def test_failed_upgrade_rolls_back_rebuild_and_all_column_additions(
    legacy_database, tmp_path
):
    path, before = legacy_database
    migrations_dir = tmp_path / "failing_upgrade"
    migrations_dir.mkdir()
    for filename in ("001_init.sql", f"{MIGRATION_VERSION}.sql"):
        shutil.copyfile(MIGRATIONS_DIR / filename, migrations_dir / filename)
    (migrations_dir / "003_broken.sql").write_text(
        "CREATE TABLE must_rollback (value INTEGER) STRICT;\nINVALID SQL;\n",
        encoding="utf-8",
    )
    with open_connection(path) as conn:
        schema_before = _schema_rows(conn)
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.OperationalError):
        migrate(path, migrations_dir=migrations_dir)

    with open_connection(path) as conn:
        assert _schema_rows(conn) == schema_before
        assert _legacy_rows(conn) == before
        assert [
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations")
        ] == ["001_init"]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    assert migrate(path) == [MIGRATION_VERSION]
