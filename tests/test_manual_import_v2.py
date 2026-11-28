"""Regression tests for manual-quote ingestion watermarks and atomic revisions."""

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from pokemmo_gtl.db.connection import open_connection
from pokemmo_gtl.ingestion import manual_import
from pokemmo_gtl.ingestion.manual_import import add_manual_quote, import_csv

RECORDED_AT = 1_700_000_000
INGESTED_AT = 1_700_050_000
BAR_SECONDS = {"4h": 14_400, "1d": 86_400}


def _create_database(path: Path, *, migrated: bool = True) -> Path:
    migrations = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "pokemmo_gtl"
        / "db"
        / "migrations"
    )
    with open_connection(path) as conn:
        conn.executescript((migrations / "001_init.sql").read_text(encoding="utf-8"))
        conn.executemany(
            """INSERT INTO items_catalog
               (item_id, asset_class, name_en, max_stack_size, updated_at)
               VALUES (?, 'pokemon_category', ?, 1, ?)""",
            [
                (900001, "Manual asset A", RECORDED_AT),
                (900002, "Manual asset B", RECORDED_AT),
            ],
        )
        if migrated:
            conn.executescript(
                (migrations / "002_v2_ledger_and_observations.sql").read_text(
                    encoding="utf-8"
                )
            )
    return path


@pytest.fixture
def v2_db(tmp_path: Path) -> Path:
    return _create_database(tmp_path / "manual-v2.db")


def _database_state(db_path: Path) -> dict:
    queries = {
        "manual_quotes": """
            SELECT quote_id, item_id, recorded_at, price, listed_qty, note,
                   source_tag, ingested_at, publish_seq
            FROM manual_quotes ORDER BY quote_id
        """,
        "publish_events": """
            SELECT publish_seq, available_at, source_type, source_id
            FROM publish_events ORDER BY publish_seq
        """,
        "item_bars": """
            SELECT item_id, timeframe, bucket_ts, close_ts, data_source,
                   book_state, bar_available_at, latest_publish_seq,
                   open_price, high_price, low_price, close_price,
                   robust_best_ask, best_ask_min, p10_price, total_listed_qty,
                   listing_count, depth_within_5pct, spread_top5_pct,
                   est_sold_qty_sum, expired_qty_sum, new_listed_qty_sum,
                   sample_count, valid_flow_samples
            FROM item_bars ORDER BY item_id, timeframe, bucket_ts
        """,
        "item_bar_revisions": """
            SELECT item_id, timeframe, bucket_ts, publish_seq, close_ts,
                   revision_available_at, data_source, book_state,
                   open_price, high_price, low_price, close_price,
                   robust_best_ask, best_ask_min, p10_price, total_listed_qty,
                   listing_count, depth_within_5pct, spread_top5_pct,
                   est_sold_qty_sum, expired_qty_sum, new_listed_qty_sum,
                   sample_count, valid_flow_samples
            FROM item_bar_revisions
            ORDER BY item_id, timeframe, bucket_ts, publish_seq
        """,
    }
    with open_connection(db_path) as conn:
        return {
            name: [tuple(row) for row in conn.execute(sql)]
            for name, sql in queries.items()
        }


def _visible_revisions(
    db_path: Path,
    *,
    as_of: int,
    read_publish_seq: int | None = None,
) -> list[dict]:
    with open_connection(db_path) as conn:
        rows = conn.execute(
            """WITH visible AS (
                   SELECT item_id, timeframe, bucket_ts, publish_seq,
                          close_price, sample_count
                   FROM item_bar_revisions
                   WHERE close_ts <= :as_of
                     AND revision_available_at <= :as_of
                     AND (:read_publish_seq IS NULL OR publish_seq <= :read_publish_seq)
               )
               SELECT v.item_id, v.timeframe, v.bucket_ts, v.publish_seq,
                      v.close_price, v.sample_count
               FROM visible AS v
               WHERE v.publish_seq = (
                   SELECT MAX(previous.publish_seq)
                   FROM visible AS previous
                   WHERE previous.item_id = v.item_id
                     AND previous.timeframe = v.timeframe
                     AND previous.bucket_ts = v.bucket_ts
               )
               ORDER BY v.item_id, v.timeframe, v.bucket_ts""",
            {"as_of": as_of, "read_publish_seq": read_publish_seq},
        ).fetchall()
    return [dict(row) for row in rows]


def test_single_quote_uses_ingestion_watermark_for_point_in_time_visibility(v2_db: Path):
    result = add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        ingested_at=INGESTED_AT,
    )

    assert result.imported_rows == 1
    assert result.skipped_snapshot_bars == 0
    assert result.errors == []

    with open_connection(v2_db) as conn:
        quote = conn.execute(
            "SELECT quote_id, ingested_at, publish_seq FROM manual_quotes"
        ).fetchone()
        events = conn.execute(
            """SELECT publish_seq, available_at, source_type, source_id
               FROM publish_events"""
        ).fetchall()
        revisions = conn.execute(
            """SELECT timeframe, close_ts, revision_available_at, publish_seq,
                      data_source, book_state, close_price, valid_flow_samples
               FROM item_bar_revisions ORDER BY timeframe"""
        ).fetchall()
        bars = conn.execute(
            """SELECT timeframe, book_state, bar_available_at, latest_publish_seq
               FROM item_bars ORDER BY timeframe"""
        ).fetchall()

    assert quote is not None
    assert quote["ingested_at"] == INGESTED_AT
    assert quote["publish_seq"] > 0
    assert [tuple(event) for event in events] == [
        (quote["publish_seq"], INGESTED_AT, "manual_quote", quote["quote_id"])
    ]
    assert len(revisions) == 2
    assert {row["timeframe"] for row in revisions} == {"4h", "1d"}
    for row in revisions:
        assert row["close_ts"] < 1_700_020_000
        assert row["revision_available_at"] == INGESTED_AT
        assert row["publish_seq"] == quote["publish_seq"]
        assert row["data_source"] == "manual"
        assert row["book_state"] == "stocked"
        assert row["close_price"] == 120_000
        assert row["valid_flow_samples"] == 0
    assert len(bars) == 2
    for row in bars:
        assert row["book_state"] == "stocked"
        assert row["bar_available_at"] == INGESTED_AT
        assert row["latest_publish_seq"] == quote["publish_seq"]

    assert _visible_revisions(v2_db, as_of=1_700_020_000) == []
    assert _visible_revisions(v2_db, as_of=INGESTED_AT - 1) == []
    visible = _visible_revisions(v2_db, as_of=INGESTED_AT)
    assert len(visible) == 2
    assert {row["timeframe"] for row in visible} == {"4h", "1d"}
    assert all(row["close_price"] == 120_000 for row in visible)


def test_same_second_publications_keep_distinct_revisions_and_read_boundaries(v2_db: Path):
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        listed_qty=2,
        ingested_at=INGESTED_AT,
    )
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=126_000,
        recorded_at=RECORDED_AT + 1,
        listed_qty=3,
        ingested_at=INGESTED_AT,
    )

    with open_connection(v2_db) as conn:
        quotes = conn.execute(
            """SELECT quote_id, publish_seq, ingested_at
               FROM manual_quotes ORDER BY quote_id"""
        ).fetchall()
        events = conn.execute(
            """SELECT publish_seq, available_at, source_type, source_id
               FROM publish_events ORDER BY publish_seq"""
        ).fetchall()
        revisions = conn.execute(
            """SELECT timeframe, publish_seq, open_price, high_price, low_price,
                      close_price, robust_best_ask, best_ask_min, p10_price,
                      total_listed_qty, sample_count, valid_flow_samples,
                      revision_available_at
               FROM item_bar_revisions ORDER BY timeframe, publish_seq"""
        ).fetchall()
        bars = conn.execute(
            """SELECT latest_publish_seq, close_price, sample_count
               FROM item_bars ORDER BY timeframe"""
        ).fetchall()

    first_seq, second_seq = [row["publish_seq"] for row in quotes]
    assert 0 < first_seq < second_seq
    assert [row["ingested_at"] for row in quotes] == [INGESTED_AT, INGESTED_AT]
    assert [tuple(event) for event in events] == [
        (first_seq, INGESTED_AT, "manual_quote", quotes[0]["quote_id"]),
        (second_seq, INGESTED_AT, "manual_quote", quotes[1]["quote_id"]),
    ]
    assert len(revisions) == 4
    for timeframe in ("4h", "1d"):
        bucket_revisions = [
            row for row in revisions if row["timeframe"] == timeframe
        ]
        assert [
            (row["publish_seq"], row["close_price"], row["sample_count"])
            for row in bucket_revisions
        ] == [(first_seq, 120_000, 1), (second_seq, 126_000, 2)]
        latest = bucket_revisions[-1]
        assert latest["open_price"] == 120_000
        assert latest["high_price"] == 126_000
        assert latest["low_price"] == 120_000
        assert latest["robust_best_ask"] == 126_000
        assert latest["best_ask_min"] == 120_000
        assert latest["p10_price"] == 126_000
        assert latest["total_listed_qty"] == 3
        assert latest["valid_flow_samples"] == 0
    assert all(row["revision_available_at"] == INGESTED_AT for row in revisions)
    assert len(bars) == 2
    assert all(
        tuple(row) == (second_seq, 126_000, 2)
        for row in bars
    )

    first_view = _visible_revisions(
        v2_db, as_of=INGESTED_AT, read_publish_seq=first_seq
    )
    assert len(first_view) == 2
    assert all(row["publish_seq"] == first_seq for row in first_view)
    assert all(row["close_price"] == 120_000 for row in first_view)
    latest_view = _visible_revisions(v2_db, as_of=INGESTED_AT)
    assert len(latest_view) == 2
    assert all(row["publish_seq"] == second_seq for row in latest_view)
    assert all(row["close_price"] == 126_000 for row in latest_view)
    assert _visible_revisions(v2_db, as_of=INGESTED_AT, read_publish_seq=0) == []


@pytest.mark.parametrize(
    "protected_timeframes",
    [("4h",), ("1d",), ("4h", "1d")],
)
def test_snapshot_protection_skips_current_bars_and_manual_revisions(
    v2_db: Path, protected_timeframes: tuple[str, ...]
):
    with open_connection(v2_db) as conn:
        for timeframe in protected_timeframes:
            seconds = BAR_SECONDS[timeframe]
            bucket_ts = RECORDED_AT - RECORDED_AT % seconds
            conn.execute(
                """INSERT INTO item_bars
                   (item_id, timeframe, bucket_ts, close_ts, data_source,
                    book_state, bar_available_at,
                    open_price, high_price, low_price, close_price,
                    robust_best_ask, best_ask_min, p10_price, total_listed_qty,
                    sample_count, valid_flow_samples)
                   VALUES (900001, ?, ?, ?, 'snapshot', 'stocked', ?,
                           250000, 260000, 240000, 255000,
                           255000, 240000, 255000, 7, 9, 0)""",
                (timeframe, bucket_ts, bucket_ts + seconds, INGESTED_AT - 10),
            )
    before = _database_state(v2_db)["item_bars"]

    result = add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        ingested_at=INGESTED_AT,
    )

    assert result.imported_rows == 1
    assert result.skipped_snapshot_bars == len(protected_timeframes)
    assert result.errors == []
    after = _database_state(v2_db)
    assert [
        row for row in after["item_bars"] if row[1] in protected_timeframes
    ] == before
    assert len(after["manual_quotes"]) == 1
    assert len(after["publish_events"]) == 1
    assert len(after["item_bar_revisions"]) == 2 - len(protected_timeframes)
    assert {row[1] for row in after["item_bar_revisions"]} == (
        {"4h", "1d"} - set(protected_timeframes)
    )
    assert all(row[6] == "manual" for row in after["item_bar_revisions"])


def test_csv_publishes_multiple_items_with_one_sequence_and_complete_metrics(
    v2_db: Path, tmp_path: Path
):
    csv_path = tmp_path / "multiple-items.csv"
    csv_path.write_text(
        "item_id,price,recorded_at,listed_qty,note\n"
        "900001,125000,1700000060,4,later observation\n"
        "900002,250000,1700000000,2,other asset\n"
        "900001,120000,1700000000,3,earlier observation\n",
        encoding="utf-8",
    )

    result = import_csv(v2_db, csv_path, now_ts=INGESTED_AT)

    assert result.imported_rows == 3
    assert result.skipped_snapshot_bars == 0
    assert result.errors == []
    with open_connection(v2_db) as conn:
        quotes = conn.execute(
            """SELECT quote_id, ingested_at, publish_seq
               FROM manual_quotes ORDER BY quote_id"""
        ).fetchall()
        events = conn.execute(
            """SELECT publish_seq, available_at, source_type, source_id
               FROM publish_events"""
        ).fetchall()
        revisions = conn.execute(
            """SELECT item_id, timeframe, publish_seq, revision_available_at,
                      data_source, book_state,
                      open_price, high_price, low_price, close_price,
                      robust_best_ask, best_ask_min, p10_price, total_listed_qty,
                      sample_count, valid_flow_samples
               FROM item_bar_revisions ORDER BY item_id, timeframe"""
        ).fetchall()
        bars = conn.execute(
            """SELECT book_state, bar_available_at, latest_publish_seq
               FROM item_bars"""
        ).fetchall()

    assert len(quotes) == 3
    assert len(events) == 1
    publish_seq = events[0]["publish_seq"]
    assert publish_seq > 0
    assert tuple(events[0]) == (
        publish_seq, INGESTED_AT, "manual_quote", quotes[-1]["quote_id"]
    )
    assert all(row["ingested_at"] == INGESTED_AT for row in quotes)
    assert all(row["publish_seq"] == publish_seq for row in quotes)
    assert len(revisions) == 4
    assert {(row["item_id"], row["timeframe"]) for row in revisions} == {
        (900001, "4h"), (900001, "1d"), (900002, "4h"), (900002, "1d")
    }
    metric_columns = (
        "open_price", "high_price", "low_price", "close_price",
        "robust_best_ask", "best_ask_min", "p10_price", "total_listed_qty",
        "sample_count", "valid_flow_samples",
    )
    expected_metrics = {
        900001: (
            120_000, 125_000, 120_000, 125_000,
            125_000, 120_000, 125_000, 4, 2, 0,
        ),
        900002: (
            250_000, 250_000, 250_000, 250_000,
            250_000, 250_000, 250_000, 2, 1, 0,
        ),
    }
    for row in revisions:
        assert row["publish_seq"] == publish_seq
        assert row["revision_available_at"] == INGESTED_AT
        assert row["data_source"] == "manual"
        assert row["book_state"] == "stocked"
        assert tuple(row[column] for column in metric_columns) == expected_metrics[
            row["item_id"]
        ]
    assert len(bars) == 4
    assert all(
        tuple(row) == ("stocked", INGESTED_AT, publish_seq)
        for row in bars
    )


@pytest.mark.parametrize("limit_kind", ["bytes", "streaming_bytes", "rows"])
def test_csv_limit_failure_preserves_all_existing_rows(
    v2_db: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_kind: str,
):
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=110_000,
        recorded_at=RECORDED_AT - 10,
        note="existing quote",
        ingested_at=INGESTED_AT - 1,
    )
    before = _database_state(v2_db)
    lines = [
        "item_id,price,recorded_at,listed_qty\n",
        "900001,130000,1700000010,5\n",
        "900002,250000,1700000010,6\n",
        "900001,140000,1700000020,7\n",
    ]
    csv_path = tmp_path / "exceeds-limit.csv"
    csv_path.write_bytes("".join(lines).encode("utf-8"))
    byte_limit = len("".join(lines[:3]).encode("utf-8"))
    limits = {"max_rows": 2} if limit_kind == "rows" else {"max_bytes": byte_limit}
    expected_error = "row limit" if limit_kind == "rows" else "byte limit"

    with monkeypatch.context() as patch:
        if limit_kind == "streaming_bytes":
            original_stat = Path.stat

            def underreported_stat(path, *args, **kwargs):
                if path == csv_path:
                    return SimpleNamespace(st_size=0)
                return original_stat(path, *args, **kwargs)

            patch.setattr(Path, "stat", underreported_stat)
        with pytest.raises(ValueError, match=expected_error):
            import_csv(v2_db, csv_path, now_ts=INGESTED_AT, **limits)

    assert _database_state(v2_db) == before


def test_csv_revision_failure_rolls_back_quotes_event_bars_and_prior_revisions(
    v2_db: Path, tmp_path: Path
):
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=110_000,
        recorded_at=RECORDED_AT - 10,
        note="existing quote",
        ingested_at=INGESTED_AT - 1,
    )
    before = _database_state(v2_db)
    with open_connection(v2_db) as conn:
        conn.execute(
            """CREATE TRIGGER reject_manual_revision
               BEFORE INSERT ON item_bar_revisions
               WHEN NEW.item_id = 900002 AND NEW.data_source = 'manual'
               BEGIN
                   SELECT RAISE(ABORT, 'reject manual revision');
               END"""
        )
    csv_path = tmp_path / "revision-failure.csv"
    csv_path.write_text(
        "item_id,price,recorded_at\n"
        "900001,130000,1700000000\n"
        "900002,250000,1700000000\n",
        encoding="utf-8",
    )

    with pytest.raises(sqlite3.IntegrityError, match="reject manual revision"):
        import_csv(v2_db, csv_path, now_ts=INGESTED_AT)

    assert _database_state(v2_db) == before


def test_revision_watermark_includes_latest_recorded_quote_in_the_bucket(
    v2_db: Path, monkeypatch: pytest.MonkeyPatch
):
    future_recorded_at = RECORDED_AT + 600
    monkeypatch.setattr(manual_import.time, "time", lambda: float(RECORDED_AT + 10))
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=125_000,
        recorded_at=future_recorded_at,
    )
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        ingested_at=RECORDED_AT + 20,
    )

    with open_connection(v2_db) as conn:
        revisions = conn.execute(
            """SELECT revision_available_at, publish_seq
               FROM item_bar_revisions"""
        ).fetchall()
        bars = conn.execute(
            "SELECT bar_available_at, close_price FROM item_bars"
        ).fetchall()
        quotes = conn.execute(
            "SELECT ingested_at FROM manual_quotes ORDER BY quote_id"
        ).fetchall()

    assert [row["ingested_at"] for row in quotes] == [
        RECORDED_AT + 10, RECORDED_AT + 20
    ]
    assert len(revisions) == 4
    assert all(
        row["revision_available_at"] == future_recorded_at
        for row in revisions
    )
    assert len(bars) == 2
    assert all(row["bar_available_at"] == future_recorded_at for row in bars)
    assert all(row["close_price"] == 125_000 for row in bars)


def test_default_ingestion_timestamp_uses_clock_for_quote_and_csv(
    v2_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(manual_import.time, "time", lambda: float(INGESTED_AT))
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
    )
    csv_path = tmp_path / "default-ingestion.csv"
    csv_path.write_text(
        "item_id,price,recorded_at\n"
        "900002,250000,1700000000\n"
        "900002,260000,\n",
        encoding="utf-8",
    )

    result = import_csv(v2_db, csv_path)

    assert result.imported_rows == 2
    assert result.errors == []
    with open_connection(v2_db) as conn:
        quotes = conn.execute(
            """SELECT recorded_at, ingested_at, publish_seq
               FROM manual_quotes ORDER BY quote_id"""
        ).fetchall()
    assert [row["recorded_at"] for row in quotes] == [
        RECORDED_AT, RECORDED_AT, INGESTED_AT
    ]
    assert all(row["ingested_at"] == INGESTED_AT for row in quotes)
    assert quotes[0]["publish_seq"] < quotes[1]["publish_seq"]
    assert quotes[1]["publish_seq"] == quotes[2]["publish_seq"]


@pytest.mark.parametrize("entrypoint", ["quote", "csv"])
@pytest.mark.parametrize("invalid_timestamp", [0, -1, True, 1.5, "1700050000", 2**63])
def test_ingestion_timestamp_must_be_a_positive_sqlite_integer(
    v2_db: Path,
    tmp_path: Path,
    entrypoint: str,
    invalid_timestamp,
):
    csv_path = tmp_path / "invalid-watermark.csv"
    csv_path.write_text(
        "item_id,price,recorded_at\n900001,120000,1700000000\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="ingested_at must be a positive SQLite integer"
    ):
        if entrypoint == "quote":
            add_manual_quote(
                v2_db,
                item_id=900001,
                price=120_000,
                recorded_at=RECORDED_AT,
                ingested_at=invalid_timestamp,
            )
        else:
            import_csv(v2_db, csv_path, now_ts=invalid_timestamp)

    assert all(not rows for rows in _database_state(v2_db).values())


def test_ingestion_timestamp_accepts_largest_sqlite_integer(v2_db: Path):
    largest_timestamp = 2**63 - 1
    add_manual_quote(
        v2_db,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        ingested_at=largest_timestamp,
    )

    with open_connection(v2_db) as conn:
        quote = conn.execute("SELECT ingested_at FROM manual_quotes").fetchone()
        event = conn.execute("SELECT available_at FROM publish_events").fetchone()
        revisions = conn.execute(
            "SELECT revision_available_at FROM item_bar_revisions"
        ).fetchall()
    assert quote["ingested_at"] == largest_timestamp
    assert event["available_at"] == largest_timestamp
    assert len(revisions) == 2
    assert all(
        row["revision_available_at"] == largest_timestamp
        for row in revisions
    )


def test_csv_with_no_valid_quotes_does_not_allocate_a_publication(
    v2_db: Path, tmp_path: Path
):
    csv_path = tmp_path / "invalid-rows.csv"
    csv_path.write_text(
        "item_id,price,recorded_at\n"
        "999999,120000,1700000000\n"
        "900001,-1,1700000000\n",
        encoding="utf-8",
    )

    result = import_csv(v2_db, csv_path, now_ts=INGESTED_AT)

    assert result.imported_rows == 0
    assert result.skipped_snapshot_bars == 0
    assert len(result.errors) == 2
    assert all(not rows for rows in _database_state(v2_db).values())


@pytest.mark.parametrize(
    "extra_columns",
    [(), ("ingested_at",), ("publish_seq",), ("ingested_at", "publish_seq")],
    ids=["001", "ingested-only", "sequence-only", "both-columns"],
)
def test_legacy_schema_keeps_single_quote_and_csv_imports_compatible(
    tmp_path: Path, extra_columns: tuple[str, ...]
):
    db_path = _create_database(tmp_path / "manual-legacy.db", migrated=False)
    column_statements = {
        "ingested_at": "ALTER TABLE manual_quotes ADD COLUMN ingested_at INTEGER",
        "publish_seq": "ALTER TABLE manual_quotes ADD COLUMN publish_seq INTEGER",
    }
    with open_connection(db_path) as conn:
        for column in extra_columns:
            conn.execute(column_statements[column])

    quote_result = add_manual_quote(
        db_path,
        item_id=900001,
        price=120_000,
        recorded_at=RECORDED_AT,
        listed_qty=3,
        note="legacy quote",
        ingested_at=INGESTED_AT,
    )
    csv_path = tmp_path / "legacy.csv"
    csv_path.write_text(
        "item_id,price,recorded_at,listed_qty,note\n"
        "900002,250000,1700000000,4,csv quote\n",
        encoding="utf-8",
    )
    csv_result = import_csv(db_path, csv_path, now_ts=INGESTED_AT + 1)

    assert quote_result.imported_rows == 1
    assert quote_result.skipped_snapshot_bars == 0
    assert quote_result.errors == []
    assert csv_result.imported_rows == 1
    assert csv_result.skipped_snapshot_bars == 0
    assert csv_result.errors == []
    with open_connection(db_path) as conn:
        quotes = conn.execute(
            """SELECT item_id, recorded_at, price, listed_qty, note, source_tag
               FROM manual_quotes ORDER BY quote_id"""
        ).fetchall()
        bars = conn.execute(
            """SELECT item_id, timeframe, data_source, close_price, sample_count
               FROM item_bars ORDER BY item_id, timeframe"""
        ).fetchall()
        publication_tables = conn.execute(
            """SELECT name FROM sqlite_master
               WHERE type = 'table'
                 AND name IN ('publish_events', 'item_bar_revisions')"""
        ).fetchall()
        if "ingested_at" in extra_columns:
            assert [
                row["ingested_at"]
                for row in conn.execute(
                    "SELECT ingested_at FROM manual_quotes ORDER BY quote_id"
                )
            ] == [INGESTED_AT, INGESTED_AT + 1]
        if "publish_seq" in extra_columns:
            assert [
                row["publish_seq"]
                for row in conn.execute(
                    "SELECT publish_seq FROM manual_quotes ORDER BY quote_id"
                )
            ] == [None, None]

    assert [tuple(row) for row in quotes] == [
        (900001, RECORDED_AT, 120_000, 3, "legacy quote", "manual"),
        (900002, RECORDED_AT, 250_000, 4, "csv quote", "manual"),
    ]
    assert [tuple(row) for row in bars] == [
        (900001, "1d", "manual", 120_000, 1),
        (900001, "4h", "manual", 120_000, 1),
        (900002, "1d", "manual", 250_000, 1),
        (900002, "4h", "manual", 250_000, 1),
    ]
    assert publication_tables == []
