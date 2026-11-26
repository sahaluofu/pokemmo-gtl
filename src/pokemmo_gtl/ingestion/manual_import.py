"""Bounded UTF-8/BOM CSV and single-quote imports with protected manual bars."""

import csv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

from pokemmo_gtl.db.connection import open_connection, write_transaction

MAX_IMPORT_BYTES = 5 * 1024 * 1024
MAX_IMPORT_ROWS = 50_000
MAX_SQLITE_INTEGER = 2**63 - 1
TIMEFRAMES = {"4h": 14_400, "1d": 86_400}


@dataclass(frozen=True)
class ImportResult:
    imported_rows: int
    skipped_snapshot_bars: int
    errors: list[str]


def add_manual_quote(
    db_path: Path,
    item_id: int,
    price: int,
    *,
    recorded_at: int | None = None,
    listed_qty: int | None = None,
    note: str | None = None,
) -> ImportResult:
    """Store one manual observation and refresh its 4h/1d bars atomically."""
    timestamp = int(time.time()) if recorded_at is None else recorded_at
    quote = _validated_quote(item_id, price, timestamp, listed_qty, note)
    with open_connection(db_path) as conn, write_transaction(conn):
        _insert_quote(conn, quote)
        touched = {
            (item_id, timestamp - timestamp % seconds, timeframe)
            for timeframe, seconds in TIMEFRAMES.items()
        }
        skipped = _materialize_bars(conn, touched)
    return ImportResult(1, skipped, [])


def import_csv(
    db_path: Path,
    csv_path: Path,
    *,
    now_ts: int | None = None,
    max_bytes: int = MAX_IMPORT_BYTES,
    max_rows: int = MAX_IMPORT_ROWS,
) -> ImportResult:
    """Stream valid quotes; a structural/size/row-limit error rolls back the file."""
    if not 0 < max_bytes <= MAX_IMPORT_BYTES or not 0 < max_rows <= MAX_IMPORT_ROWS:
        raise ValueError("import limits must be positive and within 5MB / 50,000 rows")
    path = Path(csv_path)
    if path.stat().st_size > max_bytes:
        raise ValueError(f"CSV exceeds {max_bytes} byte limit")
    imported = 0
    errors = []
    touched = set()
    default_ts = int(time.time()) if now_ts is None else now_ts
    with open_connection(db_path) as conn, write_transaction(conn):
        try:
            with path.open("rb") as binary:
                reader = csv.DictReader(_utf8_lines(binary, max_bytes, max_rows + 1), strict=True)
                if reader.fieldnames is None:
                    raise ValueError("CSV is empty")
                headers = [field.strip().lower() for field in reader.fieldnames]
                if len(set(headers)) != len(headers):
                    raise ValueError("CSV contains duplicate headers")
                if not {"item_id", "price"}.issubset(headers):
                    raise ValueError("CSV headers must include item_id and price")
                reader.fieldnames = headers
                for row_number, row in enumerate(reader, start=2):
                    if row_number - 1 > max_rows:
                        raise ValueError(f"CSV exceeds {max_rows} row limit")
                    try:
                        if None in row or any(value is None for value in row.values()):
                            raise ValueError("row has a different column count than its header")
                        item_id = _csv_integer(row["item_id"], "item_id")
                        price = _csv_integer(row["price"], "price")
                        recorded_at = (
                            _csv_integer(row["recorded_at"], "recorded_at")
                            if row.get("recorded_at", "").strip() else default_ts
                        )
                        listed_qty = (
                            _csv_integer(row["listed_qty"], "listed_qty")
                            if row.get("listed_qty", "").strip() else None
                        )
                        quote = _validated_quote(
                            item_id, price, recorded_at, listed_qty, row.get("note") or None
                        )
                        _insert_quote(conn, quote)
                    except ValueError as error:
                        errors.append(f"row {row_number}: {error}")
                        continue
                    imported += 1
                    for timeframe, seconds in TIMEFRAMES.items():
                        touched.add((item_id, recorded_at - recorded_at % seconds, timeframe))
        except (UnicodeDecodeError, csv.Error) as error:
            raise ValueError("invalid UTF-8 CSV structure or encoding") from error
        skipped = _materialize_bars(conn, touched)
    return ImportResult(imported, skipped, errors)


def _validated_quote(item_id, price, recorded_at, listed_qty, note) -> tuple:
    for field, value in (("item_id", item_id), ("price", price), ("recorded_at", recorded_at)):
        if type(value) is not int or not 0 < value <= MAX_SQLITE_INTEGER:
            raise ValueError(f"{field} must be a positive SQLite integer")
    if recorded_at > MAX_SQLITE_INTEGER - 86_400:
        raise ValueError("recorded_at cannot form a valid UTC day bucket")
    if listed_qty is not None and (
        type(listed_qty) is not int or not 0 <= listed_qty <= MAX_SQLITE_INTEGER
    ):
        raise ValueError("listed_qty must be a non-negative SQLite integer")
    if note is not None and not isinstance(note, str):
        raise ValueError("note must be text")
    return item_id, recorded_at, price, listed_qty, note


def _insert_quote(conn, quote: tuple) -> None:
    exists = conn.execute("SELECT 1 FROM items_catalog WHERE item_id = ?", (quote[0],)).fetchone()
    if exists is None:
        raise ValueError("item_id is not present in items_catalog")
    conn.execute(
        """INSERT INTO manual_quotes
           (item_id, recorded_at, price, listed_qty, note, source_tag)
           VALUES (?, ?, ?, ?, ?, 'manual')""",
        quote,
    )


def _materialize_bars(conn, touched: set[tuple[int, int, str]]) -> int:
    skipped = 0
    for item_id, bucket_ts, timeframe in sorted(touched):
        seconds = TIMEFRAMES[timeframe]
        protected = conn.execute(
            """SELECT 1 FROM item_bars WHERE item_id = ? AND timeframe = ?
               AND bucket_ts = ? AND data_source = 'snapshot'""",
            (item_id, timeframe, bucket_ts),
        ).fetchone()
        skipped += protected is not None
        quotes = conn.execute(
            """SELECT price, listed_qty FROM manual_quotes
               WHERE item_id = ? AND recorded_at >= ? AND recorded_at < ?
               ORDER BY recorded_at, quote_id""",
            (item_id, bucket_ts, bucket_ts + seconds),
        ).fetchall()
        first, last = quotes[0], quotes[-1]
        low = min(quote["price"] for quote in quotes)
        high = max(quote["price"] for quote in quotes)
        conn.execute(
            """INSERT INTO item_bars
               (item_id, timeframe, bucket_ts, close_ts, data_source,
                open_price, high_price, low_price, close_price,
                robust_best_ask, best_ask_min, p10_price, total_listed_qty,
                sample_count, valid_flow_samples)
               VALUES (?, ?, ?, ?, 'manual', ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
               ON CONFLICT(item_id, timeframe, bucket_ts) DO UPDATE SET
                 close_ts = excluded.close_ts,
                 data_source = 'manual',
                 open_price = excluded.open_price,
                 high_price = excluded.high_price,
                 low_price = excluded.low_price,
                 close_price = excluded.close_price,
                 robust_best_ask = excluded.robust_best_ask,
                 best_ask_min = excluded.best_ask_min,
                 p10_price = excluded.p10_price,
                 total_listed_qty = excluded.total_listed_qty,
                 listing_count = NULL, depth_within_5pct = NULL, spread_top5_pct = NULL,
                 est_sold_qty_sum = NULL, expired_qty_sum = NULL, new_listed_qty_sum = NULL,
                 sample_count = excluded.sample_count,
                 valid_flow_samples = 0
               WHERE item_bars.data_source != 'snapshot'""",
            (
                item_id, timeframe, bucket_ts, bucket_ts + seconds,
                first["price"], high, low, last["price"], last["price"], low,
                last["price"], last["listed_qty"], len(quotes),
            ),
        )
    return skipped


def _utf8_lines(binary: BinaryIO, max_bytes: int, max_lines: int) -> Iterator[str]:
    consumed = 0
    line_count = 0
    while line := binary.readline(max_bytes - consumed + 1):
        consumed += len(line)
        if consumed > max_bytes:
            raise ValueError(f"CSV exceeds {max_bytes} byte limit")
        line_count += 1
        if line_count > max_lines:
            raise ValueError(f"CSV exceeds {max_lines - 1} row limit")
        yield line.decode("utf-8-sig" if line_count == 1 else "utf-8")


def _csv_integer(value: str, field: str) -> int:
    text = value.strip()
    if not text or not text.isascii() or not text.isdecimal():
        raise ValueError(f"{field} must be an integer")
    try:
        return int(text)
    except ValueError as error:
        raise ValueError(f"{field} is too large") from error
