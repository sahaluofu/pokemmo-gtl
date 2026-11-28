"""Official read-only Auctions API with atomic, two-phase snapshot persistence."""

import math
import random
import re
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Self

import httpx
from pydantic import SecretStr

from pokemmo_gtl.config import AppConfig
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.ingestion.rate_limiter import (
    HEARTBEAT_INTERVAL_SEC,
    CollectorBusyError,
    CollectorLock,
    SQLiteRateLimiter,
)

ENDPOINT = "https://api.pokemmo.com/api/auctions/items"
PAGE_SIZE = 1000
MAX_PAGES = 100
MAX_RETRIES = 3
STALE_SNAPSHOT_SEC = 1800
HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=10.0)
SQLITE_MAX_INT = 2**63 - 1
PostSnapshotHook = Callable[[sqlite3.Connection, int], None]


class ApiError(RuntimeError):
    """A collection failure containing no credentials, response body or request."""


class ApiAuthError(ApiError):
    """Missing/invalid token, rejected authentication, or an unsafe redirect."""


class ApiResponseError(ApiError):
    """The official endpoint returned an invalid status or payload."""


class ApiPaginationError(ApiResponseError, AssertionError):
    """The cursor or hard page-count invariant was violated."""


@dataclass(frozen=True)
class AuctionListing:
    auction_id: int
    reference_id: int
    price: int
    quantity: int
    expires_at: int


@dataclass(frozen=True)
class SnapshotResult:
    snapshot_id: int
    server_started_at: int
    total_listings: int
    pages_fetched: int


def _header_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except (TypeError, ValueError, OverflowError, OSError):
        return None


class OfficialApiCollector:
    """Collect one snapshot while holding a shared, renewable collector lease.

    ``post_snapshot_hook(conn, snapshot_id)`` runs after raw rows and collection
    watermarks are visible in the phase-two transaction, while status is still
    ``running``. M3 may write flows and bars there using this same connection;
    it must not commit, open a nested write transaction, or perform network I/O.
    The publication timestamp is fixed before the hook so its revisions and the
    completed snapshot retain the same availability watermark.
    Any hook failure rolls back the entire snapshot, including its catalog rows.
    Supplied clients/limiters remain owned by their caller.
    """

    def __init__(
        self,
        db_path: Path,
        token: SecretStr | str | None,
        *,
        limiter: SQLiteRateLimiter | None = None,
        client: httpx.Client | None = None,
        post_snapshot_hook: PostSnapshotHook | None = None,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
    ) -> None:
        value = token.get_secret_value() if isinstance(token, SecretStr) else token
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-fA-F]{64}", value) is None:
            raise ApiAuthError("POKEMMO_API_TOKEN must contain 64 hexadecimal characters")
        self._token = SecretStr(value)
        self.db_path = Path(db_path)
        self._clock = clock
        self._sleeper = sleeper
        self._jitter = jitter
        self.post_snapshot_hook = post_snapshot_hook
        self._owns_limiter = limiter is None
        self.limiter = limiter if limiter is not None else SQLiteRateLimiter(
            self.db_path.parent / "api_limiter.db", clock=clock, sleeper=sleeper
        )
        self._owns_client = client is None
        self._client = client if client is not None else httpx.Client(
            follow_redirects=False, timeout=HTTP_TIMEOUT
        )

    def _sleep(self, delay: float, lease: CollectorLock) -> None:
        # Large Retry-After values and quota waits must not age out the lease.
        while delay > 0:
            lease.heartbeat()
            step = min(delay, HEARTBEAT_INTERVAL_SEC)
            self._sleeper(step)
            delay -= step
        lease.heartbeat()

    def _retry_delay(self, response: httpx.Response | None, retry: int) -> float:
        if response is not None:
            value = response.headers.get("Retry-After")
            if value is not None:
                try:
                    delay = float(value)
                except ValueError:
                    retry_at = _header_timestamp(value)
                    if retry_at is not None:
                        reference = _header_timestamp(response.headers.get("Date"))
                        return max(0.0, retry_at - (
                            reference if reference is not None else self._clock()
                        ))
                else:
                    if math.isfinite(delay) and delay >= 0:
                        return delay
        return 2.0**retry + self._jitter()

    def _request_page(self, skip: int | None, lease: CollectorLock) -> httpx.Response:
        params = {"count": PAGE_SIZE}
        if skip is not None:
            if skip <= 0:
                raise ApiPaginationError("Auction cursor must be positive")
            params["skip"] = skip
        for retry in range(MAX_RETRIES + 1):
            lease.heartbeat()
            self.limiter.acquire(PAGE_SIZE, heartbeat=lease.heartbeat)
            response = None
            try:
                response = self._client.get(
                    ENDPOINT,
                    params=params,
                    headers={
                        "Private-Token": self._token.get_secret_value(),
                        "Accept": "application/json",
                    },
                    follow_redirects=False,
                    timeout=HTTP_TIMEOUT,
                )
            except httpx.TransportError:
                # Never chain an httpx exception: it can carry a secret-bearing request.
                if retry == MAX_RETRIES:
                    raise ApiError("Official API transport retries exhausted") from None
            except Exception:
                raise ApiError("Official API request failed") from None
            else:
                status = response.status_code
                if 300 <= status <= 399 or status in (401, 403):
                    response.close()
                    raise ApiAuthError(f"Official API authentication rejected (HTTP {status})")
                if 200 <= status <= 299:
                    lease.heartbeat()
                    return response
                if status != 429 and not 500 <= status <= 599:
                    response.close()
                    raise ApiResponseError(f"Official API returned HTTP {status}")
                if retry == MAX_RETRIES:
                    response.close()
                    raise ApiError(f"Official API retries exhausted (HTTP {status})")
            delay = self._retry_delay(response, retry)
            if response is not None:
                response.close()
            self._sleep(delay, lease)
        raise ApiError("Official API retries exhausted")  # Defensive unreachable guard.

    @staticmethod
    def _parse_page(response: httpx.Response) -> list[AuctionListing]:
        try:
            payload = response.json()
        except (ValueError, UnicodeError):
            raise ApiResponseError("Official API response must be valid JSON") from None
        if not isinstance(payload, list):
            raise ApiResponseError("Official API response must be a JSON list")
        page = []
        fields = ("auction_id", "reference_id", "price", "quantity", "expires_at")
        for row in payload:
            if not isinstance(row, dict) or any(
                type(row.get(field)) is not int or row[field] <= 0 for field in fields
            ):
                raise ApiResponseError("Auction fields must be positive integers")
            values = {field: row[field] for field in fields}
            if values["expires_at"] > 10**11:
                values["expires_at"] //= 1000
            if any(value > SQLITE_MAX_INT for value in values.values()):
                raise ApiResponseError("Auction integer exceeds the SQLite range")
            page.append(AuctionListing(**values))
        return page

    def _fetch(self, lease: CollectorLock) -> tuple[list[AuctionListing], int, int, int]:
        listings: dict[int, AuctionListing] = {}
        skip = None
        server_started_at = int(self._clock())
        last_page_server_at = server_started_at
        for pages_fetched in range(1, MAX_PAGES + 1):
            response = self._request_page(skip, lease)
            try:
                timestamp = _header_timestamp(response.headers.get("Date"))
                page_server_at = timestamp if timestamp is not None else int(self._clock())
                if pages_fetched == 1:
                    server_started_at = page_server_at
                last_page_server_at = page_server_at
                page = self._parse_page(response)
            finally:
                response.close()
            if not page:
                return (
                    list(listings.values()), server_started_at,
                    last_page_server_at, pages_fetched,
                )
            for listing in page:
                listings.setdefault(listing.auction_id, listing)
            min_id = min(listing.auction_id for listing in page)
            if min_id <= 1:
                return (
                    list(listings.values()), server_started_at,
                    last_page_server_at, pages_fetched,
                )
            next_skip = min_id - 1
            if skip is not None and next_skip >= skip:
                raise ApiPaginationError("Auction cursor must strictly decrease")
            skip = next_skip
        raise ApiPaginationError("Official API exceeded the 100-page safety limit")

    def _start_snapshot(self) -> int:
        now = int(self._clock())
        with open_connection(self.db_path) as conn, write_transaction(conn):
            snapshot_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(snapshot_runs)")
            }
            retention_clause = (
                ", raw_retained = 0" if "raw_retained" in snapshot_columns else ""
            )
            conn.execute(
                "DELETE FROM raw_listings WHERE snapshot_id IN "
                "(SELECT snapshot_id FROM snapshot_runs WHERE status = 'running' "
                "AND started_at < ?)", (now - STALE_SNAPSHOT_SEC,),
            )
            conn.execute(
                "UPDATE snapshot_runs SET status = 'failed', completed_at = ?, "
                "error_message = 'Stale running snapshot recovered'"
                f"{retention_clause} "
                "WHERE status = 'running' AND started_at < ?",
                (now, now - STALE_SNAPSHOT_SEC),
            )
            cursor = conn.execute(
                "INSERT INTO snapshot_runs (source, started_at, server_started_at, status) "
                "VALUES ('official_api', ?, ?, 'running')", (now, now),
            )
            return int(cursor.lastrowid)

    def _persist(
        self, snapshot_id: int, listings: list[AuctionListing], server_started_at: int,
        last_page_server_at: int, pages_fetched: int, lease: CollectorLock,
    ) -> None:
        with open_connection(self.db_path) as conn, write_transaction(conn):
            # Reserve one timestamp before the hook. Re-reading the clock afterward
            # could make snapshot availability disagree with its published revisions.
            commit_ts = int(self._clock())
            available_at = max(commit_ts, last_page_server_at)
            has_api_covered = any(
                row[1] == "api_covered"
                for row in conn.execute("PRAGMA table_info(items_catalog)")
            )
            catalog_columns = [
                "item_id", "name_en", "category", "max_stack_size", "updated_at",
            ]
            if has_api_covered:
                catalog_columns.append("api_covered")
            conn.executemany(
                "INSERT OR IGNORE INTO items_catalog "
                f"({', '.join(catalog_columns)}) "
                f"VALUES ({', '.join('?' for _ in catalog_columns)})",
                [
                    (item_id, f"Unknown Item #{item_id}", "unknown", 99, commit_ts)
                    + ((int(item_id < 900_000),) if has_api_covered else ())
                    for item_id in sorted({listing.reference_id for listing in listings})
                ],
            )
            conn.executemany(
                "INSERT INTO raw_listings "
                "(snapshot_id, auction_id, item_id, price, quantity, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(snapshot_id, row.auction_id, row.reference_id, row.price,
                  row.quantity, row.expires_at) for row in listings],
            )
            snapshot_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(snapshot_runs)")
            }
            metadata = {
                "server_started_at": server_started_at,
                "total_listings": len(listings),
                "pages_fetched": pages_fetched,
            }
            for name, value in (
                ("last_page_server_at", last_page_server_at),
                ("available_at", available_at),
                ("raw_retained", 1),
            ):
                if name in snapshot_columns:
                    metadata[name] = value
            conn.execute(
                "UPDATE snapshot_runs SET "
                + ", ".join(f"{name} = ?" for name in metadata)
                + " WHERE snapshot_id = ?",
                (*metadata.values(), snapshot_id),
            )
            if self.post_snapshot_hook is not None:
                self.post_snapshot_hook(conn, snapshot_id)
            lease.heartbeat()
            final_metadata = {"status": "completed", "completed_at": commit_ts}
            if "available_at" in snapshot_columns:
                final_metadata["available_at"] = available_at
            if "raw_retained" in snapshot_columns:
                final_metadata["raw_retained"] = 1
            updated = conn.execute(
                "UPDATE snapshot_runs SET "
                + ", ".join(f"{name} = ?" for name in final_metadata)
                + " WHERE snapshot_id = ? AND status = 'running'",
                (*final_metadata.values(), snapshot_id),
            ).rowcount
            if updated != 1:
                raise ApiError("Snapshot no longer has running status")

    def _mark_failed(self, snapshot_id: int) -> None:
        with open_connection(self.db_path) as conn, write_transaction(conn):
            snapshot_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(snapshot_runs)")
            }
            retention_clause = (
                ", raw_retained = 0" if "raw_retained" in snapshot_columns else ""
            )
            conn.execute("DELETE FROM raw_listings WHERE snapshot_id = ?", (snapshot_id,))
            conn.execute(
                "UPDATE snapshot_runs SET status = 'failed', completed_at = ?, "
                "total_listings = 0, pages_fetched = 0, "
                "error_message = 'Official API snapshot collection failed'"
                f"{retention_clause} WHERE snapshot_id = ?",
                (int(self._clock()), snapshot_id),
            )

    def collect_once(self) -> SnapshotResult:
        """Fetch without a market-DB transaction, then persist everything atomically."""
        with self.limiter.collector_lock() as lease:
            snapshot_id = self._start_snapshot()
            try:
                listings, server_started_at, last_page_server_at, pages_fetched = self._fetch(
                    lease
                )
                lease.heartbeat()
                self._persist(
                    snapshot_id, listings, server_started_at,
                    last_page_server_at, pages_fetched, lease,
                )
            except BaseException as error:
                self._mark_failed(snapshot_id)
                if isinstance(error, (ApiError, CollectorBusyError, KeyboardInterrupt, SystemExit)):
                    raise
                # A hook or SQLite error may embed private data. Store and raise fixed text.
                raise ApiError("Official API snapshot collection failed") from None
            return SnapshotResult(snapshot_id, server_started_at, len(listings), pages_fetched)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
        if self._owns_limiter:
            self.limiter.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def collect_once(
    config: AppConfig, *, post_snapshot_hook: PostSnapshotHook | None = None
) -> SnapshotResult:
    """CLI/M3 integration entry point; live and sim share root/data/api_limiter.db."""
    with OfficialApiCollector(
        config.db_path, config.api_token, post_snapshot_hook=post_snapshot_hook
    ) as collector:
        return collector.collect_once()
