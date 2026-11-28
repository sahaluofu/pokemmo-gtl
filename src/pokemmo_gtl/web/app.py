"""Local, read-only market research API and adapters for the browser contracts."""

from __future__ import annotations

import asyncio
import csv
import io
import ipaddress
import json
import math
import re
import sqlite3
import statistics
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from decimal import Decimal
from http import HTTPStatus
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlsplit

from fastapi import FastAPI, File, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import Field
from starlette.exceptions import HTTPException

from pokemmo_gtl.config import AppConfig, Environment, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion import catalog_sync, manual_import, official_api, simulator
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError
from pokemmo_gtl.pipeline.aggregator import process_snapshot
from pokemmo_gtl.pipeline.retention import run_retention
from pokemmo_gtl.quant import backtester, fees, predictor
from pokemmo_gtl.web import schemas as s

STATIC_DIR = Path(__file__).resolve().parent / "static"
MAX_CSV_BYTES = s.MAX_CSV_BYTES
MAX_CSV_ROWS = s.MAX_CSV_ROWS
_LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_JOB_MUTEX = asyncio.Lock()
_JOB_GUARD = threading.Lock()
_ACTIVE_JOB: str | None = None
_QUOTE_LOCK = threading.Lock()
_CATALOG_COLUMNS = (
    "item_id", "name_en", "name_zh", "category", "asset_class",
    "max_stack_size", "description", "is_watched", "updated_at",
)
_BAR_PROJECTION = ", ".join(s.Bar.model_fields)
_EVENT_PROJECTION = ", ".join(s.MarketEvent.model_fields)
_PREDICTION_PROJECTION = (
    "prediction_id, generated_at, item_id, horizon_hours, model_name, "
    "current_price, ref_qty, prob_up, pred_q10, pred_q50, pred_q90, "
    "expected_net_roi, signal, regime_note, top_drivers_json"
)
_MANUAL_QUOTE_PROJECTION = (
    "quote_id, item_id, recorded_at, price, listed_qty, note, source_tag"
)
ItemId = Annotated[int, Field(gt=0, le=2**63 - 1)]
EventId = Annotated[int, Field(gt=0, le=2**63 - 1)]


def is_loopback_host(host: str) -> bool:
    """Recognize loopback bind addresses without performing DNS resolution."""
    if host.lower() == "localhost":
        return True
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _hostname(authority: str) -> str | None:
    if not authority or any(c.isspace() or c in "/\\?#@,%" for c in authority):
        return None
    if authority == "::1":
        return authority
    try:
        parsed = urlsplit("//" + authority)
        port = parsed.port
        if port is not None and not 0 < port <= 65535:
            return None
        return parsed.hostname
    except ValueError:
        return None


class SafeJSONResponse(JSONResponse):
    """Use strict JSON even for nested NumPy/Decimal non-finite measurements."""

    def render(self, content: Any) -> bytes:
        content = jsonable_encoder(s.sanitize_non_finite(content))
        return json.dumps(
            s.sanitize_non_finite(content), ensure_ascii=False, allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")


def _redact(value: Any, config: AppConfig) -> Any:
    if isinstance(value, str):
        if config.api_token:
            secret = config.api_token.get_secret_value()
            if secret:
                value = value.replace(secret, "[redacted]")
        for root in (str(config.root), config.root.as_posix()):
            value = value.replace(root, "[local data]")
        return value
    if isinstance(value, dict):
        return {_redact(k, config): _redact(v, config) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(v, config) for v in value]
    return value


def _error(
    config: AppConfig, status: int, code: str, message: str, details: Any = None,
    *, headers: dict[str, str] | None = None,
) -> SafeJSONResponse:
    envelope = s.ErrorEnvelope(error=s.ErrorBody(
        code=code, message=message, details=_redact(details, config),
    ))
    return SafeJSONResponse(envelope, status_code=status, headers=headers)


class APIError(HTTPException):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(status, detail=message)
        self.code = code


class _LocalSecurityMiddleware:
    """Check authorities before routing; normalize errors, including static errors."""

    def __init__(self, app, *, config: AppConfig, allow_lan: bool, hosts: set | None):
        self.app = app
        self.config = config
        self.allow_lan = allow_lan
        self.hosts = hosts

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = scope.get("headers", [])
        hosts = [v.decode("latin-1") for k, v in headers if k == b"host"]
        host = _hostname(hosts[0]) if len(hosts) == 1 else None
        if host is None or (self.hosts is not None and host not in self.hosts):
            await _error(self.config, 403, "FORBIDDEN_HOST", "Host is not allowed")(
                scope, receive, send,
            )
            return
        origins = [v.decode("latin-1") for k, v in headers if k == b"origin"]
        if scope["method"] in {"POST", "DELETE", "PUT", "PATCH"} and origins:
            valid = False
            if len(origins) == 1:
                try:
                    origin = urlsplit(origins[0])
                    origin_host = _hostname(origin.netloc)
                    valid = (
                        origin.scheme in {"http", "https"}
                        and not (origin.path or origin.query or origin.fragment)
                        and origin_host is not None
                        and (
                            origin_host in _LOCAL_HOSTS
                            or (self.allow_lan and origin_host == host)
                        )
                        and (self.hosts is None or origin_host in self.hosts)
                    )
                except ValueError:
                    pass
            if not valid:
                await _error(self.config, 403, "FORBIDDEN_ORIGIN", "Origin is not allowed")(
                    scope, receive, send,
                )
                return

        started = False
        error_start = None
        error_body = bytearray()

        async def guarded_send(message):
            nonlocal started, error_start
            if message["type"] == "http.response.start":
                if not 200 <= message["status"] < 300:
                    error_start = message
                    return
                started = True
            if error_start is not None and message["type"] == "http.response.body":
                # Our handlers already produced safe envelopes. Other non-2xx
                # responses (e.g. static redirects) get a generic envelope.
                if len(error_body) < 65536:
                    error_body.extend(message.get("body", b"")[:65536 - len(error_body)])
                if not message.get("more_body", False):
                    status = error_start["status"]
                    try:
                        payload = s.ErrorEnvelope.model_validate_json(bytes(error_body))
                    except ValueError:
                        payload = s.ErrorEnvelope(error=s.ErrorBody(
                            code=f"HTTP_{status}", message="Request was not successful",
                        ))
                    response = SafeJSONResponse(
                        _redact(payload.model_dump(), self.config), status_code=status,
                    )
                    # Keep protocol metadata such as Allow/Location, never stale lengths.
                    response.raw_headers.extend(
                        (k, v) for k, v in error_start.get("headers", [])
                        if k not in {b"content-length", b"content-type"}
                    )
                    started = True
                    await response(scope, receive, send)
                return
            await send(message)

        try:
            await self.app(scope, receive, guarded_send)
        except Exception:
            if started:
                raise
            await _error(
                self.config, 500, "INTERNAL_ERROR", "An internal server error occurred",
            )(scope, receive, send)


def _release_job() -> None:
    global _ACTIVE_JOB
    with _JOB_GUARD:
        _ACTIVE_JOB = None
        _JOB_MUTEX.release()


def _finish_cancelled_job(task: asyncio.Task) -> None:
    # Cancelling an HTTP request must not unlock a still-running worker thread.
    if not task.cancelled():
        task.exception()
    _release_job()


async def _run_job(name: str, function: Callable, *args, **kwargs):
    global _ACTIVE_JOB
    # The guard also serializes factories served on different event-loop threads.
    with _JOB_GUARD:
        if _JOB_MUTEX.locked():
            raise APIError(409, "JOB_BUSY", "Another job is already running")
        await _JOB_MUTEX.acquire()  # Uncontended acquisition never blocks/yields.
        _ACTIVE_JOB = name
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        result = await asyncio.shield(task)
    except asyncio.CancelledError:
        task.add_done_callback(_finish_cancelled_job)
        raise
    except BaseException:
        _release_job()
        raise
    else:
        _release_job()
        return result


def _require_item(conn: sqlite3.Connection, item_id: int) -> sqlite3.Row:
    row = conn.execute(
        f"SELECT {', '.join(_CATALOG_COLUMNS)} FROM items_catalog WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    if row is None:
        raise APIError(404, "ITEM_NOT_FOUND", "Item not found")
    return row


def _percent_change(current, baseline) -> float | None:
    if current is None or baseline is None or baseline <= 0:
        return None
    result = (current / baseline - 1) * 100
    return result if math.isfinite(result) else None


def _item_rows(
    conn: sqlite3.Connection, *, horizon: int | None = None, item_id: int | None = None,
) -> list[dict[str, Any]]:
    prediction_filter = " AND horizon_hours = ?" if horizon is not None else ""
    item_filter = " WHERE c.item_id = ?" if item_id is not None else ""
    params = ([horizon] if horizon is not None else [])
    if item_id is not None:
        params.append(item_id)
    catalog_projection = ", ".join(f"c.{column}" for column in _CATALOG_COLUMNS)
    has_aliases = any(
        row["name"] == "aliases_json"
        for row in conn.execute("PRAGMA table_info(items_catalog)")
    )
    alias_projection = ", c.aliases_json" if has_aliases else ""
    rows = conn.execute(
        f"""SELECT {catalog_projection}{alias_projection},
           CASE WHEN COALESCE(b.book_state, 'stocked') = 'stocked'
                THEN b.close_price ELSE NULL END AS current_price,
           b.book_state, b.close_ts AS bar_close_ts,
           b.total_listed_qty, b.depth_within_5pct,
           old.close_price AS previous_price, old.depth_within_5pct AS previous_depth,
           p.signal, p.prob_up, p.expected_net_roi
           FROM items_catalog AS c
           LEFT JOIN item_bars AS b ON b.item_id = c.item_id AND b.timeframe = '4h'
             AND b.bucket_ts = (SELECT bucket_ts FROM item_bars
                 WHERE item_id = c.item_id AND timeframe = '4h'
                 ORDER BY close_ts DESC LIMIT 1)
           LEFT JOIN item_bars AS old ON old.item_id = c.item_id AND old.timeframe = '4h'
             AND old.bucket_ts = (SELECT bucket_ts FROM item_bars
                 WHERE item_id = c.item_id AND timeframe = '4h' AND close_ts <= b.close_ts-86400
                 ORDER BY close_ts DESC LIMIT 1)
           LEFT JOIN predictions AS p ON p.prediction_id = (
                 SELECT prediction_id FROM predictions WHERE item_id = c.item_id"""
        + prediction_filter
        + " ORDER BY generated_at DESC, prediction_id DESC LIMIT 1)"
        + item_filter + " ORDER BY c.item_id",
        params,
    ).fetchall()
    output = []
    for row in rows:
        values = dict(row)
        values["is_watched"] = bool(values["is_watched"])
        values["change_24h_pct"] = _percent_change(
            values["current_price"], values["previous_price"],
        )
        values["depth_change_24h_pct"] = _percent_change(
            values["depth_within_5pct"], values["previous_depth"],
        )
        output.append(s.sanitize_non_finite(values))
    return output


def _summary(values: dict) -> s.ItemSummary:
    return s.ItemSummary.model_validate({k: v for k, v in values.items()
                                         if k in s.ItemSummary.model_fields})


def _events(conn: sqlite3.Connection, start=None, end=None) -> list[s.MarketEvent]:
    query = f"SELECT {_EVENT_PROJECTION} FROM market_events"
    params = ()
    if start is not None and end is not None:
        query += " WHERE end_ts >= ? AND start_ts <= ?"
        params = (start, end)
    return [s.MarketEvent.model_validate(dict(row)) for row in conn.execute(
        query + " ORDER BY start_ts, event_id", params,
    )]


def _overview_rp_index(
    conn: sqlite3.Connection, config: AppConfig, *, reference_ts: int,
) -> s.RPIndex | None:
    """Median fresh stocked voucher quote in yen/RP, with empty-book barriers.

    The latest observation is selected before its state and age are checked.
    A mixed or empty observation therefore cannot fall back to a previously
    stocked quote. The 24h baseline uses the same rule at reference_ts - 86400.
    """
    relations = (
        config.item_relations.get("rp_vouchers", [])
        if isinstance(config.item_relations, Mapping) else []
    )
    if isinstance(relations, Mapping):
        relations = [relations]
    max_age = int(getattr(
        config.settings, "max_rp_quote_age_sec",
        getattr(config, "max_rp_quote_age_sec", 86400),
    ))
    current_values = []
    previous_values = []
    current_timestamps = []
    observed_timestamps = []
    for relation in relations:
        if not isinstance(relation, Mapping):
            continue
        try:
            voucher_id = int(relation["item_id"])
            denomination = float(relation["denomination"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if voucher_id <= 0 or not math.isfinite(denomination) or denomination <= 0:
            continue
        for timestamp, values, is_current in (
            (reference_ts, current_values, True),
            (reference_ts - 86400, previous_values, False),
        ):
            row = conn.execute(
                "SELECT close_price, close_ts, "
                "COALESCE(book_state, 'stocked') AS book_state "
                "FROM item_bars WHERE item_id = ? AND timeframe = '4h' AND close_ts <= ? "
                "ORDER BY close_ts DESC LIMIT 1",
                (voucher_id, timestamp),
            ).fetchone()
            if row is None:
                continue
            if is_current:
                observed_timestamps.append(row["close_ts"])
            age = timestamp - row["close_ts"]
            if row["book_state"] != "stocked" or not 0 <= age <= max_age:
                continue
            price = row["close_price"]
            if price is None or not math.isfinite(price) or price <= 0:
                continue
            yen_per_rp = price / denomination
            if not math.isfinite(yen_per_rp) or yen_per_rp <= 0:
                continue
            values.append(yen_per_rp)
            if is_current:
                current_timestamps.append(row["close_ts"])
    if not observed_timestamps:
        return None
    value = statistics.median(current_values) if current_values else None
    baseline = statistics.median(previous_values) if previous_values else None
    change = _percent_change(value, baseline)
    level = None if change is None else (
        "low" if change < -5 else "high" if change >= 10 else
        "elevated" if change >= 3 else "normal"
    )
    return s.RPIndex(
        value=value, change_24h_pct=change, inflation_level=level,
        updated_at=max(current_timestamps or observed_timestamps),
    )


def _rate_budget(config: AppConfig) -> int | None:
    path = config.root / "data" / "api_limiter.db"
    budget = config.settings.rate_limit_tokens
    if not path.is_file():
        return budget
    try:
        # Health never initializes, mutates or sleeps on the shared limiter.
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        try:
            used = conn.execute(
                "SELECT COALESCE(SUM(tokens), 0) FROM api_rate_ledger WHERE req_ts > ?",
                (time.time() - config.settings.rate_limit_window_sec,),
            ).fetchone()[0]
        finally:
            conn.close()
        return max(0, budget - used)
    except sqlite3.Error:
        return None  # An unreadable ledger means the available budget is unknown.


def _drivers(raw: str) -> list[s.Driver]:
    try:
        values = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(values, list):
        return []
    drivers = []
    for value in values:
        if not isinstance(value, dict) or not value.get("feature"):
            continue
        value = s.sanitize_non_finite(value)
        contribution = value.get("contribution")
        direction = value.get("direction")
        if direction is None:
            direction = ("positive" if contribution > 0 else "negative"
                         if contribution < 0 else "neutral") if contribution is not None else None
        drivers.append(s.Driver(
            feature=value["feature"], contribution=contribution,
            value=value.get("value"), direction=direction,
        ))
        if len(drivers) == 5:
            break
    return drivers


def _sharpe(points: list[dict]) -> float | None:
    returns = []
    for previous, current in zip(points, points[1:]):
        a, b = previous["equity"], current["equity"]
        if a is not None and b is not None and math.isfinite(a) and math.isfinite(b) and a > 0:
            change = b / a - 1
            if math.isfinite(change):
                returns.append(change)
    if len(returns) < 2:
        return None
    deviation = statistics.stdev(returns)
    if deviation == 0:
        return None
    return statistics.mean(returns) / deviation * math.sqrt(365 * 6)


def _backtest_response(result: dict) -> s.BacktestResponse:
    curves = result["equity_curves"]
    comparison = []
    for row in result["comparison_table"]:
        name = row["model_name"]
        comparison.append(s.BacktestComparison(
            model_name="lightgbm" if name == "lgbm" else name,
            metrics=s.BacktestMetrics(
                total_return=(row["net_return_pct"] / 100
                              if row.get("net_return_pct") is not None else None),
                max_drawdown=(row["max_drawdown_pct"] / 100
                              if row.get("max_drawdown_pct") is not None else None),
                sharpe_ratio=_sharpe(curves.get(name, [])),
                win_rate=row.get("win_rate"), trade_count=row.get("trade_count"),
                pinball_loss=row.get("pinball_mean"), roc_auc=row.get("roc_auc"),
                interval_coverage=row.get("coverage_80"),
            ),
        ))
    return s.BacktestResponse(
        run_id=result["run_id"], comparison_table=comparison,
        equity_curves=[s.EquityCurve(
            model_name="lightgbm" if name == "lgbm" else name,
            points=[s.EquityPoint(ts=p["close_ts"], equity=p["equity"]) for p in points],
        ) for name, points in curves.items()],
    )


def _admin_action(config: AppConfig, action: str) -> None:
    if action == "simulate":
        result = simulator.simulate(
            config.db_path, env=config.env, days=90, seed=42,
            on_snapshot=lambda conn, sid, _p, _t: process_snapshot(conn, sid, config.settings),
        )
        with open_connection(config.db_path) as conn:
            run_retention(conn, config.settings, now_ts=result.end_ts)
    elif action == "collect":
        official_api.collect_once(
            config,
            post_snapshot_hook=lambda conn, sid: process_snapshot(conn, sid, config.settings),
        )
        with open_connection(config.db_path) as conn:
            run_retention(conn, config.settings)
    elif action == "sync_catalog":
        catalog_sync.sync_catalog(config.db_path, config=config)
    elif action == "forecast":
        predictor.run_forecast(config)


def escape_csv_cell(value: Any) -> Any:
    """Escape every free-text column, including tabs and carriage returns."""
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@", "\t", "\r")):
        return "'" + value
    return value


def create_app(
    env: Environment | str = Environment.live, *, config: AppConfig | None = None,
    allow_lan: bool = False, allowed_hosts: Iterable[str] | None = None,
) -> FastAPI:
    config = config if config is not None else load_config(env)
    hosts = None if allow_lan else set(_LOCAL_HOSTS)
    if allowed_hosts is not None:
        explicit = {_hostname(host) for host in allowed_hosts}
        if None in explicit or "*" in explicit:
            raise ValueError("allowed_hosts must contain explicit valid hostnames")
        hosts = explicit if allow_lan else explicit & _LOCAL_HOSTS
    migrate(config.db_path)
    app = FastAPI(
        title="PokeMMO GTL Market Research", default_response_class=SafeJSONResponse,
        redirect_slashes=False,
    )
    app.state.config = config
    app.add_middleware(_LocalSecurityMiddleware, config=config, allow_lan=allow_lan, hosts=hosts)

    @app.exception_handler(HTTPException)
    async def http_error(_request: Request, exc: HTTPException):
        if isinstance(exc, APIError):
            return _error(config, exc.status_code, exc.code, exc.detail)
        try:
            message = HTTPStatus(exc.status_code).phrase
        except ValueError:
            message = "Request failed"
        return _error(config, exc.status_code, f"HTTP_{exc.status_code}", message,
                      headers=exc.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        # Do not echo input/ctx: these can contain credentials, paths or exceptions.
        details = [{"loc": e["loc"], "type": e["type"]} for e in exc.errors()]
        return _error(config, 422, "VALIDATION_ERROR", "Request validation failed", details)

    @app.exception_handler(Exception)
    async def unexpected_error(_request: Request, _exc: Exception):
        return _error(config, 500, "INTERNAL_ERROR", "An internal server error occurred")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(STATIC_DIR / "index.html", media_type="text/html")

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/api/health", response_model=s.HealthResponse)
    def health():
        with open_connection(config.db_path) as conn:
            last = conn.execute(
                "SELECT MAX(completed_at) FROM snapshot_runs WHERE status = 'completed'",
            ).fetchone()[0]
            items = conn.execute("SELECT COUNT(*) FROM items_catalog").fetchone()[0]
            bars = conn.execute("SELECT COUNT(*) FROM item_bars").fetchone()[0]
        return s.HealthResponse(
            env=config.env.value, db_path=str(config.db_path), last_snapshot_at=last,
            token_configured=bool(config.api_token and config.api_token.get_secret_value()),
            rate_budget_remaining=_rate_budget(config), active_job=_ACTIVE_JOB,
            item_count=items, bar_count=bars,
        )

    @app.get("/api/overview", response_model=s.OverviewResponse)
    def overview(query: Annotated[s.OverviewQuery, Query()]):
        with open_connection(config.db_path) as conn:
            rows = _item_rows(conn, horizon=query.horizon)
            anchor = conn.execute(
                "SELECT MAX(ts) FROM (SELECT MAX(close_ts) AS ts FROM item_bars UNION ALL "
                "SELECT MAX(completed_at) FROM snapshot_runs WHERE status = 'completed')",
            ).fetchone()[0]
            events = _events(conn)
            reference_ts = anchor if anchor is not None else int(time.time())
            if config.env == Environment.live:
                reference_ts = max(reference_ts, int(time.time()))
            rp_index = _overview_rp_index(conn, config, reference_ts=reference_ts)
        items = [_summary(row) for row in rows]
        bullish = sorted(
            (item for item in items if item.expected_net_roi is not None),
            key=lambda item: (item.expected_net_roi,
                              item.prob_up if item.prob_up is not None else -1), reverse=True,
        )[:10]
        thinning = sorted(
            (item for item in items if item.depth_change_24h_pct is not None
             and item.depth_change_24h_pct < 0), key=lambda item: item.depth_change_24h_pct,
        )[:10]
        movers = sorted(
            (item for item in items if item.change_24h_pct is not None),
            key=lambda item: abs(item.change_24h_pct), reverse=True,
        )[:10]
        return s.OverviewResponse(
            rp_index=rp_index,
            next_event=next((e for e in events if e.end_ts >= (
                anchor if anchor is not None else int(time.time())
            )), None),
            top_bullish=bullish, top_thinning_depth=thinning,
            top_movers_24h=movers, watchlist=[item for item in items if item.is_watched],
        )

    @app.get("/api/items", response_model=s.ItemListResponse)
    def items(query: Annotated[s.ItemsQuery, Query()]):
        with open_connection(config.db_path) as conn:
            rows = _item_rows(conn)
        aliases = {entry["item_id"]: entry for entry in config.item_aliases}
        output = []
        for row in rows:
            haystack = " ".join(str(value or "") for value in (
                row["item_id"], row["name_en"], row["name_zh"], row.get("aliases_json"),
                aliases.get(row["item_id"], {}).get("aliases_json"),
                aliases.get(row["item_id"], {}).get("aliases"),
                aliases.get(row["item_id"], {}).get("name_zh"),
            )).casefold()
            if query.q.casefold() not in haystack:
                continue
            if query.category and row["category"] != query.category:
                continue
            if query.watched_only and not row["is_watched"]:
                continue
            output.append(_summary(row))
            if len(output) == query.limit:
                break
        return s.ItemListResponse(output)

    @app.post("/api/items/{item_id}/watch", response_model=s.WatchResponse)
    def watch(item_id: ItemId, request: s.WatchRequest):
        with open_connection(config.db_path) as conn, write_transaction(conn):
            _require_item(conn, item_id)
            conn.execute("UPDATE items_catalog SET is_watched = ? WHERE item_id = ?",
                         (int(request.is_watched), item_id))
        return s.WatchResponse(item_id=item_id, is_watched=request.is_watched)

    @app.get("/api/items/{item_id}/bars", response_model=s.BarsResponse)
    def bars(item_id: ItemId, query: Annotated[s.BarsQuery, Query()]):
        with open_connection(config.db_path) as conn:
            _require_item(conn, item_id)
            latest = conn.execute(
                "SELECT MAX(close_ts) FROM item_bars WHERE item_id = ? AND timeframe = ?",
                (item_id, query.timeframe),
            ).fetchone()[0]
            cutoff = max(0, latest - query.days * 86400) if latest and query.days else 0
            rows = conn.execute(
                f"SELECT {_BAR_PROJECTION} FROM item_bars "
                "WHERE item_id = ? AND timeframe = ? AND close_ts >= ? ORDER BY bucket_ts",
                (item_id, query.timeframe, cutoff),
            ).fetchall()
            events = _events(conn, rows[0]["bucket_ts"], rows[-1]["close_ts"]) if rows else (
                _events(conn)
            )
            item = _summary(_item_rows(conn, item_id=item_id)[0])
        return s.BarsResponse(item=item, bars=[dict(row) for row in rows], events=events)

    @app.get("/api/items/{item_id}/forecast", response_model=s.ForecastResponse)
    def forecast(item_id: ItemId, query: Annotated[s.ForecastQuery, Query()]):
        forecasts = []
        with open_connection(config.db_path) as conn:
            _require_item(conn, item_id)
            item = _summary(_item_rows(conn, item_id=item_id)[0])
            for horizon in query.horizon_hours:
                row = conn.execute(
                    f"SELECT {_PREDICTION_PROJECTION} FROM predictions "
                    "WHERE item_id = ? AND horizon_hours = ? "
                    "ORDER BY generated_at DESC, prediction_id DESC LIMIT 1",
                    (item_id, horizon),
                ).fetchone()
                if row is not None:
                    values = dict(row)
                    values["top_drivers"] = _drivers(values.pop("top_drivers_json"))
                    forecasts.append(s.Forecast.model_validate(values))
        return s.ForecastResponse(item=item, forecasts=forecasts)

    @app.post("/api/fees/simulate", response_model=s.FeeSimulationResponse)
    def simulate_fees(request: s.FeeSimulationRequest):
        item = None
        if request.item_id is not None:
            with open_connection(config.db_path) as conn:
                item = _require_item(conn, request.item_id)
        asset = request.asset_class or (item["asset_class"] if item else "item")
        stack = request.max_stack_size or (item["max_stack_size"] if item else (
            1 if asset in {"cosmetic", "pokemon_category"} else 99
        ))
        pokemon = asset == "pokemon_category" or (request.item_id or 0) >= 900000
        kwargs = {
            "min_fee": config.fees.pokemon_min_fee if pokemon else config.fees.item_min_fee,
            "rate": config.fees.rate, "max_fee_per_slot": config.fees.max_fee_per_slot,
        }
        relist = (request.relist_rate if request.relist_rate is not None
                  else config.fees.default_relist_rate)
        fee = fees.listing_fee(request.sell_price, request.qty, stack, **kwargs)
        effective = fees.effective_fee(
            request.sell_price, request.qty, stack, relist_rate=relist, **kwargs,
        )
        profit = fees.net_profit(
            request.buy_price, request.sell_price, request.qty, stack, relist_rate=relist, **kwargs,
        )
        breakeven = fees.breakeven_sell_price(
            request.buy_price, request.qty, stack, relist_rate=relist, **kwargs,
        )
        return s.FeeSimulationResponse(
            slots_used=(request.qty + stack - 1) // stack,
            listing_fee=fee, effective_fee=effective, net_profit=profit,
            net_roi=profit / (Decimal(request.buy_price) * Decimal(request.qty))
            if request.buy_price else None, breakeven_sell_price=max(1, breakeven),
        )

    @app.post("/api/backtest/run", response_model=s.BacktestResponse)
    async def backtest(request: s.BacktestRequest):
        if request.item_id is not None:
            with open_connection(config.db_path) as conn:
                _require_item(conn, request.item_id)
        result = await _run_job(
            "backtest", backtester.run_backtest, config, horizon_hours=request.horizon_hours,
            models=["lgbm" if name == "lightgbm" else name for name in request.models],
            item_id=request.item_id, category=request.category,
        )
        return _backtest_response(result)

    @app.post("/api/quotes/manual", response_model=s.ManualQuoteResponse)
    def manual_quote(request: s.ManualQuoteRequest):
        timestamp = request.recorded_at if request.recorded_at is not None else int(time.time())
        with _QUOTE_LOCK:
            with open_connection(config.db_path) as conn:
                _require_item(conn, request.item_id)
            try:
                result = manual_import.add_manual_quote(
                    config.db_path, request.item_id, request.price, recorded_at=timestamp,
                    listed_qty=request.listed_qty, note=request.note,
                )
            except ValueError as exc:
                raise APIError(400, "INVALID_QUOTE", "Quote cannot be imported") from exc
            with open_connection(config.db_path) as conn:
                row = conn.execute(
                    f"SELECT {_MANUAL_QUOTE_PROJECTION} FROM manual_quotes "
                    "WHERE item_id = ? AND recorded_at = ? "
                    "AND price = ? AND listed_qty IS ? AND note IS ? "
                    "ORDER BY quote_id DESC LIMIT 1",
                    (request.item_id, timestamp, request.price, request.listed_qty, request.note),
                ).fetchone()
        return s.ManualQuoteResponse(
            **dict(row), skipped_snapshot_bars=result.skipped_snapshot_bars,
        )

    @app.post("/api/quotes/import-csv", response_model=s.CSVImportResponse)
    async def import_csv(file: UploadFile = File(...)):
        path = None
        try:
            if file.size is not None and file.size > MAX_CSV_BYTES:
                raise APIError(413, "CSV_TOO_LARGE", "CSV exceeds the 5 MiB limit")
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temporary:
                path = Path(temporary.name)
                consumed = 0
                while chunk := await file.read(min(65536, MAX_CSV_BYTES - consumed + 1)):
                    consumed += len(chunk)
                    if consumed > MAX_CSV_BYTES:
                        raise APIError(413, "CSV_TOO_LARGE", "CSV exceeds the 5 MiB limit")
                    temporary.write(chunk)

            def perform_import():
                with _QUOTE_LOCK:
                    return manual_import.import_csv(
                        config.db_path, path, max_bytes=MAX_CSV_BYTES, max_rows=MAX_CSV_ROWS,
                    )

            try:
                result = await asyncio.to_thread(perform_import)
            except ValueError as exc:
                raise APIError(
                    400, "INVALID_CSV", "Invalid CSV structure, encoding or import limits",
                ) from exc
            errors = []
            for issue in result.errors:
                match = re.fullmatch(r"row (\d+):\s*(.+)", issue, flags=re.DOTALL)
                errors.append(s.CSVImportError(
                    row=int(match[1]) if match else None,
                    message=_redact(match[2] if match else issue, config),
                ))
            return s.CSVImportResponse(
                imported_rows=result.imported_rows,
                skipped_snapshot_bars=result.skipped_snapshot_bars, errors=errors,
            )
        finally:
            await file.close()
            if path is not None:
                path.unlink(missing_ok=True)

    @app.get("/api/items/{item_id}/export.csv")
    def export_csv(item_id: ItemId, query: Annotated[s.CSVExportQuery, Query()]):
        with open_connection(config.db_path) as conn:
            item = dict(_require_item(conn, item_id))
            bars = conn.execute(
                f"SELECT {_BAR_PROJECTION} FROM item_bars "
                "WHERE item_id = ? AND timeframe = ? ORDER BY bucket_ts",
                (item_id, query.timeframe),
            ).fetchall()
        columns = ["name_en", "name_zh", "category", "asset_class", "max_stack_size", "description"]
        bar_columns = list(s.Bar.model_fields)
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(columns + bar_columns)
        for bar in bars:
            values = [item[column] for column in columns] + [bar[column] for column in bar_columns]
            writer.writerow([escape_csv_cell(value) for value in values])
        return Response(
            output.getvalue(), media_type=s.CSV_MEDIA_TYPE,
            headers={"Content-Disposition": (
                f'attachment; filename="item-{item_id}-{query.timeframe}.csv"'
            )},
        )

    @app.get("/api/events", response_model=s.EventListResponse)
    def events(query: Annotated[s.EventsQuery, Query()]):
        with open_connection(config.db_path) as conn:
            return s.EventListResponse(_events(conn))

    @app.post("/api/events", response_model=s.EventCreateResponse)
    def create_event(request: s.EventCreateRequest):
        values = request.model_dump()
        with open_connection(config.db_path) as conn, write_transaction(conn):
            cursor = conn.execute(
                "INSERT INTO market_events "
                "(name, event_type, announced_at, start_ts, end_ts, impact_scope, notes) "
                "VALUES (:name, :event_type, :announced_at, :start_ts, :end_ts, "
                ":impact_scope, :notes)",
                values,
            )
            event_id = cursor.lastrowid
        return s.EventCreateResponse(event_id=event_id, **values)

    @app.delete("/api/events/{event_id}", response_model=s.EventDeleteResponse)
    def delete_event(event_id: EventId):
        with open_connection(config.db_path) as conn, write_transaction(conn):
            cursor = conn.execute("DELETE FROM market_events WHERE event_id = ?", (event_id,))
            if not cursor.rowcount:
                raise APIError(404, "EVENT_NOT_FOUND", "Event not found")
        return s.EventDeleteResponse(event_id=event_id, deleted=True)

    @app.post("/api/admin/trigger", response_model=s.AdminTriggerResponse)
    async def trigger(request: s.AdminTriggerRequest):
        if config.env == Environment.live and request.action == "simulate":
            raise APIError(
                403, "FORBIDDEN_SIMULATION_IN_LIVE",
                "Simulation is forbidden in the live environment",
            )
        try:
            await _run_job(request.action, _admin_action, config, request.action)
        except official_api.ApiAuthError as exc:
            raise APIError(401, "API_AUTH_ERROR", "Official API authentication failed") from exc
        except CollectorBusyError as exc:
            raise APIError(
                409, "COLLECTOR_BUSY", "An official API collector is already running",
            ) from exc
        except official_api.ApiError as exc:
            raise APIError(502, "UPSTREAM_ERROR", "Official API collection failed") from exc
        return s.AdminTriggerResponse(action=request.action, status="completed", active_job=None)

    return app
