"""Command-line interface for local PokeMMO GTL market research."""

import time
from collections import Counter
from pathlib import Path
from typing import Annotated

import typer

from pokemmo_gtl.config import Environment, load_config
from pokemmo_gtl.db.connection import open_connection
from pokemmo_gtl.db.repository import migrate
from pokemmo_gtl.ingestion.catalog_sync import sync_catalog
from pokemmo_gtl.ingestion.manual_import import import_csv
from pokemmo_gtl.ingestion.official_api import ApiAuthError, ApiError, collect_once
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError
from pokemmo_gtl.ingestion.simulator import simulate
from pokemmo_gtl.pipeline.aggregator import process_snapshot
from pokemmo_gtl.pipeline.retention import run_retention
from pokemmo_gtl.quant.backtester import run_backtest
from pokemmo_gtl.quant.predictor import run_forecast

app = typer.Typer(no_args_is_help=True)


@app.callback()
def main() -> None:
    """PokeMMO GTL read-only market research."""


@app.command("init-db")
def init_db(env: Annotated[Environment, typer.Option("--env")] = Environment.live) -> None:
    """Create the selected STRICT database and apply pending migrations."""
    config = load_config(env)
    applied = migrate(config.db_path)
    typer.echo(
        f"Initialized env={env.value} db={config.db_path} migrations_applied={len(applied)}"
    )


@app.command("sync-catalog")
def sync_catalog_command(
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    offline_only: Annotated[bool, typer.Option("--offline-only")] = False,
    from_local: Annotated[Path | None, typer.Option("--from-local")] = None,
) -> None:
    """Load seed data and synchronize the item dictionary and market events."""
    config = load_config(env)
    migrate(config.db_path)
    count = sync_catalog(
        config.db_path, config=config, offline_only=offline_only, from_local=from_local
    )
    typer.echo(f"Catalog synchronized env={env.value} items={count}")


@app.command("import-csv")
def import_csv_command(
    csv_path: Annotated[Path, typer.Argument(exists=True, dir_okay=False)],
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
) -> None:
    """Import bounded UTF-8 manual quotes into the selected database."""
    config = load_config(env)
    migrate(config.db_path)
    result = import_csv(config.db_path, csv_path)
    typer.echo(
        f"Imported env={env.value} rows={result.imported_rows} "
        f"skipped_snapshot_bars={result.skipped_snapshot_bars} errors={len(result.errors)}"
    )
    for error in result.errors:
        typer.echo(error, err=True)


@app.command("simulate")
def simulate_command(
    days: Annotated[int, typer.Option("--days", min=1)] = 90,
    seed: Annotated[int, typer.Option("--seed")] = 42,
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    end_ts: Annotated[int | None, typer.Option("--end-ts", min=1)] = None,
) -> None:
    """Generate a deterministic local market snapshot stream (sim environment only)."""
    if env is Environment.live:
        raise typer.BadParameter("simulate requires --env sim")
    config = load_config(env)
    result = simulate(
        config.db_path,
        env=env,
        days=days,
        seed=seed,
        end_ts=end_ts,
        on_snapshot=lambda conn, snapshot_id, _prev, _ts: process_snapshot(
            conn, snapshot_id, config.settings
        ),
    )
    with open_connection(config.db_path) as conn:
        deleted = run_retention(conn, config.settings, now_ts=result.end_ts)
    typer.echo(
        f"Simulation complete env={env.value} snapshots={result.snapshots} "
        f"listings={result.listings} start_ts={result.start_ts} end_ts={result.end_ts} "
        f"raw_listings_pruned={deleted}"
    )


@app.command("collect")
def collect_command(
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    once: Annotated[bool, typer.Option("--once")] = False,
    daemon: Annotated[bool, typer.Option("--daemon")] = False,
    interval: Annotated[float, typer.Option("--interval", min=0)] = 900,
    max_cycles: Annotated[int | None, typer.Option("--max-cycles", min=1)] = None,
) -> None:
    """Collect official snapshots once or on a fixed start-to-start cadence."""
    if once and daemon:
        raise typer.BadParameter("--once and --daemon cannot be used together")
    is_daemon = daemon
    config = load_config(env)
    migrate(config.db_path)

    def post_snapshot_hook(conn, snapshot_id):
        process_snapshot(conn, snapshot_id, config.settings)

    cycle = 0
    try:
        while max_cycles is None or cycle < max_cycles:
            cycle_start = time.monotonic()
            cycle += 1
            try:
                result = collect_once(config, post_snapshot_hook=post_snapshot_hook)
            except ApiAuthError:
                typer.echo("Authentication failed. Check POKEMMO_API_TOKEN.", err=True)
                raise typer.Exit(code=1) from None
            except CollectorBusyError as error:
                if not is_daemon:
                    typer.echo(f"Collector busy: {error}", err=True)
                    raise typer.Exit(code=1) from None
                typer.echo(f"Warning: collector busy: {error}", err=True)
            except ApiError as error:
                if not is_daemon:
                    typer.echo(f"Collection failed: {error}", err=True)
                    raise typer.Exit(code=1) from None
                typer.echo(f"Warning: collection failed: {error}", err=True)
            else:
                with open_connection(config.db_path) as conn:
                    deleted = run_retention(conn, config.settings)
                typer.echo(
                    f"Collected snapshot_id={result.snapshot_id} "
                    f"listings={result.total_listings} raw_listings_pruned={deleted}"
                )

            if not is_daemon or (max_cycles is not None and cycle >= max_cycles):
                break
            time.sleep(max(0.0, interval - (time.monotonic() - cycle_start)))
    except KeyboardInterrupt:
        typer.echo("Collector stopped.")


def _parse_integer_list(value: str, option_name: str) -> tuple[int, ...]:
    try:
        parts = [part.strip() for part in value.split(",")]
        if not parts or any(not part for part in parts):
            raise ValueError
        values = tuple(dict.fromkeys(int(part) for part in parts))
    except ValueError:
        raise typer.BadParameter(f"{option_name} must be comma-separated integers") from None
    if any(value <= 0 for value in values):
        raise typer.BadParameter(f"{option_name} values must be positive")
    return values


@app.command("forecast")
def forecast_command(
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    horizons: Annotated[str, typer.Option("--horizons")] = "24,72,168",
    item_ids: Annotated[list[int] | None, typer.Option("--item-id")] = None,
) -> None:
    """Generate and persist causal forecasts for the selected horizons."""
    config = load_config(env)
    parsed_horizons = _parse_integer_list(horizons, "--horizons")
    records = run_forecast(config, horizons=parsed_horizons, item_ids=item_ids or None)
    signals = Counter(record["signal"] for record in records)
    breakdown = ", ".join(f"{signal}={signals[signal]}" for signal in sorted(signals))
    typer.echo(
        f"Forecasts generated={len(records)} signals={breakdown or 'none'}"
    )


@app.command("backtest")
def backtest_command(
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    horizon: Annotated[int, typer.Option("--horizon", min=1)] = 72,
    models: Annotated[str, typer.Option("--models")] = "naive,statistical,lgbm",
    item_id: Annotated[int | None, typer.Option("--item-id", min=1)] = None,
    category: Annotated[str | None, typer.Option("--category")] = None,
) -> None:
    """Persist walk-forward backtests and print the model comparison metrics."""
    config = load_config(env)
    model_parts = [part.strip().lower() for part in models.split(",")]
    if not model_parts or any(not model for model in model_parts):
        raise typer.BadParameter("--models must be comma-separated model names")
    model_aliases = {"lightgbm": "lgbm"}
    parsed_models = tuple(dict.fromkeys(model_aliases.get(model, model) for model in model_parts))
    allowed_models = {"naive", "statistical", "lgbm"}
    unknown = sorted(set(parsed_models) - allowed_models)
    if unknown:
        raise typer.BadParameter(f"unsupported model(s): {', '.join(unknown)}")

    result = run_backtest(
        config,
        horizon_hours=horizon,
        models=parsed_models,
        item_id=item_id,
        category=category,
    )
    typer.echo(
        f"Backtest horizon_hours={horizon} item_scope={result['item_scope']} "
        f"models={len(result['comparison_table'])}"
    )
    for metrics in result["comparison_table"]:
        typer.echo(
            f"{metrics['model_name']}: ROC-AUC={_format_metric(metrics.get('roc_auc'), 4)} "
            f"pinball_mean={_format_metric(metrics.get('pinball_mean'), 6)} "
            f"net_return_pct={_format_metric(metrics.get('net_return_pct'), 2)} "
            f"max_drawdown_pct={_format_metric(metrics.get('max_drawdown_pct'), 2)} "
            f"trade_count={metrics.get('trade_count', 0)}"
        )


def _format_metric(value: object, digits: int) -> str:
    return "n/a" if value is None else f"{float(value):.{digits}f}"


@app.command("serve")
def serve_command(
    env: Annotated[Environment, typer.Option("--env")] = Environment.live,
    host: Annotated[str, typer.Option("--host")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", min=1, max=65535)] = 8000,
    allow_lan: Annotated[bool, typer.Option("--allow-lan")] = False,
) -> None:
    """Serve the local dashboard, requiring an explicit opt-in for LAN binding."""
    if host.lower() not in {"127.0.0.1", "localhost", "::1", "[::1]"} and not allow_lan:
        typer.echo(
            "Refusing non-loopback bind; pass --allow-lan to expose the server on the LAN.",
            err=True,
        )
        raise typer.Exit(code=2)

    import uvicorn

    from pokemmo_gtl.web.app import create_app

    application = create_app(env=env, allow_lan=allow_lan)
    uvicorn.run(application, host=host, port=port)


if __name__ == "__main__":
    app()
