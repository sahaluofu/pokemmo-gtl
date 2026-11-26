"""CLI wiring checks using isolated databases under pytest's temporary root."""

import sys
import types
from dataclasses import replace
from shutil import copytree
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from pokemmo_gtl import cli
from pokemmo_gtl.config import PROJECT_ROOT, Environment
from pokemmo_gtl.config import load_config as load_project_config
from pokemmo_gtl.db.connection import open_connection, write_transaction
from pokemmo_gtl.ingestion.official_api import ApiAuthError, ApiError
from pokemmo_gtl.ingestion.rate_limiter import CollectorBusyError


@pytest.fixture
def isolated_cli(tmp_path, monkeypatch):
    runner = CliRunner()
    copytree(PROJECT_ROOT / "config", tmp_path / "config")

    def load_isolated_config(env=Environment.live):
        base = load_project_config(env)
        return replace(base, root=tmp_path)

    monkeypatch.setattr(cli, "load_config", load_isolated_config)
    return runner, tmp_path


def test_init_and_offline_catalog_sync_use_temp_database(isolated_cli):
    runner, root = isolated_cli

    initialized = runner.invoke(cli.app, ["init-db", "--env", "sim"])
    assert initialized.exit_code == 0, initialized.output
    assert "env=sim" in initialized.output

    synced = runner.invoke(cli.app, ["sync-catalog", "--env", "sim", "--offline-only"])
    assert synced.exit_code == 0, synced.output
    assert "items=43" in synced.output
    assert (root / "data" / "gtl_sim.db").is_file()
    assert not (root / "data" / "gtl_live.db").exists()


def test_simulate_rejects_live_environment(isolated_cli):
    runner, root = isolated_cli

    result = runner.invoke(cli.app, ["simulate", "--days", "1", "--env", "live"])

    assert result.exit_code != 0
    assert "simulate requires --env sim" in result.output
    assert not (root / "data" / "gtl_live.db").exists()


def test_simulation_materializes_bars_retains_raw_rows_and_runs_quant(isolated_cli):
    runner, root = isolated_cli
    db_path = root / "data" / "gtl_sim.db"

    result = runner.invoke(cli.app, ["simulate", "--days", "5", "--seed", "42", "--env", "sim"])
    assert result.exit_code == 0, result.output
    assert "snapshots=480" in result.output
    assert "raw_listings_pruned=" in result.output
    pruned = int(result.output.split("raw_listings_pruned=")[-1].split()[0])
    assert pruned > 0

    settings = load_project_config(Environment.sim).settings
    cutoff = settings.simulation_default_end_ts - settings.retention_hours * 3600
    with open_connection(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM snapshot_runs WHERE source = 'simulator' AND status = 'completed'"
        ).fetchone()[0] == 480
        frames = {
            row["timeframe"]: row["bars"]
            for row in conn.execute(
                "SELECT timeframe, COUNT(*) AS bars FROM item_bars GROUP BY timeframe"
            )
        }
        assert all(frames.get(timeframe, 0) > 0 for timeframe in ("15m", "4h", "1d"))
        assert conn.execute("SELECT COUNT(*) FROM inferred_flows").fetchone()[0] > 0
        assert conn.execute(
            """SELECT COUNT(*) FROM raw_listings AS r
               JOIN snapshot_runs AS s USING (snapshot_id)
               WHERE s.completed_at < ?""",
            (cutoff,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """SELECT COUNT(*) FROM raw_listings AS r
               JOIN snapshot_runs AS s USING (snapshot_id)
               WHERE s.completed_at >= ?""",
            (cutoff,),
        ).fetchone()[0] > 0

    forecast = runner.invoke(cli.app, ["forecast", "--env", "sim"])
    assert forecast.exit_code == 0, forecast.output
    assert "Forecasts generated=" in forecast.output
    assert "signals=" in forecast.output

    backtest = runner.invoke(
        cli.app,
        ["backtest", "--env", "sim", "--horizon", "72", "--models", "naive,statistical"],
    )
    assert backtest.exit_code == 0, backtest.output
    assert "ROC-AUC=" in backtest.output
    assert "pinball_mean=" in backtest.output
    assert "net_return_pct=" in backtest.output
    assert "max_drawdown_pct=" in backtest.output
    assert "trade_count=" in backtest.output
    with open_connection(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] > 0
        assert conn.execute("SELECT COUNT(*) FROM backtest_runs").fetchone()[0] == 2


def test_quant_commands_parse_repeated_ids_and_lightgbm_alias(isolated_cli, monkeypatch):
    runner, _root = isolated_cli
    calls = []

    def fake_forecast(config, *, horizons, item_ids):
        calls.append(("forecast", horizons, item_ids))
        return [{"signal": "BUY_WATCH"}]

    def fake_backtest(config, *, horizon_hours, models, item_id, category):
        calls.append(("backtest", horizon_hours, models, item_id, category))
        return {
            "item_scope": f"item:{item_id}",
            "comparison_table": [
                {
                    "model_name": model,
                    "roc_auc": None,
                    "pinball_mean": None,
                    "net_return_pct": 0,
                    "max_drawdown_pct": 0,
                    "trade_count": 0,
                }
                for model in models
            ],
        }

    monkeypatch.setattr(cli, "run_forecast", fake_forecast)
    monkeypatch.setattr(cli, "run_backtest", fake_backtest)

    forecast = runner.invoke(
        cli.app,
        [
            "forecast", "--env", "sim", "--horizons", "24,72",
            "--item-id", "4", "--item-id", "8",
        ],
    )
    assert forecast.exit_code == 0, forecast.output
    assert "BUY_WATCH=1" in forecast.output

    backtest = runner.invoke(
        cli.app,
        [
            "backtest", "--env", "sim", "--horizon", "72",
            "--models", "naive,lightgbm", "--item-id", "4", "--category", "material",
        ],
    )
    assert backtest.exit_code == 0, backtest.output
    assert calls == [
        ("forecast", (24, 72), [4, 8]),
        ("backtest", 72, ("naive", "lgbm"), 4, "material"),
    ]


def test_import_csv_accepts_utf8_bom(isolated_cli):
    runner, root = isolated_cli
    assert runner.invoke(cli.app, ["init-db", "--env", "sim"]).exit_code == 0
    assert runner.invoke(
        cli.app, ["sync-catalog", "--env", "sim", "--offline-only"]
    ).exit_code == 0
    csv_path = root / "quotes.csv"
    csv_path.write_text(
        "\ufeffitem_id,price,recorded_at,listed_qty,note\n4,123,1768435200,2,cli import\n",
        encoding="utf-8",
    )

    imported = runner.invoke(cli.app, ["import-csv", str(csv_path), "--env", "sim"])

    assert imported.exit_code == 0, imported.output
    assert "rows=1" in imported.output
    with open_connection(root / "data" / "gtl_sim.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM manual_quotes").fetchone()[0] == 1


def test_collect_once_wires_aggregation_then_retention(isolated_cli, monkeypatch):
    runner, root = isolated_cli
    events = []
    migrate_calls = []
    real_migrate = cli.migrate
    monkeypatch.setattr(
        cli,
        "migrate",
        lambda path: (migrate_calls.append(path), real_migrate(path))[1],
    )

    def fake_collect(config, *, post_snapshot_hook):
        events.append("collect")
        with open_connection(config.db_path) as conn, write_transaction(conn):
            snapshot_id = conn.execute(
                """INSERT INTO snapshot_runs
                   (source, started_at, server_started_at, status)
                   VALUES ('official_api', 1768435200, 1768435200, 'running')"""
            ).lastrowid
            post_snapshot_hook(conn, snapshot_id)
            conn.execute(
                "UPDATE snapshot_runs SET status = 'completed', completed_at = 1768435200 "
                "WHERE snapshot_id = ?",
                (snapshot_id,),
            )
        return SimpleNamespace(snapshot_id=snapshot_id, total_listings=2)

    def fake_process(conn, snapshot_id, settings):
        assert conn.in_transaction
        events.append(("process", snapshot_id, settings))

    def fake_retention(conn, settings):
        assert not conn.in_transaction
        events.append("retention")
        return 7

    monkeypatch.setattr(cli, "collect_once", fake_collect)
    monkeypatch.setattr(cli, "process_snapshot", fake_process)
    monkeypatch.setattr(cli, "run_retention", fake_retention)

    result = runner.invoke(cli.app, ["collect", "--env", "sim", "--once"])

    assert result.exit_code == 0, result.output
    assert migrate_calls == [root / "data" / "gtl_sim.db"]
    assert [event if isinstance(event, str) else event[0] for event in events] == [
        "collect",
        "process",
        "retention",
    ]
    assert "raw_listings_pruned=7" in result.output


def test_collect_auth_error_is_nonzero_and_does_not_print_credentials(isolated_cli, monkeypatch):
    runner, _root = isolated_cli
    secret = "token=do-not-disclose"

    def fail_auth(*_args, **_kwargs):
        raise ApiAuthError(secret)

    monkeypatch.setattr(cli, "collect_once", fail_auth)

    result = runner.invoke(cli.app, ["collect", "--env", "sim", "--once"])

    assert result.exit_code == 1
    assert "Authentication failed" in result.output
    assert secret not in result.output


def test_collect_rejects_once_and_daemon_together(isolated_cli):
    runner, _root = isolated_cli

    result = runner.invoke(cli.app, ["collect", "--env", "sim", "--once", "--daemon"])

    assert result.exit_code != 0
    assert "--once and --daemon" in result.output


def test_collect_daemon_uses_start_to_start_cadence_and_continues_errors(
    isolated_cli, monkeypatch
):
    runner, _root = isolated_cli
    now = [0.0]
    starts = []
    sleeps = []
    outcomes = iter((CollectorBusyError("busy"), ApiError("temporary failure")))

    def monotonic():
        return now[0]

    def collect(_config, *, post_snapshot_hook):
        starts.append(monotonic())
        now[0] += 0.25
        raise next(outcomes)

    def sleep(delay):
        sleeps.append(delay)
        now[0] += delay

    monkeypatch.setattr(cli.time, "monotonic", monotonic)
    monkeypatch.setattr(cli.time, "sleep", sleep)
    monkeypatch.setattr(cli, "collect_once", collect)

    result = runner.invoke(
        cli.app,
        ["collect", "--env", "sim", "--daemon", "--interval", "1", "--max-cycles", "2"],
    )

    assert result.exit_code == 0, result.output
    assert starts == [0.0, 1.0]
    assert sleeps == [0.75]
    assert "Warning: collector busy" in result.output
    assert "Warning: collection failed" in result.output


def test_serve_requires_lan_opt_in_and_imports_web_lazily(isolated_cli, monkeypatch):
    runner, _root = isolated_cli

    denied = runner.invoke(cli.app, ["serve", "--env", "sim", "--host", "0.0.0.0"])
    assert denied.exit_code == 2
    assert "--allow-lan" in denied.output

    marker = object()
    app_module = types.ModuleType("pokemmo_gtl.web.app")
    calls = []
    app_module.create_app = lambda **kwargs: calls.append(("create", kwargs)) or marker
    web_package = types.ModuleType("pokemmo_gtl.web")
    web_package.__path__ = []
    uvicorn_module = types.ModuleType("uvicorn")
    uvicorn_module.run = lambda application, **kwargs: calls.append(
        ("run", application, kwargs)
    )
    monkeypatch.setitem(sys.modules, "pokemmo_gtl.web", web_package)
    monkeypatch.setitem(sys.modules, "pokemmo_gtl.web.app", app_module)
    monkeypatch.setitem(sys.modules, "uvicorn", uvicorn_module)

    allowed = runner.invoke(
        cli.app, ["serve", "--env", "sim", "--host", "0.0.0.0", "--allow-lan"]
    )

    assert allowed.exit_code == 0, allowed.output
    assert calls == [
        ("create", {"env": Environment.sim, "allow_lan": True}),
        ("run", marker, {"host": "0.0.0.0", "port": 8000}),
    ]
