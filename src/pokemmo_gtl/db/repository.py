"""Atomic, versioned schema migrations (no market business logic)."""

import sqlite3
import time
from pathlib import Path

from pokemmo_gtl.db.connection import open_connection, write_transaction

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _execute_sql(conn: sqlite3.Connection, script: str) -> None:
    # executescript() implicitly commits, so execute complete statements individually.
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            conn.execute(statement)
            statement = ""
    if statement.strip():
        raise ValueError("Incomplete migration SQL")


def migrate(
    db_path: Path, *, migrations_dir: Path = MIGRATIONS_DIR, applied_at: int | None = None
) -> list[str]:
    """Apply pending migrations and their ledger entries in one reserved transaction."""
    applied = []
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version TEXT PRIMARY KEY, applied_at INTEGER NOT NULL) STRICT"
        )
        for migration in sorted(migrations_dir.glob("*.sql")):
            version = migration.stem
            if conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?", (version,)
            ).fetchone():
                continue
            _execute_sql(conn, migration.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, int(time.time()) if applied_at is None else applied_at),
            )
            applied.append(version)
    return applied
