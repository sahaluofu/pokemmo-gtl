"""Item data from PokeMMO-Tools/pokemmo-data with a local UTF-8 fallback."""

import json
import sqlite3
import time
import warnings
from pathlib import Path
from typing import Any

import httpx

from pokemmo_gtl.config import AppConfig, Environment, load_config
from pokemmo_gtl.db.connection import open_connection, write_transaction

REPOSITORY = "PokeMMO-Tools/pokemmo-data"
RAW_BASE = f"https://raw.githubusercontent.com/{REPOSITORY}/master/data"
_ID_FIELDS = ("item_id", "reference_id", "referenceId", "id", "itemId")
_NAME_FIELDS = ("name_en", "english_name", "name", "english", "en")


def load_seed_catalog(path: Path) -> list[dict[str, Any]]:
    """Validate the small bundled dictionary; IDs are real items except >=900000."""
    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or not 30 <= len(entries) <= 50:
        raise ValueError("seed catalog must contain 30 to 50 items")
    seen = set()
    for entry in entries:
        item_id = entry.get("item_id")
        stack = entry.get("max_stack_size")
        if type(item_id) is not int or item_id <= 0 or item_id in seen:
            raise ValueError("seed catalog IDs must be unique positive integers")
        if not isinstance(entry.get("name_en"), str) or not entry["name_en"].strip():
            raise ValueError(f"missing seed name for item {item_id}")
        if type(stack) is not int or stack <= 0:
            raise ValueError(f"invalid stack size for item {item_id}")
        if entry.get("asset_class") in {"cosmetic", "pokemon_category"} and stack != 1:
            raise ValueError(f"item {item_id} must use max_stack_size=1")
        seen.add(item_id)
    return entries


def sync_default_events(db_path: Path, events: list[dict[str, Any]]) -> int:
    """Refresh default events by stable name/type identity inside one reserved transaction."""
    with open_connection(db_path) as conn, write_transaction(conn):
        for event in events:
            _upsert_event(conn, event)
    return len(events)


def sync_catalog(
    db_path: Path,
    *,
    config: AppConfig | None = None,
    env: Environment | str = Environment.live,
    root: Path | None = None,
    offline_only: bool = False,
    from_local: Path | None = None,
    timeout: float = 20.0,
) -> int:
    """Merge seed, upstream dictionary and configured Chinese aliases atomically.

    Network failures fall back to the bundled seed. Explicit local input errors
    propagate so a mistyped directory cannot look like a successful local import.
    """
    app_config = config or load_config(env, root=root)
    records = {
        item["item_id"]: dict(item)
        for item in load_seed_catalog(app_config.root / "config" / "seed_catalog_minimal.json")
    }
    incoming = []
    if from_local is not None:
        incoming = load_local_catalog(from_local)
    elif not offline_only:
        try:
            incoming = fetch_upstream_catalog(timeout=timeout)
        except (httpx.HTTPError, ValueError, RuntimeError):
            warnings.warn("Catalog download failed; using bundled seed catalog.", stacklevel=2)
    for entry in incoming:
        records[entry["item_id"]] = {**records.get(entry["item_id"], {}), **entry}
    for alias in app_config.item_aliases:
        item_id = alias["item_id"]
        if item_id not in records:
            raise ValueError(f"alias references unknown item {item_id}")
        records[item_id].update(alias)
    for relation in app_config.item_relations["rp_vouchers"]:
        if relation["item_id"] not in records or relation["denomination"] <= 0:
            raise ValueError("invalid RP voucher relation")
    for relation in app_config.item_relations["upstream_relations"]:
        if any(
            records.get(relation[key]) is None for key in ("target_item_id", "upstream_item_id")
        ):
            raise ValueError("upstream relation references unknown item")

    updated_at = (
        app_config.settings.simulation_default_end_ts
        if app_config.env is Environment.sim
        else int(time.time())
    )
    with open_connection(db_path) as conn, write_transaction(conn):
        conn.executemany(
            """INSERT INTO items_catalog
               (item_id, asset_class, name_en, name_zh, category, max_stack_size,
                description, is_watched, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(item_id) DO UPDATE SET
                 asset_class = excluded.asset_class,
                 name_en = excluded.name_en,
                 name_zh = COALESCE(excluded.name_zh, items_catalog.name_zh),
                 category = excluded.category,
                 max_stack_size = excluded.max_stack_size,
                 description = COALESCE(excluded.description, items_catalog.description),
                 is_watched = excluded.is_watched,
                 updated_at = excluded.updated_at""",
            [
                (
                    item["item_id"], item.get("asset_class", "item"), item["name_en"],
                    item.get("name_zh"), item.get("category", "other"),
                    item.get("max_stack_size", 99), item.get("description"),
                    int(bool(item.get("is_watched", False))), updated_at,
                )
                for item in sorted(records.values(), key=lambda record: record["item_id"])
            ],
        )
        for event in app_config.events:
            _upsert_event(conn, event)
    return len(records)


def load_local_catalog(directory: Path) -> list[dict[str, Any]]:
    """Load items.json and its berry/cosmetic metadata from a repository checkout."""
    directory = Path(directory).resolve()
    if not directory.is_dir():
        raise ValueError(f"catalog directory does not exist: {directory}")
    paths = sorted(directory.rglob("items.json"))
    if not paths:
        paths = sorted(directory.glob("*.json"))
    records = []
    for file_path in paths:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        entries = _normalize_payload(payload)
        _apply_metadata(
            entries,
            {
                name: json.loads((file_path.parent / name).read_text(encoding="utf-8"))
                for name in ("items-berry.json", "items-cosmetic.json")
                if (file_path.parent / name).is_file()
            },
        )
        records.extend(entries)
    if not records:
        raise ValueError("no named item records found in local catalog directory")
    return records


def fetch_upstream_catalog(*, timeout: float = 20.0) -> list[dict[str, Any]]:
    """Use the repository's documented raw paths, avoiding GitHub API rate limits."""
    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        payloads = {}
        for name in ("items.json", "items-berry.json", "items-cosmetic.json"):
            response = client.get(f"{RAW_BASE}/{name}")
            response.raise_for_status()
            if len(response.content) > 10 * 1024 * 1024:
                raise ValueError("upstream catalog file exceeds 10 MB")
            payloads[name] = response.json()
    records = _normalize_payload(payloads.pop("items.json"))
    if not records:
        raise ValueError("upstream catalog contains no named items")
    _apply_metadata(records, payloads)
    return records


def _normalize_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and "items" in payload:
        payload = payload["items"]
    if isinstance(payload, dict):
        if any(key in payload for key in _ID_FIELDS):
            payload = [payload]
        else:
            payload = [
                {"item_id": key, **value} if isinstance(value, dict) else value
                for key, value in payload.items()
            ]
    if not isinstance(payload, list):
        return []
    records = []
    for source in payload:
        if not isinstance(source, dict):
            continue
        item_id = next((source[key] for key in _ID_FIELDS if key in source), None)
        name = next((source[key] for key in _NAME_FIELDS if source.get(key)), None)
        if isinstance(name, dict):
            name = name.get("en") or name.get("english")
        if not isinstance(name, str) or not name.strip() or isinstance(item_id, bool):
            continue
        try:
            item_id = int(item_id)
            stack = int(source.get("max_stack_size", source.get("stack_size", 99)))
        except (ValueError, TypeError):
            continue
        if item_id <= 0 or stack <= 0:
            continue
        entry = {"item_id": item_id, "name_en": name.strip()}
        for key in ("category", "asset_class", "name_zh", "description"):
            if isinstance(source.get(key), str):
                entry[key] = source[key]
        if "max_stack_size" in source or "stack_size" in source:
            entry["max_stack_size"] = stack
        records.append(entry)
    return records


def _apply_metadata(records: list[dict[str, Any]], payloads: dict[str, Any]) -> None:
    by_id = {entry["item_id"]: entry for entry in records}
    for berry in payloads.get("items-berry.json", []):
        for item_id in berry.get("items", []):
            if item_id in by_id:
                by_id[item_id].update(asset_class="berry", category="berry", max_stack_size=99)
    for cosmetic in payloads.get("items-cosmetic.json", []):
        for item_id in cosmetic.get("item_id", []):
            if item_id in by_id:
                by_id[item_id].update(asset_class="cosmetic", category="cosmetic", max_stack_size=1)


def _upsert_event(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    if event["end_ts"] <= event["start_ts"]:
        raise ValueError("event end_ts must be after start_ts")
    row = conn.execute(
        "SELECT event_id FROM market_events WHERE name = ? AND event_type = ? "
        "ORDER BY event_id LIMIT 1",
        (event["name"], event["event_type"]),
    ).fetchone()
    values = (
        event["announced_at"], event["start_ts"], event["end_ts"],
        event.get("impact_scope", "all"), event.get("notes"),
    )
    if row is None:
        conn.execute(
            """INSERT INTO market_events
               (announced_at, start_ts, end_ts, impact_scope, notes, name, event_type)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (*values, event["name"], event["event_type"]),
        )
    else:
        conn.execute(
            """UPDATE market_events SET announced_at = ?, start_ts = ?, end_ts = ?,
                 impact_scope = ?, notes = ? WHERE event_id = ?""",
            (*values, row["event_id"]),
        )
