"""DuckLake access for the cold archive and gold layer.

DuckLake is a lakehouse format: table data stays in Parquet files, while all
metadata (snapshots, schema, file lists, column statistics) lives in a SQL
catalog. It is **embedded** — the DuckDB ``ducklake`` extension loads in-process,
so there is no separate service or container. For this prototype the catalog is a
local DuckDB file and the data is a local directory on the shared volume.

This module centralizes the one way callers open a DuckLake connection, so the
Dagster assets (writers) and the dashboard / archive reader (readers) all attach
the same catalog identically. All business logic stays in Polars; the only SQL
here is DuckLake ``ATTACH`` / ``CREATE TABLE`` / ``INSERT`` / ``SELECT``.

The attached catalog is aliased ``lake`` and tables live in ``lake.main`` — e.g.
``lake.main.raw_events_archive``. The parent directory of the catalog file must
exist before attaching (a DuckLake requirement); ``connect`` ensures it.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    import duckdb

# Catalog alias and schema used everywhere; keep in one place.
CATALOG_ALIAS = "lake"
SCHEMA = "main"

# Table names within the catalog.
ARCHIVE_TABLE = f"{CATALOG_ALIAS}.{SCHEMA}.raw_events_archive"
SESSIONS_TABLE = f"{CATALOG_ALIAS}.{SCHEMA}.gold_sessions"
READINGS_TABLE = f"{CATALOG_ALIAS}.{SCHEMA}.gold_session_readings"
ANALYTICS_TABLE = f"{CATALOG_ALIAS}.{SCHEMA}.gold_analytics_daily"


@contextmanager
def connect(catalog_path: str, data_path: str) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection with the DuckLake catalog attached as ``lake``.

    ``catalog_path`` is a local DuckDB catalog file (e.g.
    ``data/lake/catalog.ducklake``); ``data_path`` is the local directory that
    holds the table Parquet files (e.g. ``data/lake/data``). Both parents are
    created if missing — DuckLake requires the catalog's directory to exist
    before attaching. A new DuckLake is created automatically if none exists in
    the catalog, so this doubles as first-time setup.
    """
    import duckdb

    os.makedirs(os.path.dirname(catalog_path) or ".", exist_ok=True)
    os.makedirs(data_path, exist_ok=True)

    # DuckLake pins the data path in the catalog at creation and rejects a later
    # attach whose DATA_PATH differs. The same catalog file is reached from
    # different cwds (a host run vs. the container's /app bind mount), so a stored
    # absolute path can never match both. OVERRIDE_DATA_PATH tells DuckLake to use
    # the DATA_PATH supplied now instead of the stored one, making the catalog
    # portable across environments. The trailing separator keeps it a directory.
    data_path = os.path.join(data_path, "")

    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        # Pin the session timezone to UTC. DuckDB renders TIMESTAMPTZ values in
        # the session's TimeZone, which otherwise defaults to the host's local
        # zone (e.g. Europe/Berlin) — so the same UTC instant would read back
        # localized differently per environment and break tz-sensitive callers.
        # Forcing UTC makes DuckLake reads deterministic regardless of host.
        con.execute("SET TimeZone = 'UTC';")
        con.execute(
            f"ATTACH 'ducklake:{catalog_path}' AS {CATALOG_ALIAS} "
            "(DATA_PATH ?, OVERRIDE_DATA_PATH TRUE)",
            [data_path],
        )
        yield con
    finally:
        con.close()


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    """True if the fully-qualified ``table`` exists in the attached catalog.

    ``table`` is e.g. ``lake.main.raw_events_archive``; only the bare table name
    is matched against ``information_schema.tables`` for catalog ``lake`` (the
    same existence check used by the readers).
    """
    table_name = table.rsplit(".", 1)[-1]
    rows = con.execute(
        "SELECT 1 FROM information_schema.tables "
        f"WHERE table_catalog = '{CATALOG_ALIAS}' AND table_name = ? LIMIT 1",
        [table_name],
    ).fetchall()
    return bool(rows)


def _jsonify_payload(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure a ``payload`` column is stored as a JSON string (Utf8).

    The archive read path (``read_archive_ducklake`` -> ``_parse_payloads``)
    expects ``payload`` to be Utf8 JSON text. Postgres already hands us
    ``payload::text`` (Utf8), which is left as-is; but if a caller passes a
    dict/Object column, serialize each value to a JSON string so the round-trip
    still yields the original dicts.
    """
    if "payload" not in df.columns or df.schema["payload"] == pl.Utf8:
        return df
    import json

    serialized = [None if v is None else json.dumps(v) for v in df["payload"].to_list()]
    return df.with_columns(pl.Series("payload", serialized, dtype=pl.Utf8))


def append_frame(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """Append a Polars frame to a DuckLake table, creating it on first write.

    DuckDB reads the Python-local variable ``df`` by name in the SQL. The first
    time, ``CREATE TABLE ... AS SELECT`` seeds the table and its schema; after
    that, ``INSERT INTO ... BY NAME SELECT`` appends by column name, tolerating
    column reordering or additive drift. A ``payload`` column is normalized to
    JSON text first so the archive read path round-trips it back to dicts.
    """
    df = _jsonify_payload(df)  # noqa: F841 - referenced by name in SQL below
    if _table_exists(con, table):
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM df")
    else:
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")


def replace_partition(
    con: duckdb.DuckDBPyConnection,
    table: str,
    day_col: str,
    day: str,
    df: pl.DataFrame,
) -> None:
    """Idempotently replace one day's rows in a DuckLake table.

    Deletes the existing rows whose ``day_col::date`` equals ``day`` (an ISO
    ``YYYY-MM-DD`` string) and inserts ``df`` in their place, so re-running a
    partition is a no-op on row count. Creates the table on first write. Used by
    the partitioned archive (``day_col='ingest_ts'``) and gold assets
    (``day_col='start_time'``). ``df`` is bound by name in the SQL.
    """
    df = _jsonify_payload(df)  # noqa: F841 - referenced by name in SQL below
    if not _table_exists(con, table):
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        return
    con.execute(f"DELETE FROM {table} WHERE {day_col}::date = ?", [day])
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM df")


def replace_table(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    """Fully (re)create a DuckLake table from a Polars frame.

    Used by the gold assets, which rebuild each table from scratch every run.
    DuckDB resolves the local ``df`` variable by name in the SQL.
    """
    df = _jsonify_payload(df)  # noqa: F841 - referenced by name in SQL below
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df")


def read_gold_table(catalog_path: str, data_path: str, table: str) -> pl.DataFrame:
    """Read a gold DuckLake table into a Polars frame.

    Opens the catalog, returns an empty frame if ``table`` does not yet exist
    (so dashboards and downstream assets never crash before the pipeline has
    run), else selects the whole table via ``.pl()``.
    """
    with connect(catalog_path, data_path) as con:
        if not _table_exists(con, table):
            return pl.DataFrame()
        return con.execute(f"SELECT * FROM {table}").pl()


def catalog_overview(catalog_path: str, data_path: str) -> dict[str, pl.DataFrame]:
    """Summarize the DuckLake catalog for human inspection.

    The raw ``ducklake_*`` catalog tables are internal bookkeeping and bury the
    useful facts; this surfaces them as three ready-to-display frames:

    - ``tables``:    one row per catalog table with its live row count.
    - ``columns``:   the column schema (name + type) of every catalog table.
    - ``snapshots``: the DuckLake snapshot history (one row per commit), newest
      first — each archive append and each gold rebuild is a snapshot.

    All frames are empty when the catalog has no tables / no snapshots yet.
    """
    empty = pl.DataFrame()
    with connect(catalog_path, data_path) as con:
        tables = con.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog = '{CATALOG_ALIAS}' ORDER BY table_name"
        ).pl()

        if tables.is_empty():
            return {"tables": empty, "columns": empty, "snapshots": _snapshots(con)}

        # Per-table live row counts (SELECT count(*) per table — a few tables).
        counts = {
            name: con.execute(f"SELECT count(*) FROM {CATALOG_ALIAS}.{SCHEMA}.{name}").fetchone()[0]
            for name in tables["table_name"].to_list()
        }
        tables = tables.with_columns(
            pl.col("table_name").replace_strict(counts, return_dtype=pl.Int64).alias("rows")
        )

        columns = con.execute(
            "SELECT table_name, column_name, data_type, ordinal_position "
            "FROM information_schema.columns "
            f"WHERE table_catalog = '{CATALOG_ALIAS}' "
            "ORDER BY table_name, ordinal_position"
        ).pl()

        return {"tables": tables, "columns": columns, "snapshots": _snapshots(con)}


def _snapshots(con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """DuckLake snapshot history (newest first), empty if none/unavailable."""
    try:
        return con.execute(
            f"SELECT * FROM ducklake_snapshots('{CATALOG_ALIAS}') ORDER BY snapshot_id DESC"
        ).pl()
    except Exception:  # noqa: BLE001 - no snapshots yet or function unavailable
        return pl.DataFrame()
