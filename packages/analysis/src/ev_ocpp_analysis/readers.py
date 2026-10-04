"""Per-source readers that fetch raw events into a Polars frame for the fold.

The two sources differ only in how rows are fetched; the shared
``reconstruct_sessions`` fold is identical (Requirement 7). Both readers return a
frame carrying the RawEvent columns the fold expects
(``charger_id, msg_type, unique_id, action, payload, ingest_ts``), so
"fetch -> fold" is one import.

- Historical: DuckDB reads the immutable cold DuckLake archive table (Requirement 7.3).
- Live: Postgres via ``polars.read_database_uri`` + ConnectorX (Requirements 7.2, 8.1, 13.2).
"""

from __future__ import annotations

import json
from datetime import datetime

import polars as pl

from .ducklake import ARCHIVE_TABLE, _table_exists, connect


def _parse_payloads(df: pl.DataFrame) -> pl.DataFrame:
    """Decode a JSON-string ``payload`` column to Python dicts for the fold."""
    if df.is_empty() or df.schema.get("payload") != pl.Utf8:
        return df
    decoded = [json.loads(p) if p else {} for p in df["payload"].to_list()]
    return df.with_columns(pl.Series("payload", decoded, dtype=pl.Object))


# --- Historical reader: Parquet archive via DuckDB -------------------------


def read_archive_duckdb(parquet_glob: str, predicate: str | None = None) -> pl.DataFrame:
    """Read raw events from the Parquet archive via a DuckDB scan.

    ``predicate`` is an optional SQL WHERE clause (without the keyword) for
    predicate pushdown, e.g. ``"charger_id = 'charger1'"``. Returns a frame with
    the RawEvent columns, with ``payload`` decoded to dicts.
    """
    import duckdb

    where = f" WHERE {predicate}" if predicate else ""
    sql = (
        "SELECT charger_id, msg_type, unique_id, action, payload, ingest_ts "
        f"FROM read_parquet(?){where} ORDER BY event_id"
    )
    con = duckdb.connect()
    try:
        df = con.execute(sql, [parquet_glob]).pl()
    finally:
        con.close()
    return _parse_payloads(df)


# --- Historical reader: DuckLake cold archive table ------------------------


def read_archive_ducklake(
    catalog_path: str, data_path: str, predicate: str | None = None
) -> pl.DataFrame:
    """Read raw events from the cold DuckLake archive table.

    Attaches the DuckLake catalog and selects the RawEvent columns from
    ``lake.main.raw_events_archive``, handing Polars a frame via ``.pl()``.
    ``predicate`` is an optional SQL WHERE clause (without the keyword) for
    pushdown, e.g. ``"charger_id = 'charger1'"``. ``payload`` is decoded to dicts
    for the fold. Returns an empty frame if the table does not yet exist.
    """
    where = f" WHERE {predicate}" if predicate else ""
    sql = (
        "SELECT charger_id, msg_type, unique_id, action, payload, ingest_ts "
        f"FROM {ARCHIVE_TABLE}{where} ORDER BY event_id"
    )
    with connect(catalog_path, data_path) as con:
        if not _table_exists(con, ARCHIVE_TABLE):
            return pl.DataFrame()
        df = con.execute(sql).pl()
    return _parse_payloads(df)


# --- Live reader: Postgres via ConnectorX ----------------------------------


def read_recent_window_postgres(conn_uri: str, since: datetime) -> pl.DataFrame:
    """Read the bounded recent window of raw events from Postgres (ConnectorX).

    Reads rows with ``ingest_ts >= since`` for live/in-flight session
    reconstruction. ``payload`` is decoded to dicts for the fold.
    """
    query = (
        "SELECT charger_id, msg_type, unique_id, action, payload::text AS payload, "
        f"ingest_ts FROM raw_events WHERE ingest_ts >= '{since.isoformat()}' "
        "ORDER BY event_id"
    )
    df = pl.read_database_uri(query, conn_uri, engine="connectorx")
    return _parse_payloads(df)


def read_latest_per_charger_postgres(conn_uri: str) -> pl.DataFrame:
    """Read the latest RawEvent per ``charger_id`` for the live status view.

    Powers the dashboard's cheap latest-per-charger snapshot without running the
    fold (Requirement 8.1). ``payload`` is decoded to dicts.
    """
    query = (
        "SELECT DISTINCT ON (charger_id) charger_id, msg_type, unique_id, action, "
        "payload::text AS payload, ingest_ts FROM raw_events "
        "ORDER BY charger_id, event_id DESC"
    )
    df = pl.read_database_uri(query, conn_uri, engine="connectorx")
    return _parse_payloads(df)


def read_latest_metervalues_per_charger_postgres(conn_uri: str) -> pl.DataFrame:
    """Read the latest MeterValues RawEvent per ``charger_id``.

    The newest event per charger is usually a Heartbeat/ack with no readings;
    power and SoC only live in MeterValues payloads, so the live status view
    pulls the latest MeterValues separately for its measurand columns.
    """
    query = (
        "SELECT DISTINCT ON (charger_id) charger_id, payload::text AS payload, ingest_ts "
        "FROM raw_events WHERE action = 'MeterValues' "
        "ORDER BY charger_id, event_id DESC"
    )
    df = pl.read_database_uri(query, conn_uri, engine="connectorx")
    return _parse_payloads(df)
