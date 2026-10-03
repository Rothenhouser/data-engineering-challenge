"""Per-source readers that fetch raw events into a Polars frame for the fold.

The two sources differ only in how rows are fetched; the shared
``reconstruct_sessions`` fold is identical (Requirement 7). Both readers return a
frame carrying the RawEvent columns the fold expects
(``station_id, msg_type, unique_id, action, payload, ingest_ts``), so
"fetch -> fold" is one import.

- Historical: DuckDB scans the immutable Parquet archive (Requirements 7.3, 13.3).
- Live: Postgres via ``polars.read_database_uri`` + ConnectorX (Requirements 7.2, 8.1, 13.2).
"""

from __future__ import annotations

import json
from datetime import datetime

import polars as pl


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
    predicate pushdown, e.g. ``"station_id = 'charger1'"``. Returns a frame with
    the RawEvent columns, with ``payload`` decoded to dicts.
    """
    import duckdb

    where = f" WHERE {predicate}" if predicate else ""
    sql = (
        "SELECT station_id, msg_type, unique_id, action, payload, ingest_ts "
        f"FROM read_parquet(?){where} ORDER BY event_id"
    )
    con = duckdb.connect()
    try:
        df = con.execute(sql, [parquet_glob]).pl()
    finally:
        con.close()
    return _parse_payloads(df)


# --- Live reader: Postgres via ConnectorX ----------------------------------


def read_recent_window_postgres(conn_uri: str, since: datetime) -> pl.DataFrame:
    """Read the bounded recent window of raw events from Postgres (ConnectorX).

    Reads rows with ``ingest_ts >= since`` for live/in-flight session
    reconstruction. ``payload`` is decoded to dicts for the fold.
    """
    query = (
        "SELECT station_id, msg_type, unique_id, action, payload::text AS payload, "
        f"ingest_ts FROM raw_events WHERE ingest_ts >= '{since.isoformat()}' "
        "ORDER BY event_id"
    )
    df = pl.read_database_uri(query, conn_uri, engine="connectorx")
    return _parse_payloads(df)


def read_latest_per_station_postgres(conn_uri: str) -> pl.DataFrame:
    """Read the latest RawEvent per ``station_id`` for the live status view.

    Powers the dashboard's cheap latest-per-charger snapshot without running the
    fold (Requirement 8.1). ``payload`` is decoded to dicts.
    """
    query = (
        "SELECT DISTINCT ON (station_id) station_id, msg_type, unique_id, action, "
        "payload::text AS payload, ingest_ts FROM raw_events "
        "ORDER BY station_id, event_id DESC"
    )
    df = pl.read_database_uri(query, conn_uri, engine="connectorx")
    return _parse_payloads(df)
