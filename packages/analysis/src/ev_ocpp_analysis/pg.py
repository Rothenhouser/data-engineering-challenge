"""Postgres raw-landing contract: schema + append-only psycopg writer.

The ``raw_events`` table is the single convergence point both ingestion paths
write to. It is append-only — no unique constraint, no dedup (Requirement 2.5);
duplicate business frames are intentionally recorded and collapsed later by the
sessionization fold. All writes are parameterized psycopg INSERTs (no ORM,
Requirement 13.1/13.7). ``payload`` is stored AS-IS as jsonb (Requirement 2.3);
``ingest_ts`` is wall-clock ingest time, not parsed from the payload (2.4).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime

import psycopg

from .parsing import RawRow

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    charger_id TEXT        NOT NULL,
    msg_type   SMALLINT    NOT NULL,
    unique_id  TEXT        NOT NULL,
    action     TEXT,
    payload    JSONB       NOT NULL,
    ingest_ts  TIMESTAMPTZ NOT NULL
);
"""

_INSERT_SQL = (
    "INSERT INTO raw_events (charger_id, msg_type, unique_id, action, payload, ingest_ts) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)


def init_schema(conn: psycopg.Connection) -> None:
    """Create the append-only ``raw_events`` table if it does not exist."""
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()


def _params(row: RawRow, ingest_ts: datetime) -> tuple:
    mapping = row.as_row()
    return (
        mapping["charger_id"],
        mapping["msg_type"],
        mapping["unique_id"],
        mapping.get("action"),
        json.dumps(mapping.get("payload") or {}),
        ingest_ts,
    )


def write_raw_event(
    conn: psycopg.Connection, row: RawRow, ingest_ts: datetime | None = None
) -> None:
    """Append one RawEvent, committed per-message for crash safety (2.6, 2.7)."""
    conn.execute(_INSERT_SQL, _params(row, ingest_ts or datetime.now(UTC)))
    conn.commit()


def write_raw_events(
    conn: psycopg.Connection, rows: Iterable[RawRow], ingest_ts: datetime | None = None
) -> int:
    """Append many RawEvents in one parameterized batch; returns the row count."""
    ts = ingest_ts or datetime.now(UTC)
    params = [_params(r, ts) for r in rows]
    if not params:
        return 0
    with conn.cursor() as cur:
        cur.executemany(_INSERT_SQL, params)
    conn.commit()
    return len(params)
