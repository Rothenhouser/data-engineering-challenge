"""Shared simulation-control contract (one small Postgres row).

Both the stream consumer and the dashboard read/write a single-row ``sim_control``
table so the simulation can be steered live, without restarting containers:

- ``stream_delay`` — seconds the stream consumer sleeps between frames.
- ``clock_mode``   — how the dashboard derives "now" for the live fold:
  ``data`` (max event time in the window) or ``wall`` (real wall-clock).

Kept deliberately tiny and plain-SQL (psycopg, no ORM), consistent with the rest
of the persistence layer.
"""

from __future__ import annotations

from typing import Any

import psycopg

CREATE_SIM_CONTROL_SQL = """
CREATE TABLE IF NOT EXISTS sim_control (
    id           SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    stream_delay DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    clock_mode   TEXT             NOT NULL DEFAULT 'data'
);
INSERT INTO sim_control (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""

DEFAULTS: dict[str, Any] = {"stream_delay": 1.0, "clock_mode": "data"}


def init_sim_control(conn: psycopg.Connection) -> None:
    """Create the single-row control table and seed its default row."""
    conn.execute(CREATE_SIM_CONTROL_SQL)
    conn.commit()


def read_sim_control(conn: psycopg.Connection) -> dict[str, Any]:
    """Return the current controls, falling back to defaults if unset."""
    try:
        row = conn.execute(
            "SELECT stream_delay, clock_mode FROM sim_control WHERE id = 1"
        ).fetchone()
    except psycopg.Error:
        return dict(DEFAULTS)
    if not row:
        return dict(DEFAULTS)
    return {"stream_delay": float(row[0]), "clock_mode": row[1]}


def write_sim_control(
    conn: psycopg.Connection,
    stream_delay: float | None = None,
    clock_mode: str | None = None,
) -> None:
    """Update whichever controls are provided (one-row upsert)."""
    init_sim_control(conn)
    if stream_delay is not None:
        conn.execute("UPDATE sim_control SET stream_delay = %s WHERE id = 1", (stream_delay,))
    if clock_mode is not None:
        conn.execute("UPDATE sim_control SET clock_mode = %s WHERE id = 1", (clock_mode,))
    conn.commit()
