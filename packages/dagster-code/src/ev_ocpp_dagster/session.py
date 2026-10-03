"""Session-reconstruction asset: rebuild gold session facts from the archive.

Reads raw events from the Cold_Archive through the DuckLake->Polars reader, runs
the shared fold, and materializes two gold DuckLake tables (Requirements 4.1,
4.7, 7.1): ``gold_sessions`` (session facts) and ``gold_session_readings`` (the
per-session charging-curve time series, each row linked by ``session_id``). Gold
is derived solely from the archive without reading prior gold state, so a re-run
over the same archive reproduces field-for-field equal facts (Requirements
9.1-9.3) — each run fully replaces the gold tables.

Depends on ``raw_events_archive``: it reads the archive table that asset writes,
so the dependency is declared with ``deps`` (the data is exchanged through the
shared DuckLake archive, not a Dagster IO manager).
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import (
    READINGS_TABLE,
    SESSIONS_TABLE,
    connect,
    read_archive_ducklake,
    reconstruct_sessions_and_readings,
    replace_table,
)

from .config import LAKE_CATALOG, LAKE_DATA
from .dump import raw_events_archive


@asset(
    deps=[raw_events_archive],
    group_name="gold",
    description="Gold session facts and per-session readings, reconstructed "
    "from the raw DuckLake archive via the shared sessionization fold.",
)
def gold_sessions(context: AssetExecutionContext) -> None:
    """Reconstruct session facts and readings from the archive into gold."""
    events = read_archive_ducklake(LAKE_CATALOG, LAKE_DATA)
    if events.is_empty():
        context.log.info("session: no archive rows in %s", LAKE_CATALOG)
        return
    sessions, readings = reconstruct_sessions_and_readings(events)
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        replace_table(con, SESSIONS_TABLE, sessions)
        replace_table(con, READINGS_TABLE, readings)
    context.log.info(
        "session: wrote %d sessions to %s, %d readings to %s",
        sessions.height,
        SESSIONS_TABLE,
        readings.height,
        READINGS_TABLE,
    )
