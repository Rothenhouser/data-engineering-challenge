"""Session-reconstruction asset: rebuild gold session facts from the archive.

Reads raw events from the Cold_Archive through the DuckDB->Polars reader, runs
the shared fold, and materializes two gold tables (Requirements 4.1, 4.7, 7.1):
``sessions.parquet`` (session facts) and ``session_readings.parquet`` (the
per-session charging-curve time series, each row linked by ``session_id``). Gold
is derived solely from the archive without reading prior gold state, so a re-run
over the same archive reproduces field-for-field equal facts (Requirements
9.1-9.3).

Depends on ``raw_events_archive``: it reads the archive files that asset writes,
so the dependency is declared with ``deps`` (the data is exchanged through the
shared Parquet archive, not a Dagster IO manager).
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import glob
import os

from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import read_archive_duckdb, reconstruct_sessions_and_readings

from .config import ARCHIVE_GLOB, GOLD_DIR, GOLD_PATH, GOLD_READINGS_PATH
from .dump import raw_events_archive


@asset(
    deps=[raw_events_archive],
    group_name="gold",
    description="Gold session facts and per-session readings, reconstructed "
    "from the raw Parquet archive via the shared sessionization fold.",
)
def gold_sessions(context: AssetExecutionContext) -> None:
    """Reconstruct session facts and readings from the archive into gold."""
    if not glob.glob(ARCHIVE_GLOB):
        context.log.info("session: no archive files at %s", ARCHIVE_GLOB)
        return
    events = read_archive_duckdb(ARCHIVE_GLOB)
    sessions, readings = reconstruct_sessions_and_readings(events)
    os.makedirs(GOLD_DIR, exist_ok=True)
    sessions.write_parquet(GOLD_PATH)
    readings.write_parquet(GOLD_READINGS_PATH)
    context.log.info(
        "session: wrote %d sessions to %s, %d readings to %s",
        sessions.height, GOLD_PATH, readings.height, GOLD_READINGS_PATH,
    )
