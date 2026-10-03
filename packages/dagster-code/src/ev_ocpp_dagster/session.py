"""Session-reconstruction job: rebuild gold session facts from the archive.

Reads raw events from the Cold_Archive through the DuckDB->Polars reader, runs
the shared fold, and materializes two gold tables (Requirements 4.1, 4.7, 7.1):
``sessions.parquet`` (session facts) and ``session_readings.parquet`` (the
per-session charging-curve time series, each row linked by ``session_id``). Gold
is derived solely from the archive without reading prior gold state, so a re-run
over the same archive reproduces field-for-field equal facts (Requirements
9.1-9.3).
"""

from __future__ import annotations

import glob
import os

from dagster import job, op
from ev_ocpp_analysis import read_archive_duckdb, reconstruct_sessions_and_readings

from .config import ARCHIVE_GLOB, GOLD_DIR, GOLD_PATH, GOLD_READINGS_PATH


@op
def reconstruct_sessions_op(context) -> None:
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


@job
def session_job() -> None:
    reconstruct_sessions_op()
