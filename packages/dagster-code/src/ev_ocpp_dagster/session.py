"""Session-reconstruction job: rebuild gold session facts from the archive.

Reads raw events from the Cold_Archive through the DuckDB->Polars reader, runs
the shared ``reconstruct_sessions`` fold, and materializes session facts to
Parquet gold (Requirements 4.1, 4.7, 7.1). Gold is derived solely from the
archive without reading prior gold state, so a re-run over the same archive
reproduces field-for-field equal facts (Requirements 9.1-9.3). ``payload`` is a
nested struct in gold, so it is dropped before writing.
"""

from __future__ import annotations

import glob
import os

from dagster import job, op
from ev_ocpp_analysis import read_archive_duckdb, reconstruct_sessions

from .config import ARCHIVE_GLOB, GOLD_DIR, GOLD_PATH


@op
def reconstruct_sessions_op(context) -> None:
    if not glob.glob(ARCHIVE_GLOB):
        context.log.info("session: no archive files at %s", ARCHIVE_GLOB)
        return
    events = read_archive_duckdb(ARCHIVE_GLOB)
    sessions = reconstruct_sessions(events)
    os.makedirs(GOLD_DIR, exist_ok=True)
    sessions.write_parquet(GOLD_PATH)
    context.log.info("session: wrote %d sessions to %s", sessions.height, GOLD_PATH)


@job
def session_job() -> None:
    reconstruct_sessions_op()
