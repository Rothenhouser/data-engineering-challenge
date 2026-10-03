"""Analytics job: roll gold sessions up into daily per-charger statistics.

Reads the gold session facts and the raw Parquet archive (for fault counts via
StatusNotification), computes one row per (station_id, day) with the shared
``compute_daily_stats``, and materializes it to a single ``analytics_daily.parquet``
gold file. Pure function of its inputs, so re-runs are reproducible.
"""

from __future__ import annotations

import glob
import os

import polars as pl
from dagster import job, op
from ev_ocpp_analysis import compute_daily_stats, read_archive_duckdb

from .config import ARCHIVE_GLOB, GOLD_ANALYTICS_PATH, GOLD_DIR, GOLD_PATH


@op
def compute_daily_stats_op(context) -> None:
    if not os.path.exists(GOLD_PATH):
        context.log.info("analytics: no gold sessions at %s", GOLD_PATH)
        return
    sessions = pl.read_parquet(GOLD_PATH)
    raw = read_archive_duckdb(ARCHIVE_GLOB) if glob.glob(ARCHIVE_GLOB) else pl.DataFrame()
    daily = compute_daily_stats(sessions, raw)
    os.makedirs(GOLD_DIR, exist_ok=True)
    daily.write_parquet(GOLD_ANALYTICS_PATH)
    context.log.info(
        "analytics: wrote %d daily rows to %s", daily.height, GOLD_ANALYTICS_PATH
    )


@job
def analytics_job() -> None:
    compute_daily_stats_op()
