"""Analytics asset: roll gold sessions up into daily per-charger statistics.

Reads the gold session facts and the raw Parquet archive (for fault counts via
StatusNotification), computes one row per (station_id, day) with the shared
``compute_daily_stats``, and materializes it to a single ``analytics_daily.parquet``
gold file. Pure function of its inputs, so re-runs are reproducible.

Depends on ``gold_sessions`` (it reads ``sessions.parquet``) and on
``raw_events_archive`` (it reads the archive for fault counts); both are declared
with ``deps`` since the data flows through shared Parquet files, not a Dagster IO
manager.
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import glob
import os

import polars as pl
from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import compute_daily_stats, read_archive_duckdb

from .config import ARCHIVE_GLOB, GOLD_ANALYTICS_PATH, GOLD_DIR, GOLD_PATH
from .dump import raw_events_archive
from .session import gold_sessions


@asset(
    deps=[gold_sessions, raw_events_archive],
    group_name="gold",
    description="Daily per-charger analytics rolled up from gold sessions and "
    "archive fault events; one row per (station_id, day).",
)
def gold_analytics_daily(context: AssetExecutionContext) -> None:
    """Roll gold sessions up into one row per (station_id, day)."""
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
