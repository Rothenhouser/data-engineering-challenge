"""Analytics asset: roll one content day's gold sessions into daily stats.

Partitioned by content day (session start-time day) and driven by an eager
automation condition, so when ``gold_sessions`` materializes a content-day
partition this asset re-materializes the same day automatically.

Reads this day's gold sessions and the raw DuckLake archive (for
StatusNotification fault counts), computes per-``(charger_id, connector_id, day)``
rows via the shared ``compute_daily_stats``, keeps only this partition's day, and
writes them into the ``gold_analytics_daily`` table for the day via
``replace_partition`` — a pure function of its inputs, so re-runs are
reproducible.

Depends on ``gold_sessions`` (reads the gold sessions table) and
``raw_events_archive`` (reads the archive for faults); both via ``deps`` since
the data flows through shared DuckLake tables, not a Dagster IO manager.
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import polars as pl
from dagster import AssetExecutionContext, AutomationCondition, asset
from ev_ocpp_analysis import (
    ANALYTICS_TABLE,
    SESSIONS_TABLE,
    compute_daily_stats,
    connect,
    read_archive_ducklake,
    read_gold_table,
    replace_partition,
)

from .config import LAKE_CATALOG, LAKE_DATA
from .dump import raw_events_archive
from .partitions import CONTENT_PARTITIONS
from .session import gold_sessions


@asset(
    deps=[gold_sessions, raw_events_archive],
    partitions_def=CONTENT_PARTITIONS,
    # eager, but without the default "latest time window" gate — the content
    # partitions are historical (2025-08), so that gate would permanently skip
    # them. We want any updated content day re-materialized regardless of age.
    automation_condition=AutomationCondition.eager().without(
        AutomationCondition.in_latest_time_window()
    ),
    group_name="gold",
    description="Daily per-connector analytics for one content day, rolled up "
    "from that day's gold sessions and archive fault events.",
)
def gold_analytics_daily(context: AssetExecutionContext) -> None:
    """Roll this content day's sessions up into per-connector daily rows."""
    day = context.partition_key  # ISO 'YYYY-MM-DD'
    sessions = read_gold_table(LAKE_CATALOG, LAKE_DATA, SESSIONS_TABLE)
    day_sessions = (
        sessions.filter(pl.col("start_time").dt.date().cast(pl.Utf8) == day)
        if not sessions.is_empty()
        else sessions
    )
    if day_sessions.is_empty():
        context.log.info("analytics: no gold sessions for content day %s", day)
        return

    raw = read_archive_ducklake(LAKE_CATALOG, LAKE_DATA)
    daily = compute_daily_stats(day_sessions, raw)
    # Faults are dated by payload time inside compute_daily_stats; keep only this
    # partition's day so faults on other days don't leak in as extra rows.
    daily = daily.filter(pl.col("day").cast(pl.Utf8) == day)
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        replace_partition(con, ANALYTICS_TABLE, "day", day, daily)
    context.log.info("analytics: wrote %d daily rows for content day %s", daily.height, day)
