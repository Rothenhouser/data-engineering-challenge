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
from .partitions import CONTENT_PARTITIONS
from .session import gold_sessions


@asset(
    deps=[
        gold_sessions,
        # Does depend on the raw archive, but listing it as a dep (or via a
        # partition mapping) prevents eager automation. TODO: a separate
        # content-partitioned asset for raw faults, then add raw_events_archive.
    ],
    partitions_def=CONTENT_PARTITIONS,
    automation_condition=AutomationCondition.eager().without(
        AutomationCondition.in_latest_time_window()
    ),
    group_name="gold",
    description="Daily per-connector analytics for one content day, rolled up "
    "from that day's gold sessions and archive fault events.",
)
def gold_analytics_daily(context: AssetExecutionContext) -> None:
    """Aggregate this content day's sessions into per-connector rows."""
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
