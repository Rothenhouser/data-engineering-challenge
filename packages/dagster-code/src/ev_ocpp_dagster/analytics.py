"""Analytics asset: roll gold sessions up into daily per-charger statistics.

Reads the gold session facts and the raw DuckLake archive (for fault counts via
StatusNotification), computes one row per (charger_id, day) with the shared
``compute_daily_stats``, and materializes it to the ``gold_analytics_daily``
DuckLake table. Pure function of its inputs, so re-runs are reproducible — the
table is fully replaced each run.

Depends on ``gold_sessions`` (it reads the gold sessions table) and on
``raw_events_archive`` (it reads the archive for fault counts); both are declared
with ``deps`` since the data flows through shared DuckLake tables, not a Dagster
IO manager.
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import (
    ANALYTICS_TABLE,
    SESSIONS_TABLE,
    compute_daily_stats,
    connect,
    read_archive_ducklake,
    read_gold_table,
    replace_table,
)

from .config import LAKE_CATALOG, LAKE_DATA
from .dump import raw_events_archive
from .session import gold_sessions


@asset(
    deps=[gold_sessions, raw_events_archive],
    group_name="gold",
    description="Daily per-charger analytics rolled up from gold sessions and "
    "archive fault events; one row per (charger_id, day).",
)
def gold_analytics_daily(context: AssetExecutionContext) -> None:
    """Roll gold sessions up into one row per (charger_id, day)."""
    sessions = read_gold_table(LAKE_CATALOG, LAKE_DATA, SESSIONS_TABLE)
    if sessions.is_empty():
        context.log.info("analytics: no gold sessions in %s", SESSIONS_TABLE)
        return
    raw = read_archive_ducklake(LAKE_CATALOG, LAKE_DATA)
    daily = compute_daily_stats(sessions, raw)
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        replace_table(con, ANALYTICS_TABLE, daily)
    context.log.info("analytics: wrote %d daily rows to %s", daily.height, ANALYTICS_TABLE)
