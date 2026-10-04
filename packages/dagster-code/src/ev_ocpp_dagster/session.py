"""Session-reconstruction asset: rebuild gold session facts per content day.

Reads the **full** DuckLake archive, runs the shared sessionization fold, then
writes only the sessions whose ``start_time`` falls on this asset's content-day
partition into the gold tables (Requirements 4.1, 4.7, 7.1): ``gold_sessions``
(session facts) and ``gold_session_readings`` (the per-session charging curve).

Reading the whole archive each run is deliberate for the demonstrator (Option C
— see README "Future improvements"): correctness is trivial (a content day is a
pure function of all events, so late/batch data arriving for any past day is
handled), at the cost of re-folding the full archive. The per-partition write is
idempotent: the day's sessions are replaced, and that day's readings are deleted
by ``session_id`` before re-insert, so re-running a content day never
duplicates. Gold is derived solely from the archive, so a re-run reproduces
field-for-field equal facts (Requirements 9.1-9.3).

Depends on ``raw_events_archive`` (reads its DuckLake table, so declared with
``deps`` — data flows through shared DuckLake tables, not a Dagster IO manager).
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import polars as pl
from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import (
    READINGS_TABLE,
    SESSIONS_TABLE,
    connect,
    read_archive_ducklake,
    reconstruct_sessions_and_readings,
    replace_partition,
)

from .config import LAKE_CATALOG, LAKE_DATA
from .dump import raw_events_archive
from .partitions import CONTENT_PARTITIONS


@asset(
    deps=[raw_events_archive],
    partitions_def=CONTENT_PARTITIONS,
    group_name="gold",
    description="Gold session facts and per-session readings for one content "
    "day (session start-time day), folded from the full raw DuckLake archive.",
)
def gold_sessions(context: AssetExecutionContext) -> None:
    """Reconstruct this content-day partition's sessions + readings into gold."""
    day = context.partition_key  # ISO 'YYYY-MM-DD'
    events = read_archive_ducklake(LAKE_CATALOG, LAKE_DATA)
    if events.is_empty():
        context.log.info("session: no archive rows in %s", LAKE_CATALOG)
        return

    sessions, readings = reconstruct_sessions_and_readings(events)
    # Keep only sessions whose start-time day is this partition.
    day_sessions = sessions.filter(pl.col("start_time").dt.date().cast(pl.Utf8) == day)
    day_ids = day_sessions["session_id"].to_list()
    day_readings = (
        readings.filter(pl.col("session_id").is_in(day_ids)) if day_ids else readings.clear()
    )

    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        replace_partition(con, SESSIONS_TABLE, "start_time", day, day_sessions)
        _replace_readings(con, day_ids, day_readings)
    context.log.info(
        "session: wrote %d sessions / %d readings for content day %s",
        day_sessions.height,
        day_readings.height,
        day,
    )


def _replace_readings(con, session_ids: list[str], readings: pl.DataFrame) -> None:
    """Idempotently replace this day's readings, keyed by ``session_id``.

    Readings carry no day column, so the partition is identified by the set of
    session_ids started on the day: delete those sessions' existing readings,
    then insert the fresh ones. ``readings`` is bound by name in the INSERT.
    """
    from ev_ocpp_analysis.ducklake import _table_exists

    if not _table_exists(con, READINGS_TABLE):
        con.execute(f"CREATE TABLE {READINGS_TABLE} AS SELECT * FROM readings")
        return
    if session_ids:
        con.execute(f"DELETE FROM {READINGS_TABLE} WHERE session_id = ANY(?)", [session_ids])
    con.execute(f"INSERT INTO {READINGS_TABLE} BY NAME SELECT * FROM readings")
