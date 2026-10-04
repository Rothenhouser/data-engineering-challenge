"""The ``ev_ocpp`` Dagster code location.

Two partitioned grains split the pipeline (see README "Future improvements"):

    raw_events_archive            (ingestion-day partitions)
       └─> gold_sessions          (content-day partitions)
             └─> gold_analytics_daily  (content-day, eager automation)

Jobs:
- ``archive_job`` materializes the current ingestion-day partition of
  ``raw_events_archive`` on a 5-minute schedule.
- ``session_job`` materializes one content-day partition of ``gold_sessions``.
- ``archive_to_sessions_sensor`` watches ``raw_events_archive`` materializations
  and requests ``session_job`` runs for every content day the newly-archived
  ingestion day touched (a fan-out), clamped to the content partition range.
- ``gold_analytics_daily`` carries an eager ``AutomationCondition``, so it
  re-materializes a content day automatically once its sessions update; an
  automation-condition sensor drives it.

The loader keeps its own file-drop sensor + op job as the ingestion entry point.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dagster import (
    AssetKey,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    EventLogEntry,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    asset_sensor,
    define_asset_job,
    schedule,
)

from .analytics import gold_analytics_daily
from .config import LAKE_CATALOG, LAKE_DATA
from .dump import raw_events_archive
from .loader import historical_file_sensor, load_file_job
from .partitions import CONTENT_PARTITIONS
from .session import gold_sessions

# --- Jobs -------------------------------------------------------------------

archive_job = define_asset_job(name="archive_job", selection=[raw_events_archive])

session_job = define_asset_job(name="session_job", selection=[gold_sessions])


# Materialize the current ingestion-day partition every 5 minutes. A plain
# schedule (not build_schedule_from_partitioned_job, which forces the daily
# partition cadence) lets the sub-daily cron re-run today's partition.
@schedule(
    job=archive_job,
    cron_schedule="*/5 * * * *",
    name="archive_schedule",
    default_status=DefaultScheduleStatus.RUNNING,
)
def archive_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    today = datetime.now(UTC).date().isoformat()
    return RunRequest(partition_key=today)


# --- Archive -> sessions fan-out sensor -------------------------------------


def _content_days_in_ingestion_partition(ingestion_day: str) -> list[str]:
    """Distinct session-start content days present in one ingestion-day slice.

    Reads the archive rows ingested on ``ingestion_day`` and returns the distinct
    StartTransaction payload-timestamp days (ISO strings) that fall inside the
    content partition range. These are the content-day partitions whose sessions
    the new data may have changed.
    """
    from ev_ocpp_analysis import ARCHIVE_TABLE, connect
    from ev_ocpp_analysis.ducklake import _table_exists

    lo, hi = CONTENT_PARTITIONS.start, CONTENT_PARTITIONS.end  # tz-aware bounds
    sql = (
        "SELECT DISTINCT CAST(json_extract_string(payload, '$.timestamp') AS DATE) AS d "
        f"FROM {ARCHIVE_TABLE} "
        "WHERE action = 'StartTransaction' "
        "  AND ingest_ts >= ?::date AND ingest_ts < ?::date + 1 "
        "  AND json_extract_string(payload, '$.timestamp') IS NOT NULL"
    )
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        if not _table_exists(con, ARCHIVE_TABLE):
            return []
        rows = con.execute(sql, [ingestion_day, ingestion_day]).fetchall()
    days = []
    for (d,) in rows:
        if d is None:
            continue
        iso = d.isoformat()
        # Clamp to the content partition range; skip out-of-range days.
        if lo.date().isoformat() <= iso <= (hi.date().isoformat()):
            days.append(iso)
    return sorted(set(days))


@asset_sensor(
    asset_key=AssetKey("raw_events_archive"),
    job=session_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def archive_to_sessions_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    """Fan a session_job run out to each content day the archived day touched."""
    mat = (
        asset_event.dagster_event
        and asset_event.dagster_event.event_specific_data
        and asset_event.dagster_event.event_specific_data.materialization
    )
    ingestion_day = getattr(mat, "partition", None)
    if not ingestion_day:
        return
    content_days = _content_days_in_ingestion_partition(ingestion_day)
    if not content_days:
        context.log.info("sensor: archive %s touched no in-range content days", ingestion_day)
        return
    for day in content_days:
        yield RunRequest(
            run_key=f"{ingestion_day}:{day}",
            partition_key=day,
        )


# Drives the eager AutomationCondition on gold_analytics_daily.
automation_sensor = AutomationConditionSensorDefinition(
    name="automation_sensor",
    target=[gold_analytics_daily],
    default_status=DefaultSensorStatus.RUNNING,
)


defs = Definitions(
    assets=[raw_events_archive, gold_sessions, gold_analytics_daily],
    jobs=[load_file_job, archive_job, session_job],
    sensors=[
        historical_file_sensor,
        archive_to_sessions_sensor,
        automation_sensor,
    ],
    schedules=[archive_schedule],
)
