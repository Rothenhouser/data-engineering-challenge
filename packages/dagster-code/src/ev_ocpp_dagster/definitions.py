"""The ``ev_ocpp`` Dagster code location.

Assembles the historical loader sensor, the daily-partitioned dump-to-Parquet
asset job, and the session-reconstruction job into one Definitions object the
daemon ticks. The dump archive asset is driven by a partitioned schedule (one
tick per day partition); the session job runs on a plain schedule. Failed runs
surface in the UI (Requirements 12.3, 14.3).
"""

from __future__ import annotations

from dagster import (
    Definitions,
    ScheduleDefinition,
    build_schedule_from_partitioned_job,
)

from .analytics import analytics_job
from .dump import dump_to_parquet_job, raw_events_archive
from .loader import historical_file_sensor, load_file_job
from .session import session_job

# One tick per day partition of the archive asset job.
dump_schedule = build_schedule_from_partitioned_job(
    dump_to_parquet_job, name="dump_schedule"
)
session_schedule = ScheduleDefinition(
    job=session_job, cron_schedule="*/10 * * * *", name="session_schedule"
)
# Analytics runs shortly after the session rebuild so it reads fresh gold.
analytics_schedule = ScheduleDefinition(
    job=analytics_job, cron_schedule="2-59/10 * * * *", name="analytics_schedule"
)

defs = Definitions(
    assets=[raw_events_archive],
    jobs=[load_file_job, dump_to_parquet_job, session_job, analytics_job],
    sensors=[historical_file_sensor],
    schedules=[dump_schedule, session_schedule, analytics_schedule],
)
