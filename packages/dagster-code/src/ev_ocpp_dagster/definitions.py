"""The ``ev_ocpp`` Dagster code location.

Assembles the historical loader sensor, the dump-to-Parquet job, and the
session-reconstruction job into one Definitions object the daemon ticks. The
dump and session jobs run on schedules; failed runs surface in the UI
(Requirements 12.3, 14.3).
"""

from __future__ import annotations

from dagster import Definitions, ScheduleDefinition

from .dump import dump_to_parquet_job
from .loader import historical_file_sensor, load_file_job
from .session import session_job

dump_schedule = ScheduleDefinition(
    job=dump_to_parquet_job, cron_schedule="*/5 * * * *", name="dump_schedule"
)
session_schedule = ScheduleDefinition(
    job=session_job, cron_schedule="*/10 * * * *", name="session_schedule"
)

defs = Definitions(
    jobs=[load_file_job, dump_to_parquet_job, session_job],
    sensors=[historical_file_sensor],
    schedules=[dump_schedule, session_schedule],
)
