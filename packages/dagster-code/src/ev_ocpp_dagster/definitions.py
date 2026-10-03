"""The ``ev_ocpp`` Dagster code location.

Assembles the historical loader (sensor + op job) and the medallion asset
pipeline into one Definitions object the daemon ticks.

The pipeline is three assets chained by their ``deps``:

    raw_events_archive  ->  gold_sessions  ->  gold_analytics_daily

A single asset job, ``ocpp_pipeline_job``, materializes all three in dependency
order on a schedule. The loader keeps its own file-drop sensor and op job as the
ingestion entry point. Failed runs surface in the UI (Requirements 12.3, 14.3).
"""

from __future__ import annotations

from dagster import Definitions, ScheduleDefinition, define_asset_job

from .analytics import gold_analytics_daily
from .dump import raw_events_archive
from .loader import historical_file_sensor, load_file_job
from .session import gold_sessions

# One job over the whole archive -> sessions -> analytics chain. The assets'
# ``deps`` fix the run order; selecting all three runs them in sequence.
ocpp_pipeline_job = define_asset_job(
    name="ocpp_pipeline_job",
    selection=[raw_events_archive, gold_sessions, gold_analytics_daily],
)

pipeline_schedule = ScheduleDefinition(
    job=ocpp_pipeline_job, cron_schedule="*/5 * * * *", name="pipeline_schedule"
)

defs = Definitions(
    assets=[raw_events_archive, gold_sessions, gold_analytics_daily],
    jobs=[load_file_job, ocpp_pipeline_job],
    sensors=[historical_file_sensor],
    schedules=[pipeline_schedule],
)
