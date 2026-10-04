"""Daily partition definitions for the medallion pipeline.

Two independent time grains (see README "Future improvements"):

- ``INGESTION_PARTITIONS`` — the raw archive is partitioned by *ingestion day*
  (``ingest_ts``, real wall-clock), open-ended from 2026-10-01. The archive
  asset job updates the current day's partition on a schedule.
- ``CONTENT_PARTITIONS`` — the gold layer (sessions, analytics) is partitioned
  by *content day* (session ``start_time``), a static closed range covering the
  demonstrator's sample data. The session job writes each session into its
  start-time-day partition and fans a materialization out per touched day.
"""

from __future__ import annotations

from dagster import DailyPartitionsDefinition

# Ingestion-day partitions for the raw archive: real wall-clock, open-ended.
# end_offset=1 makes the *current* day a valid partition (a daily def otherwise
# only exposes days that have fully elapsed), so the 5-minute schedule can
# materialize today.
INGESTION_PARTITIONS = DailyPartitionsDefinition(start_date="2026-10-01", end_offset=1)

# Content-day partitions for gold: session start-time day.
# Hard-coded for demo just to make UI cleaner.
CONTENT_PARTITIONS = DailyPartitionsDefinition(start_date="2025-08-20", end_date="2025-09-01")
