"""Dump-to-archive asset: archive one ingestion day's landing rows to DuckLake.

The archive is a DuckLake asset partitioned by **ingestion day** (the real
wall-clock ``ingest_ts``). Materializing a partition reads that day's slice from
Postgres (``ingest_ts`` in ``[day, day+1)``) and writes it into the archive table
for that day via ``replace_partition`` — a delete-day-then-insert, so re-running
the current day's partition every few minutes never duplicates rows
(Requirements 3.1-3.3). Archived data is otherwise immutable: a given day is only
ever rewritten by re-reading that same day's landing rows (3.6).

Partitioning by ingestion day caps the per-run input to one day of data and lets
downstream jobs reprocess only the days that changed (see README "Future
improvements").
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import polars as pl
from dagster import AssetExecutionContext, AssetSpec, asset
from ev_ocpp_analysis import ARCHIVE_TABLE, connect, replace_partition

from .config import LAKE_CATALOG, LAKE_DATA, PG_URI
from .partitions import INGESTION_PARTITIONS

# The Postgres ``raw_events`` landing zone, modeled as an external **source
# asset**: Dagster does not produce it (the stream consumer and the historical
# loader write it imperatively), it only observes/depends on it. Declaring it as
# an AssetSpec gives it a node in the asset graph so the archive can list it as
# an upstream dep and the loader can report materializations against it.
raw_events = AssetSpec(
    key="raw_events",
    group_name="landing",
    description="Append-only Postgres landing zone for raw OCPP events, written "
    "by the stream consumer and the historical file loader (external to Dagster).",
)


@asset(
    partitions_def=INGESTION_PARTITIONS,
    deps=[raw_events],
    group_name="archive",
    description="Immutable DuckLake archive of raw OCPP events, partitioned by "
    "ingestion day. Each run idempotently replaces its day's slice from Postgres.",
)
def raw_events_archive(context: AssetExecutionContext) -> None:
    """Archive the ingestion-day partition's slice of ``raw_events``."""
    day = context.partition_key  # ISO 'YYYY-MM-DD'
    query = (
        "SELECT event_id, charger_id, msg_type, unique_id, action, "
        "payload::text AS payload, ingest_ts FROM raw_events "
        f"WHERE ingest_ts >= '{day}' AND ingest_ts < '{day}'::date + 1 "
        "ORDER BY event_id"
    )
    df = pl.read_database_uri(query, PG_URI, engine="connectorx")
    if df.is_empty():
        context.log.info("archive: no rows ingested on %s", day)
        return
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        replace_partition(con, ARCHIVE_TABLE, "ingest_ts", day, df)
    context.log.info("archive: wrote %d rows for ingestion day %s", df.height, day)
