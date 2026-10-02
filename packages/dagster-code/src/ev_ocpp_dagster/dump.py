"""Dump-to-Parquet job: archive new landing rows to the cold store.

Reads the not-yet-archived slice (``event_id`` > Watermark_Cursor) from Postgres,
appends it as a new immutable Parquet file to the Cold_Archive, then advances the
watermark to the greatest ``event_id`` in that slice (Requirements 3.1-3.3). Rows
are archived before retention can drop them (3.4); archived files are never
updated in place (3.6). A re-run from the same watermark reproduces the same
files via the duplicate-tolerant fold downstream (12.4).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import polars as pl
from dagster import job, op

from .config import ARCHIVE_DIR, PG_URI, WATERMARK_PATH


def _read_watermark() -> int:
    if os.path.exists(WATERMARK_PATH):
        return int(open(WATERMARK_PATH).read().strip() or "0")
    return 0


def _write_watermark(value: int) -> None:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    with open(WATERMARK_PATH, "w") as fh:
        fh.write(str(value))


@op
def dump_to_parquet_op(context) -> None:
    watermark = _read_watermark()
    query = (
        "SELECT event_id, station_id, msg_type, unique_id, action, "
        f"payload::text AS payload, ingest_ts FROM raw_events WHERE event_id > {watermark} "
        "ORDER BY event_id"
    )
    df = pl.read_database_uri(query, PG_URI, engine="connectorx")
    if df.is_empty():
        context.log.info("dump: no new rows above watermark=%d", watermark)
        return
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    out = os.path.join(ARCHIVE_DIR, f"raw_{stamp}.parquet")
    df.write_parquet(out)  # new immutable file, never updated in place
    new_watermark = int(df["event_id"].max())
    _write_watermark(new_watermark)
    context.log.info(
        "dump: wrote %d rows to %s, watermark %d -> %d",
        df.height, out, watermark, new_watermark,
    )


@job
def dump_to_parquet_job() -> None:
    dump_to_parquet_op()
