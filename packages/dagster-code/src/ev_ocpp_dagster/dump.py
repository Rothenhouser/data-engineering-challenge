"""Dump-to-archive asset: archive new landing rows to the cold DuckLake store.

The archive is modelled as a single Dagster asset. Materializing it reads the
not-yet-archived slice (``event_id`` > Watermark_Cursor) from Postgres and
appends it to the immutable cold DuckLake archive table as a new DuckLake
snapshot/append (Requirements 3.1-3.3). The watermark is derived from the
catalog itself — the greatest ``event_id`` already archived — so no separate
cursor file is needed. Rows are archived before retention can drop them (3.4);
archived snapshots are never updated in place (3.6). A re-run from the same
watermark reproduces the same rows via the duplicate-tolerant fold downstream
(12.4).

NOTE: this asset is intentionally unpartitioned for now. Daily partitioning is
still open: a day partition could mean "events whose OCPP payload time falls on
that day" (but not every frame is timestamped, and facts can arrive late) or
"rows ingested during that day" (more robust, but harder to reconcile and
entangled with simulated wall-clock time). Until that is settled, the asset
archives all currently-available data.
"""

# NOTE: no `from __future__ import annotations` here — Dagster validates the real
# AssetExecutionContext type hint on the asset fn, not a stringized annotation.

import polars as pl
from dagster import AssetExecutionContext, asset
from ev_ocpp_analysis import ARCHIVE_TABLE, append_frame, connect

from .config import LAKE_CATALOG, LAKE_DATA, PG_URI

# Unqualified name of the archive table within the DuckLake catalog, used to
# check existence before querying it.
_ARCHIVE_TABLE_NAME = ARCHIVE_TABLE.rsplit(".", 1)[-1]


@asset(
    group_name="archive",
    description="Immutable DuckLake archive of raw OCPP events. Appends the "
    "slice above the event_id watermark as a new snapshot on each materialization.",
)
def raw_events_archive(context: AssetExecutionContext) -> None:
    """Append the not-yet-archived slice of ``raw_events`` to the Cold_Archive."""
    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        # Derive the watermark from the catalog. On the first run the archive
        # table does not exist yet; querying it would make DuckDB attempt a
        # replacement scan (finding the asset's own Python global of the same
        # name) rather than raise a catalog error, so check existence explicitly
        # instead of catching an exception.
        exists = con.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog = 'lake' AND table_name = ? LIMIT 1",
            [_ARCHIVE_TABLE_NAME],
        ).fetchone()
        if exists:
            row = con.execute(f"SELECT max(event_id) FROM {ARCHIVE_TABLE}").fetchone()
            watermark = int(row[0]) if row and row[0] is not None else 0
        else:
            watermark = 0

        query = (
            "SELECT event_id, charger_id, msg_type, unique_id, action, "
            f"payload::text AS payload, ingest_ts FROM raw_events WHERE event_id > {watermark} "
            "ORDER BY event_id"
        )
        df = pl.read_database_uri(query, PG_URI, engine="connectorx")
        if df.is_empty():
            context.log.info("archive: no new rows above watermark=%d", watermark)
            return
        append_frame(con, ARCHIVE_TABLE, df)
        new_watermark = int(df["event_id"].max())
        context.log.info(
            "archive: appended %d rows to %s, watermark %d -> %d",
            df.height,
            ARCHIVE_TABLE,
            watermark,
            new_watermark,
        )
