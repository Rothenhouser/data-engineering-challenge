"""Tests for the partition-scoped DuckLake write helper."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
from ev_ocpp_analysis import connect, replace_partition
from ev_ocpp_analysis.ducklake import SCHEMA

_TABLE = f"lake.{SCHEMA}.t"


def _frame(day: str, ids: list[int]) -> pl.DataFrame:
    y, m, d = (int(p) for p in day.split("-"))
    ts = datetime(y, m, d, tzinfo=UTC)
    return pl.DataFrame(
        {"ingest_ts": [ts] * len(ids), "id": ids},
        schema={"ingest_ts": pl.Datetime(time_zone="UTC"), "id": pl.Int64},
    )


def test_replace_partition_is_idempotent_and_isolated(tmp_path) -> None:
    """Re-writing a day replaces only that day; other days are untouched."""
    catalog = str(tmp_path / "catalog.ducklake")
    data = str(tmp_path / "data")

    with connect(catalog, data) as con:
        replace_partition(con, _TABLE, "ingest_ts", "2026-10-01", _frame("2026-10-01", [1, 2]))
        replace_partition(con, _TABLE, "ingest_ts", "2026-10-02", _frame("2026-10-02", [3]))
        # Re-write day 1 with different rows: day 1 replaced, day 2 intact.
        replace_partition(con, _TABLE, "ingest_ts", "2026-10-01", _frame("2026-10-01", [9]))

        rows = con.execute(f"SELECT id FROM {_TABLE} ORDER BY id").fetchall()

    assert [r[0] for r in rows] == [3, 9]
