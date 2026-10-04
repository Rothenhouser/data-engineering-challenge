"""Daily per-connector analytics derived from gold sessions + raw events.

``compute_daily_stats`` rolls charging sessions up to one row per
``(charger_id, connector_id, day)`` and joins in a fault count from the raw
``StatusNotification`` frames (a fault is any StatusNotification whose
``errorCode`` is not ``NoError``).

Columns per (charger_id, connector_id, day):
- ``session_count``       — sessions started that day
- ``total_energy_kwh``    — energy delivered that day
- ``avg_power`` / ``peak_power``
- ``seconds_in_session``  — summed session duration (completed sessions only;
  open sessions have no duration)
- ``utilization_pct``     — share of the 24h day spent in a session
- ``fault_count``         — StatusNotification frames with errorCode != NoError
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from .sites import site_for

DAILY_COLUMNS = (
    "charger_id",
    "connector_id",
    "site_id",
    "day",
    "session_count",
    "total_energy_kwh",
    "avg_power",
    "peak_power",
    "seconds_in_session",
    "utilization_pct",
    "fault_count",
)
_SECONDS_PER_DAY = 86400.0
_FAULT_KEY = ["charger_id", "connector_id", "day"]


def _fault_counts(raw_events: pl.DataFrame) -> pl.DataFrame:
    """Per (charger_id, connector_id, day) count of StatusNotification faults.

    A fault is a StatusNotification Call whose payload ``errorCode`` is not
    ``NoError``. The connector comes from the payload ``connectorId`` and the day
    from the payload timestamp.
    """
    empty = pl.DataFrame(
        schema={
            "charger_id": pl.Utf8,
            "connector_id": pl.Int64,
            "day": pl.Date,
            "fault_count": pl.UInt32,
        }
    )
    if raw_events.is_empty():
        return empty
    sn = raw_events.filter(pl.col("action") == "StatusNotification")
    if sn.is_empty():
        return empty
    rows: list[dict] = []
    for r in sn.iter_rows(named=True):
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        if (payload.get("errorCode") or "NoError") == "NoError":
            continue
        # Parse the day in Python: payload timestamps are tz-aware ISO strings,
        # which Polars' str.to_datetime rejects without an explicit format.
        ts = payload.get("timestamp")
        day = None
        if isinstance(ts, str) and ts:
            try:
                day = datetime.fromisoformat(ts).date()
            except ValueError:
                day = None
        if day is not None:
            rows.append(
                {
                    "charger_id": r["charger_id"],
                    "connector_id": payload.get("connectorId"),
                    "day": day,
                }
            )
    if not rows:
        return empty
    return (
        pl.DataFrame(rows, schema={"charger_id": pl.Utf8, "connector_id": pl.Int64, "day": pl.Date})
        .group_by("charger_id", "connector_id", "day")
        .agg(pl.len().cast(pl.UInt32).alias("fault_count"))
    )


def compute_daily_stats(sessions: pl.DataFrame, raw_events: pl.DataFrame) -> pl.DataFrame:
    """Compute per-connector daily statistics from sessions + raw events."""
    faults = _fault_counts(raw_events)

    if sessions.is_empty():
        # Still surface fault-only days (a connector can fault without a session).
        if faults.is_empty():
            return pl.DataFrame(schema=dict.fromkeys(DAILY_COLUMNS, pl.Null))
        return faults.with_columns(
            pl.col("charger_id").map_elements(site_for, return_dtype=pl.Utf8).alias("site_id"),
            pl.lit(0).alias("session_count"),
            pl.lit(0.0).alias("total_energy_kwh"),
            pl.lit(0.0).alias("avg_power"),
            pl.lit(0.0).alias("peak_power"),
            pl.lit(0.0).alias("seconds_in_session"),
            pl.lit(0.0).alias("utilization_pct"),
        ).select(DAILY_COLUMNS)

    has_site = "site_id" in sessions.columns
    daily = (
        sessions.with_columns(pl.col("start_time").dt.date().alias("day"))
        .group_by("charger_id", "connector_id", "day")
        .agg(
            (pl.col("site_id").first() if has_site else pl.lit(None)).alias("site_id"),
            pl.len().cast(pl.UInt32).alias("session_count"),
            pl.col("total_energy_kwh").sum().round(3).alias("total_energy_kwh"),
            pl.col("avg_power").mean().round(2).alias("avg_power"),
            pl.col("peak_power").max().alias("peak_power"),
            pl.col("duration").sum().alias("seconds_in_session"),
        )
        .with_columns(
            (pl.col("seconds_in_session").fill_null(0.0) / _SECONDS_PER_DAY * 100.0)
            .round(2)
            .alias("utilization_pct")
        )
    )
    out = daily.join(faults, on=_FAULT_KEY, how="full", coalesce=True).with_columns(
        pl.col("fault_count").fill_null(0),
        pl.col("session_count").fill_null(0),
        pl.col("total_energy_kwh").fill_null(0.0),
        pl.col("utilization_pct").fill_null(0.0),
        # Fault-only days (no session) have no site_id from the left side.
        pl.col("site_id").fill_null(
            pl.col("charger_id").map_elements(site_for, return_dtype=pl.Utf8)
        ),
    )
    return out.select(DAILY_COLUMNS).sort("day", "charger_id", "connector_id")
