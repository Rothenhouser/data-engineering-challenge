"""Streamlit dashboard: live status, in-flight sessions, historical analytics.

Reads two stores through the shared library:
- Postgres (hot landing) for the live status snapshot and the in-flight session
  view, the latter reusing the shared ``reconstruct_sessions`` fold over a
  bounded recent window (Requirements 8.1-8.4, 7.4).
- Parquet gold for completed-session history and per-station / per-day fleet
  rollups computed on the fly in Polars (Requirement 8.5).

Time-range and station filters apply across the views (Requirements 8.6, 8.7).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
import streamlit as st
from ev_ocpp_analysis import (
    read_latest_metervalues_per_station_postgres,
    read_latest_per_station_postgres,
    read_recent_window_postgres,
    reconstruct_sessions,
)
from streamlit_autorefresh import st_autorefresh

# Streamlit runs this file as a top-level script (no package context), so import
# config by absolute module path rather than a relative import.
from ev_ocpp_dashboard.config import GOLD_PATH, PG_URI, WINDOW_MINUTES


def _measurand(payload: dict[str, Any], name: str) -> float | None:
    """Pull a single measurand value from a MeterValues payload."""
    for mv in payload.get("meterValue") or []:
        for sv in mv.get("sampledValue") or []:
            if sv.get("measurand") == name:
                try:
                    return float(sv["value"])
                except KeyError, TypeError, ValueError:
                    return None
    return None


@st.cache_data(ttl=5)
def _latest_per_station() -> pl.DataFrame:
    # Latest event per station gives the status + ingest_ts, but it is usually a
    # Heartbeat/ack with no readings — so pull the latest MeterValues separately
    # for the power/SoC columns and join them in.
    df = read_latest_per_station_postgres(PG_URI)
    if df.is_empty():
        return df
    status = df.select("station_id", "action", "ingest_ts")

    mv = read_latest_metervalues_per_station_postgres(PG_URI)
    if mv.is_empty():
        readings = pl.DataFrame(
            {"station_id": [], "power_kw": [], "soc_pct": []},
            schema={"station_id": pl.Utf8, "power_kw": pl.Float64, "soc_pct": pl.Float64},
        )
    else:
        payloads = mv["payload"].to_list()
        readings = mv.select("station_id").with_columns(
            pl.Series("power_kw", [_measurand(p, "Power.Active.Import") for p in payloads]),
            pl.Series("soc_pct", [_measurand(p, "SoC") for p in payloads]),
        )
    return status.join(readings, on="station_id", how="left").select(
        "station_id", "action", "power_kw", "soc_pct", "ingest_ts"
    )


@st.cache_data(ttl=5)
def _inflight_sessions(window_minutes: int) -> pl.DataFrame:
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    events = read_recent_window_postgres(PG_URI, since)
    if events.is_empty():
        return pl.DataFrame()
    sessions = reconstruct_sessions(events)
    return sessions.filter(pl.col("status") == "active")


@st.cache_data(ttl=30)
def _gold_sessions() -> pl.DataFrame:
    if not os.path.exists(GOLD_PATH):
        return pl.DataFrame()
    return pl.read_parquet(GOLD_PATH)


def _apply_filters(df: pl.DataFrame, stations: list[str], start, end, ts_col: str) -> pl.DataFrame:
    if df.is_empty():
        return df
    if stations:
        df = df.filter(pl.col("station_id").is_in(stations))
    if ts_col in df.columns:
        df = df.filter(
            (pl.col(ts_col) >= datetime.combine(start, datetime.min.time(), UTC))
            & (pl.col(ts_col) <= datetime.combine(end, datetime.max.time(), UTC))
        )
    return df


def main() -> None:
    st.set_page_config(page_title="EV OCPP Dashboard", layout="wide")
    st.title("EV charging — OCPP dashboard")

    gold = _gold_sessions()
    live = _latest_per_station()

    # --- filters (apply across views) --------------------------------------
    stations = sorted(
        set(gold["station_id"].to_list() if not gold.is_empty() else [])
        | set(live["station_id"].to_list() if not live.is_empty() else [])
    )
    with st.sidebar:
        st.header("Filters")
        picked = st.multiselect("Station", stations)
        today = datetime.now(UTC).date()
        start = st.date_input("From", today - timedelta(days=7))
        end = st.date_input("To", today)

        st.header("Auto-refresh")
        auto = st.toggle("Enabled", value=True)
        every = st.slider("Interval (s)", 2, 60, 5, disabled=not auto)
        if auto:
            # Rerun the whole script on a timer; the cached readers (ttl=5s)
            # keep it cheap, so live views stay current without manual reload.
            st_autorefresh(interval_ms=every * 1000, key="auto_refresh")

    live_f = _apply_filters(live, picked, start, end, "ingest_ts")
    gold_f = _apply_filters(gold, picked, start, end, "start_time")

    # --- live status --------------------------------------------------------
    st.subheader("Live status (latest event per charger)")
    if live_f.is_empty():
        st.info("No live events — is the stream consumer running against Postgres?")
    else:
        st.dataframe(live_f, width="stretch")

    # --- in-flight sessions -------------------------------------------------
    st.subheader(f"In-flight sessions (last {WINDOW_MINUTES} min)")
    inflight = _apply_filters(_inflight_sessions(WINDOW_MINUTES), picked, start, end, "start_time")
    if inflight.is_empty():
        st.info("No active sessions in the recent window.")
    else:
        now = datetime.now(UTC)
        inflight = inflight.with_columns(
            (pl.lit(now) - pl.col("start_time")).dt.total_seconds().alias("running_seconds")
        )
        st.dataframe(
            inflight.select(
                "station_id",
                "connector_id",
                "running_seconds",
                "total_energy_kwh",
                "avg_power",
                "peak_power",
            ),
            width="stretch",
        )

    # --- historical analytics ----------------------------------------------
    st.subheader("Historical sessions & fleet rollups")
    if gold_f.is_empty():
        st.info("No gold session facts yet — run the Dagster session job.")
        return
    st.dataframe(gold_f, width="stretch")

    per_station = (
        gold_f.group_by("station_id")
        .agg(
            pl.len().alias("sessions"),
            pl.col("total_energy_kwh").sum().round(2).alias("energy_kwh"),
            pl.col("avg_power").mean().round(2).alias("avg_power"),
            pl.col("peak_power").max().alias("peak_power"),
        )
        .sort("station_id")
    )
    per_day = (
        gold_f.with_columns(pl.col("start_time").dt.date().alias("day"))
        .group_by("day")
        .agg(
            pl.len().alias("sessions"),
            pl.col("total_energy_kwh").sum().round(2).alias("energy_kwh"),
        )
        .sort("day")
    )
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Per station")
        st.dataframe(per_station, width="stretch")
    with col2:
        st.caption("Per day")
        st.dataframe(per_day, width="stretch")


main()
