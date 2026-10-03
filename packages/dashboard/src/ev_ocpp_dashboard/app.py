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
import psycopg
import streamlit as st
from ev_ocpp_analysis import (
    read_recent_window_postgres,
    read_sim_control,
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
                except (KeyError, TypeError, ValueError):
                    return None
    return None


@st.cache_data(ttl=5)
def _recent_window(window_minutes: int) -> pl.DataFrame:
    """Read the bounded recent Postgres window once; shared by the live views."""
    since = datetime.now(UTC) - timedelta(minutes=window_minutes)
    return read_recent_window_postgres(PG_URI, since)


@st.cache_data(ttl=5)
def _clock_mode() -> str:
    """Current clock mode from sim_control ('data' or 'wall')."""
    try:
        with psycopg.connect(PG_URI, connect_timeout=5) as conn:
            return read_sim_control(conn).get("clock_mode", "data")
    except psycopg.Error:
        return "data"


def _payload_time(payload: dict[str, Any]) -> datetime | None:
    ts = payload.get("timestamp") or payload.get("currentTime")
    if isinstance(ts, str) and ts:
        try:
            return datetime.fromisoformat(ts)
        except ValueError:
            return None
    return None


def _sim_now(events: pl.DataFrame) -> datetime:
    """The 'now' the live fold should use.

    The simulated feed replays historical payload timestamps (e.g. 2025) while
    rows are *ingested* at today's wall-clock time, so wall-clock is the wrong
    reference. In ``data`` mode (default) use the newest payload timestamp in the
    window — the same time base as the session start/stop boundaries — so running
    duration, energy and active/incomplete status are all correct. In ``wall``
    mode use real time.
    """
    if _clock_mode() == "wall" or events.is_empty():
        return datetime.now(UTC)
    times = [
        t
        for p in events["payload"].to_list()
        if (t := _payload_time(p if isinstance(p, dict) else {}))
    ]
    return max(times) if times else datetime.now(UTC)


def _latest_readings(events: pl.DataFrame) -> pl.DataFrame:
    """Latest Power.Active.Import, SoC and last-seen time per station."""
    rows = events.sort("ingest_ts").to_dicts()
    latest: dict[str, dict] = {}
    for r in rows:  # ascending time, so the last write per station wins
        sid = r["station_id"]
        cur = latest.setdefault(sid, {"station_id": sid, "power_kw": None, "soc_pct": None})
        cur["last_seen"] = r["ingest_ts"]
        if r.get("action") == "MeterValues":
            payload = r["payload"] if isinstance(r["payload"], dict) else {}
            p = _measurand(payload, "Power.Active.Import")
            s = _measurand(payload, "SoC")
            if p is not None:
                cur["power_kw"] = p
            if s is not None:
                cur["soc_pct"] = s
    return pl.DataFrame(list(latest.values())) if latest else pl.DataFrame()


@st.cache_data(ttl=5)
def _charger_overview(window_minutes: int) -> pl.DataFrame:
    """One row per charger seen recently: in-session status + live power/energy.

    Everything is derived from the bounded recent Postgres window: active
    sessions come from the shared ``reconstruct_sessions`` fold, and the latest
    power/SoC/last-seen come from the same events.
    """
    events = _recent_window(window_minutes)
    if events.is_empty():
        return pl.DataFrame()

    readings = _latest_readings(events)
    now = _sim_now(events)
    sessions = reconstruct_sessions(events, now=now)
    active = (
        sessions.filter(pl.col("status") == "active")
        .select(
            "station_id",
            pl.lit(True).alias("in_session"),
            pl.col("start_time").alias("session_start"),
            pl.col("total_energy_kwh").round(2).alias("energy_kwh_so_far"),
        )
        if not sessions.is_empty()
        else pl.DataFrame(
            schema={
                "station_id": pl.Utf8,
                "in_session": pl.Boolean,
                "session_start": pl.Datetime,
                "energy_kwh_so_far": pl.Float64,
            }
        )
    )
    overview = (
        readings.join(active, on="station_id", how="left")
        .with_columns(pl.col("in_session").fill_null(False))  # noqa: FBT003
        .with_columns(
            pl.when(pl.col("in_session"))
            .then(pl.col("power_kw"))
            .otherwise(None)
            .alias("current_power_kw"),
        )
    )
    # Running session minutes for in-session chargers.
    if "session_start" in overview.columns:
        overview = overview.with_columns(
            pl.when(pl.col("in_session"))
            .then((pl.lit(now) - pl.col("session_start")).dt.total_seconds() / 60.0)
            .otherwise(None)
            .round(1)
            .alias("session_minutes")
        )
    return overview.select(
        "station_id",
        "in_session",
        "current_power_kw",
        "energy_kwh_so_far",
        "session_minutes",
        "soc_pct",
        "last_seen",
    ).sort("station_id")


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
    overview = _charger_overview(WINDOW_MINUTES)

    # --- filters (apply across views) --------------------------------------
    stations = sorted(
        set(gold["station_id"].to_list() if not gold.is_empty() else [])
        | set(overview["station_id"].to_list() if not overview.is_empty() else [])
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
            st_autorefresh(interval=every * 1000, key="auto_refresh")

    gold_f = _apply_filters(gold, picked, start, end, "start_time")
    if picked and not overview.is_empty():
        overview = overview.filter(pl.col("station_id").is_in(picked))

    # --- per-charger live overview -----------------------------------------
    st.subheader(f"Charger overview (last {WINDOW_MINUTES} min)")
    if overview.is_empty():
        st.info("No recent events — is the stream consumer running against Postgres?")
    else:
        charging = int(overview["in_session"].sum())
        c1, c2 = st.columns(2)
        c1.metric("Chargers seen", overview.height)
        c2.metric("In session", charging)
        st.dataframe(
            overview.rename(
                {
                    "in_session": "in session",
                    "current_power_kw": "power (kW)",
                    "energy_kwh_so_far": "energy so far (kWh)",
                    "session_minutes": "session (min)",
                    "soc_pct": "SoC (%)",
                    "last_seen": "last seen",
                }
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
