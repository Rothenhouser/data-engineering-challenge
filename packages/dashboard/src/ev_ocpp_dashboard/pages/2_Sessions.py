"""Fourth page: per-charger session explorer with charging-curve plots.

Flow: pick a charger -> see its historical sessions (gold) plus the current live
session if one is open -> pick a session -> plot its charging curve (power and
SoC over time).

Historical sessions and their readings come from the gold DuckLake tables
(``gold_sessions`` / ``gold_session_readings``). The current session and its
readings are reconstructed live from the recent Postgres window with the same
shared fold, so a live session matches how it will later appear in gold.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl
import streamlit as st

# Absolute imports: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import (
    READINGS_TABLE,
    SESSIONS_TABLE,
    read_gold_table,
    read_recent_window_postgres,
    reconstruct_sessions_and_readings,
)
from ev_ocpp_dashboard.config import (
    LAKE_CATALOG,
    LAKE_DATA,
    PG_URI,
    WINDOW_MINUTES,
)

st.set_page_config(page_title="Sessions", layout="wide")
st.title("Charging sessions")


@st.cache_data(ttl=30)
def _gold() -> tuple[pl.DataFrame, pl.DataFrame]:
    sessions = read_gold_table(LAKE_CATALOG, LAKE_DATA, SESSIONS_TABLE)
    readings = read_gold_table(LAKE_CATALOG, LAKE_DATA, READINGS_TABLE)
    return sessions, readings


@st.cache_data(ttl=5)
def _live() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Live sessions + readings from the recent Postgres window.

    Uses the newest payload timestamp as 'now' (the feed replays historical
    time), matching the main overview page.
    """
    since = datetime.now(UTC) - timedelta(minutes=WINDOW_MINUTES)
    events = read_recent_window_postgres(PG_URI, since)
    if events.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    times: list[datetime] = []
    for p in events["payload"].to_list():
        if not isinstance(p, dict):
            continue
        ts = p.get("timestamp") or p.get("currentTime")
        if isinstance(ts, str) and ts:
            try:
                times.append(datetime.fromisoformat(ts))
            except ValueError:
                pass
    now = max(times) if times else datetime.now(UTC)
    return reconstruct_sessions_and_readings(events, now=now)


gold_sessions, gold_readings = _gold()
live_sessions, live_readings = _live()

all_chargers = sorted(
    set(gold_sessions["charger_id"].to_list() if not gold_sessions.is_empty() else [])
    | set(live_sessions["charger_id"].to_list() if not live_sessions.is_empty() else [])
)
if not all_chargers:
    st.info("No sessions yet. Start the stream and run the Dagster session job.")
    st.stop()

charger = st.selectbox("Charger", all_chargers)

# --- build the session list for this charger -------------------------------
hist = (
    gold_sessions.filter(pl.col("charger_id") == charger)
    if not gold_sessions.is_empty()
    else pl.DataFrame()
)
current = (
    live_sessions.filter((pl.col("charger_id") == charger) & (pl.col("status") == "active"))
    if not live_sessions.is_empty()
    else pl.DataFrame()
)

st.subheader("Sessions")
cols = ["session_id", "status", "start_time", "end_time", "duration", "total_energy_kwh"]
if not current.is_empty():
    st.caption("Current (live) session")
    st.dataframe(current.select([c for c in cols if c in current.columns]), width="stretch")
if not hist.is_empty():
    st.caption("Historical sessions")
    st.dataframe(hist.select(cols).sort("start_time", descending=True), width="stretch")
elif current.is_empty():
    st.info(f"No sessions for {charger}.")
    st.stop()

# --- pick a session and plot its charging curve ----------------------------
options: list[tuple[str, str]] = []  # (label, session_id)
if not current.is_empty():
    for sid in current["session_id"].to_list():
        options.append((f"CURRENT · {sid}", sid))
if not hist.is_empty():
    for row in hist.sort("start_time", descending=True).iter_rows(named=True):
        label = f"{row['start_time']} · {row['status']} · {row['session_id']}"
        options.append((label, row["session_id"]))

labels = [o[0] for o in options]
chosen = st.selectbox("Session to plot", labels) if labels else None
if not chosen:
    st.stop()
session_id = dict((lbl, sid) for lbl, sid in options)[chosen]

# readings: prefer live (fresher) if present, else gold
live_r = (
    live_readings.filter(pl.col("session_id") == session_id)
    if not live_readings.is_empty()
    else pl.DataFrame()
)
gold_r = (
    gold_readings.filter(pl.col("session_id") == session_id)
    if not gold_readings.is_empty()
    else pl.DataFrame()
)
readings = live_r if not live_r.is_empty() else gold_r

st.subheader("Charging curve")
if readings.is_empty():
    st.info("No MeterValues readings recorded for this session.")
    st.stop()

readings = readings.sort("timestamp")
st.caption(f"{readings.height} readings")

# Energy.Active.Import.Register is a lifetime odometer; subtract its first value
# in the session so the curve shows energy delivered this session (from 0).
reg = readings["energy_register_kwh"].drop_nulls()
if reg.len() > 0:
    baseline = reg[0]
    readings = readings.with_columns(
        (pl.col("energy_register_kwh") - baseline).alias("cumulative_energy_kwh")
    )

if readings["power_kw"].drop_nulls().len() > 0:
    st.markdown("**Power (kW)**")
    st.line_chart(readings, x="timestamp", y="power_kw")
if readings["soc_pct"].drop_nulls().len() > 0:
    st.markdown("**State of charge (%)**")
    st.line_chart(readings, x="timestamp", y="soc_pct")
if "cumulative_energy_kwh" in readings.columns:
    st.markdown("**Cumulative energy (kWh, this session)**")
    st.line_chart(readings, x="timestamp", y="cumulative_energy_kwh")

with st.expander("Raw readings"):
    st.dataframe(readings, width="stretch")
