"""Fifth page: fleet analytics from the daily gold table.

Reads the ``gold_analytics_daily`` DuckLake table (one row per charger per day,
produced by the Dagster analytics job) and offers:
- rank chargers by total energy sold over a chosen day or month,
- a utilization overview (% of time each charger was in a session),
- total fault counts per charger.
"""

from __future__ import annotations

import polars as pl
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import ANALYTICS_TABLE, read_gold_table
from ev_ocpp_dashboard.config import LAKE_CATALOG, LAKE_DATA

st.set_page_config(page_title="Analytics", layout="wide")
st.title("Fleet analytics")


@st.cache_data(ttl=30)
def _daily() -> pl.DataFrame:
    return read_gold_table(LAKE_CATALOG, LAKE_DATA, ANALYTICS_TABLE)


daily = _daily()
if daily.is_empty():
    st.info("No analytics yet — run the Dagster analytics job.")
    st.stop()

daily = daily.with_columns(pl.col("day").cast(pl.Date))
min_day, max_day = daily["day"].min(), daily["day"].max()
st.caption(f"Data spans {min_day} → {max_day} across {daily['charger_id'].n_unique()} chargers.")

# --- energy ranking (per day or per month) ---------------------------------
st.subheader("Energy sold — charger ranking")
grain = st.radio("Period", ["Day", "Month"], horizontal=True)

if grain == "Day":
    days = sorted(daily["day"].unique().to_list(), reverse=True)
    pick = st.selectbox("Day", days)
    scoped = daily.filter(pl.col("day") == pick)
    label = str(pick)
else:
    months = (
        daily.with_columns(pl.col("day").dt.strftime("%Y-%m").alias("month"))["month"]
        .unique()
        .sort(descending=True)
        .to_list()
    )
    pick = st.selectbox("Month", months)
    scoped = daily.filter(pl.col("day").dt.strftime("%Y-%m") == pick)
    label = pick

ranking = (
    scoped.group_by("charger_id")
    .agg(
        pl.col("total_energy_kwh").sum().round(2).alias("energy_kwh"),
        pl.col("session_count").sum().alias("sessions"),
        pl.col("fault_count").sum().alias("faults"),
    )
    .sort("energy_kwh", descending=True)
)
if ranking.is_empty():
    st.info(f"No data for {label}.")
else:
    st.caption(f"Total energy sold per charger — {label}")
    st.bar_chart(ranking, x="charger_id", y="energy_kwh")
    st.dataframe(ranking, width="stretch")

# --- utilization overview ---------------------------------------------------
st.subheader("Utilization — share of time in a session")
util = (
    daily.group_by("charger_id")
    .agg(pl.col("utilization_pct").mean().round(2).alias("avg_utilization_pct"))
    .sort("avg_utilization_pct", descending=True)
)
st.caption("Average daily utilization per charger (% of the day in a session)")
st.bar_chart(util, x="charger_id", y="avg_utilization_pct")

# --- faults -----------------------------------------------------------------
st.subheader("Faults per charger")
faults = (
    daily.group_by("charger_id")
    .agg(pl.col("fault_count").sum().alias("total_faults"))
    .sort("total_faults", descending=True)
)
c1, c2 = st.columns([2, 1])
with c1:
    st.bar_chart(faults, x="charger_id", y="total_faults")
with c2:
    st.metric("Fleet total faults", int(faults["total_faults"].sum()))
    st.dataframe(faults, width="stretch")
