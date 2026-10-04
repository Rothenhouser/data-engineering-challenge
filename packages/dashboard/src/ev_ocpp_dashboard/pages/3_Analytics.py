"""Fifth page: fleet analytics from the daily gold table.

Reads the ``gold_analytics_daily`` DuckLake table (one row per
``(charger_id, connector_id, day)``, produced by the Dagster analytics job).

A selector at the top scopes everything below to either a whole charger
(its connectors rolled up) or a single connector:
- rank chargers by total energy sold over a chosen day or month,
- a utilization overview (% of time in a session),
- total fault counts.
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


def _roll_up_to_charger(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse per-connector rows to one row per (charger_id, day).

    Additive metrics are summed so a charger total equals the sum of its
    connectors. Non-additive metrics are recomputed: peak_power = max,
    avg_power = mean. utilization_pct = mean of per-connector utilization —
    parallel connectors can each approach 100%, so summing could exceed 100%.
    """
    return (
        df.group_by("charger_id", "day")
        .agg(
            pl.col("site_id").first(),
            pl.lit(0).alias("connector_id"),  # 0 == "all connectors"
            pl.col("session_count").sum(),
            pl.col("total_energy_kwh").sum().round(3),
            pl.col("avg_power").mean().round(2),
            pl.col("peak_power").max(),
            pl.col("seconds_in_session").sum(),
            pl.col("utilization_pct").mean().round(2),
            pl.col("fault_count").sum(),
        )
        .select(daily.columns)
    )


daily = _daily()
if daily.is_empty():
    st.info("No analytics yet — run the Dagster analytics job.")
    st.stop()

daily = daily.with_columns(pl.col("day").cast(pl.Date))
min_day, max_day = daily["day"].min(), daily["day"].max()
st.caption(f"Data spans {min_day} → {max_day} across {daily['charger_id'].n_unique()} chargers.")

# --- scope selector ---------------------------------------------------------
mode = st.radio("View", ["Charger (total)", "Connector"], horizontal=True)
chargers = sorted(daily["charger_id"].unique().to_list())
charger = st.selectbox("Charger", chargers)

if mode == "Connector":
    connectors = sorted(
        daily.filter(pl.col("charger_id") == charger)["connector_id"].unique().to_list()
    )
    connector = st.selectbox("Connector", connectors)
    scope = daily.filter((pl.col("charger_id") == charger) & (pl.col("connector_id") == connector))
    st.caption(f"Scoped to charger {charger}, connector {connector}.")
else:
    scope = _roll_up_to_charger(daily.filter(pl.col("charger_id") == charger))
    st.caption(f"Scoped to charger {charger} (all connectors rolled up).")

# --- energy ranking (per day or per month) ---------------------------------
st.subheader("Energy sold — ranking")
grain = st.radio("Period", ["Day", "Month"], horizontal=True)

if grain == "Day":
    days = sorted(scope["day"].unique().to_list(), reverse=True)
    pick = st.selectbox("Day", days)
    scoped = scope.filter(pl.col("day") == pick)
    label = str(pick)
else:
    months = (
        scope.with_columns(pl.col("day").dt.strftime("%Y-%m").alias("month"))["month"]
        .unique()
        .sort(descending=True)
        .to_list()
    )
    pick = st.selectbox("Month", months)
    scoped = scope.filter(pl.col("day").dt.strftime("%Y-%m") == pick)
    label = pick

ranking = (
    scoped.group_by("charger_id", "connector_id")
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
    st.caption(f"Total energy sold — {label}")
    st.dataframe(ranking, width="stretch")
    st.metric("Energy sold (kWh)", float(ranking["energy_kwh"].sum()))

# --- utilization overview ---------------------------------------------------
st.subheader("Utilization — share of time in a session")
util = (
    scope.group_by("day")
    .agg(pl.col("utilization_pct").mean().round(2).alias("avg_utilization_pct"))
    .sort("day")
)
st.caption("Daily utilization (% of the day in a session)")
st.bar_chart(util, x="day", y="avg_utilization_pct")

# --- faults -----------------------------------------------------------------
st.subheader("Faults")
faults = scope.group_by("day").agg(pl.col("fault_count").sum().alias("total_faults")).sort("day")
c1, c2 = st.columns([2, 1])
with c1:
    st.bar_chart(faults, x="day", y="total_faults")
with c2:
    st.metric("Total faults", int(faults["total_faults"].sum()))
    st.dataframe(faults, width="stretch")
