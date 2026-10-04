"""Fifth page: fleet analytics from the daily gold table.

Reads the ``gold_analytics_daily`` DuckLake table (one row per
``(charger_id, connector_id, day)``, produced by the Dagster analytics job).

Layout, top to bottom:
- A fleet-wide **ranking** table over a chosen day or month: every charger (or
  connector, via the toggle) with its total energy, utilization and faults.
- A **selection** of one charger (or connector) with its day-by-day history
  (energy, utilization, faults) below.
"""

from __future__ import annotations

import polars as pl
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import ANALYTICS_TABLE, read_gold_table
from ev_ocpp_dashboard.config import LAKE_CATALOG, LAKE_DATA

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
        .select(df.columns)
    )


daily = _daily()
if daily.is_empty():
    st.info("No analytics yet — wait for first Dagster archive job (or trigger manually).")
    st.stop()

daily = daily.with_columns(pl.col("day").cast(pl.Date))
min_day, max_day = daily["day"].min(), daily["day"].max()
st.caption(f"Data spans {min_day} → {max_day} across {daily['charger_id'].n_unique()} chargers.")

# --- granularity: whole charger (connectors rolled up) or per connector -----
by_connector = st.toggle("Break down by connector", value=False)
view = daily if by_connector else _roll_up_to_charger(daily)
unit = "connector" if by_connector else "charger"

# =============================================================================
# Ranking — fleet-wide, over a chosen day or month
# =============================================================================
st.subheader("Ranking")
grain = st.radio("Period", ["Day", "Month"], horizontal=True)

if grain == "Day":
    days = sorted(view["day"].unique().to_list(), reverse=True)
    pick = st.selectbox("Day", days)
    period = view.filter(pl.col("day") == pick)
    label = str(pick)
else:
    months = (
        view.with_columns(pl.col("day").dt.strftime("%Y-%m").alias("month"))["month"]
        .unique()
        .sort(descending=True)
        .to_list()
    )
    pick = st.selectbox("Month", months)
    period = view.filter(pl.col("day").dt.strftime("%Y-%m") == pick)
    label = pick

key_cols = ["charger_id", "connector_id"] if by_connector else ["charger_id"]
ranking = (
    period.group_by(key_cols)
    .agg(
        pl.col("total_energy_kwh").sum().round(2).alias("energy_kwh"),
        pl.col("utilization_pct").mean().round(2).alias("avg_utilization_pct"),
        pl.col("session_count").sum().alias("sessions"),
        pl.col("fault_count").sum().alias("faults"),
    )
    .sort("energy_kwh", descending=True)
)
if ranking.is_empty():
    st.info(f"No data for {label}.")
else:
    st.caption(f"All {unit}s — {label}, ranked by energy sold")
    st.dataframe(ranking, width="stretch")
    c1, c2, c3 = st.columns(3)
    c1.metric("Energy sold (kWh)", float(ranking["energy_kwh"].sum()))
    c2.metric("Sessions", int(ranking["sessions"].sum()))
    c3.metric("Faults", int(ranking["faults"].sum()))

st.divider()

# =============================================================================
# Selection — one charger / connector, history over days
# =============================================================================
st.subheader("History")
chargers = sorted(view["charger_id"].unique().to_list())
charger = st.selectbox("Charger", chargers)

if by_connector:
    connectors = sorted(
        view.filter(pl.col("charger_id") == charger)["connector_id"].unique().to_list()
    )
    connector = st.selectbox("Connector", connectors)
    scope = view.filter((pl.col("charger_id") == charger) & (pl.col("connector_id") == connector))
    st.caption(f"Charger {charger}, connector {connector} — day-by-day history.")
else:
    scope = view.filter(pl.col("charger_id") == charger)
    st.caption(f"Charger {charger} (all connectors) — day-by-day history.")

history = (
    scope.group_by("day")
    .agg(
        pl.col("total_energy_kwh").sum().round(2).alias("energy_kwh"),
        pl.col("utilization_pct").mean().round(2).alias("avg_utilization_pct"),
        pl.col("fault_count").sum().alias("faults"),
    )
    .sort("day")
)

col1, col2 = st.columns(2)
with col1:
    st.caption("Energy sold per day (kWh)")
    st.bar_chart(history, x="day", y="energy_kwh")
with col2:
    st.caption("Utilization per day (% of day in a session)")
    st.bar_chart(history, x="day", y="avg_utilization_pct")

st.caption("Faults per day")
fcol1, fcol2 = st.columns([2, 1])
with fcol1:
    st.bar_chart(history, x="day", y="faults")
with fcol2:
    st.metric("Total faults", int(history["faults"].sum()))
st.dataframe(history, width="stretch")
