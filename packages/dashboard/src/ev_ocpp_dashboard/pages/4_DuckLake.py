"""Sixth page: DuckLake catalog inspector.

Surfaces the DuckLake catalog's metadata — the tables and their live row counts,
each table's column schema, and the snapshot history — so the embedded lakehouse
is browsable without opening the raw ``ducklake_*`` catalog tables by hand. The
cold archive and gold layers are stored as DuckLake tables (Parquet data + a
local DuckDB catalog file); this page reads that catalog, not the data itself.
"""

from __future__ import annotations

import polars as pl
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import catalog_overview
from ev_ocpp_dashboard.config import LAKE_CATALOG, LAKE_DATA

st.set_page_config(page_title="DuckLake", layout="wide")
st.title("DuckLake catalog")
st.caption(
    "Embedded lakehouse (no service): Parquet data under a local directory, "
    "metadata in a local DuckDB catalog file. Cold archive + gold live here."
)


@st.cache_data(ttl=15)
def _overview() -> dict[str, pl.DataFrame]:
    return catalog_overview(LAKE_CATALOG, LAKE_DATA)


try:
    overview = _overview()
except Exception as exc:  # noqa: BLE001 - surface attach/query errors in the UI
    st.error(f"Could not read the DuckLake catalog: {exc}")
    st.stop()

tables = overview["tables"]
columns = overview["columns"]
snapshots = overview["snapshots"]

if tables.is_empty():
    st.info(
        "The catalog has no tables yet — run the Dagster pipeline "
        "(dump → sessions → analytics) to populate the archive and gold tables."
    )
    st.stop()

# --- tables + row counts ----------------------------------------------------
st.subheader("Tables")
st.dataframe(tables, width="stretch")

# --- per-table schema -------------------------------------------------------
st.subheader("Schema")
table_names = tables["table_name"].to_list()
picked = st.selectbox("Table", table_names)
schema = columns.filter(pl.col("table_name") == picked).select(
    "column_name", "data_type", "ordinal_position"
)
st.dataframe(schema, width="stretch")

# --- snapshot history -------------------------------------------------------
st.subheader("Snapshot history")
if snapshots.is_empty():
    st.info("No snapshots recorded yet.")
else:
    st.caption(
        f"{snapshots.height} snapshot(s), newest first — each archive append "
        "and each gold rebuild is one snapshot."
    )
    st.dataframe(snapshots, width="stretch")
