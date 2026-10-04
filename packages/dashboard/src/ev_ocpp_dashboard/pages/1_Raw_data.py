"""Second page: raw inspection of the landing table and the DuckLake tables.

Shows the first N rows of the Postgres ``raw_events`` table and previews the
immutable DuckLake cold archive table and the gold session facts, so you can see
exactly what is landing and being derived.
"""

from __future__ import annotations

import polars as pl
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import (
    SESSIONS_TABLE,
    read_archive_ducklake,
    read_gold_table,
)
from ev_ocpp_dashboard.config import LAKE_CATALOG, LAKE_DATA, PG_URI

st.title("Raw data inspector")

n = st.sidebar.slider("Rows to show", min_value=5, max_value=500, value=50, step=5)


@st.cache_data(ttl=5)
def _raw_rows(limit: int) -> pl.DataFrame:
    # payload cast to text so ConnectorX returns a readable string column.
    query = (
        "SELECT event_id, charger_id, msg_type, unique_id, action, "
        f"payload::text AS payload, ingest_ts FROM raw_events ORDER BY event_id DESC LIMIT {limit}"
    )
    return pl.read_database_uri(query, PG_URI, engine="connectorx")


@st.cache_data(ttl=5)
def _raw_total() -> int:
    df = pl.read_database_uri("SELECT count(*) AS n FROM raw_events", PG_URI, engine="connectorx")
    return int(df["n"][0]) if not df.is_empty() else 0


# --- Postgres raw_events ----------------------------------------------------
st.subheader("Postgres `raw_events` (latest first)")
try:
    total = _raw_total()
    st.caption(f"{total} rows total")
    st.dataframe(_raw_rows(n), width="stretch")
except Exception as exc:  # noqa: BLE001 - surface any conn/query error in the UI
    st.error(f"Could not read raw_events: {exc}")

# --- Cold DuckLake archive --------------------------------------------------
st.subheader("Cold DuckLake archive")
archive = read_archive_ducklake(LAKE_CATALOG, LAKE_DATA)
if archive.is_empty():
    st.info("No rows in the DuckLake archive table yet — run the Dagster dump job.")
else:
    st.caption(f"{archive.height} rows in the DuckLake archive table")
    st.dataframe(archive.head(n), width="stretch")

# --- Gold session facts -----------------------------------------------------
st.subheader("Gold session facts")
gold = read_gold_table(LAKE_CATALOG, LAKE_DATA, SESSIONS_TABLE)
if gold.is_empty():
    st.info("No gold sessions yet — run the Dagster session job.")
else:
    st.caption(f"{gold.height} sessions in the DuckLake gold table")
    st.dataframe(gold.head(n), width="stretch")
