"""Second page: raw inspection of the landing table and the Parquet files.

Shows the first N rows of the Postgres ``raw_events`` table and previews the
immutable Parquet archive files and the gold session facts, so you can see
exactly what is landing and being derived.
"""

from __future__ import annotations

import glob
import os

import polars as pl
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_dashboard.config import ARCHIVE_DIR, GOLD_PATH, PG_URI

st.set_page_config(page_title="Raw data", layout="wide")
st.title("Raw data inspector")

n = st.sidebar.slider("Rows to show", min_value=5, max_value=500, value=50, step=5)


@st.cache_data(ttl=5)
def _raw_rows(limit: int) -> pl.DataFrame:
    # payload cast to text so ConnectorX returns a readable string column.
    query = (
        "SELECT event_id, station_id, msg_type, unique_id, action, "
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

# --- Parquet archive --------------------------------------------------------
st.subheader("Cold Parquet archive")
files = sorted(glob.glob(os.path.join(ARCHIVE_DIR, "*.parquet")))
if not files:
    st.info(f"No archive files in {ARCHIVE_DIR} yet — run the Dagster dump job.")
else:
    st.caption(f"{len(files)} archive file(s) in {ARCHIVE_DIR}")
    picked = st.selectbox("Archive file", files, format_func=os.path.basename)
    df = pl.read_parquet(picked)
    st.caption(f"{df.height} rows x {df.width} cols")
    st.dataframe(df.head(n), width="stretch")

# --- Gold session facts -----------------------------------------------------
st.subheader("Gold session facts")
if os.path.exists(GOLD_PATH):
    gold = pl.read_parquet(GOLD_PATH)
    st.caption(f"{gold.height} sessions in {GOLD_PATH}")
    st.dataframe(gold.head(n), width="stretch")
else:
    st.info(f"No gold file at {GOLD_PATH} yet — run the Dagster session job.")
