"""Third page: steer the simulation live.

Writes to the shared ``sim_control`` row in Postgres, which the stream consumer
polls for its inter-frame delay and the dashboard reads for its clock mode:

- Streaming speed — how fast the stream consumer replays frames (delay seconds).
- Clock mode — how the live views derive "now": ``data`` (newest payload
  timestamp in the window, correct for a historical replay) or ``wall`` (real
  wall-clock time).

Changes take effect without restarting any container.
"""

from __future__ import annotations

import psycopg
import streamlit as st

# Absolute import: Streamlit runs page files as top-level scripts (no package).
from ev_ocpp_analysis import init_sim_control, read_sim_control, write_sim_control
from ev_ocpp_dashboard.config import PG_URI

st.set_page_config(page_title="Simulation", layout="centered")
st.title("Simulation controls")


def _load() -> dict:
    with psycopg.connect(PG_URI, connect_timeout=5) as conn:
        init_sim_control(conn)
        return read_sim_control(conn)


try:
    current = _load()
except psycopg.Error as exc:
    st.error(f"Could not reach Postgres: {exc}")
    st.stop()

st.caption(
    "These controls are shared via Postgres; the stream consumer and the "
    "live views pick them up within a few seconds."
)

# --- streaming speed --------------------------------------------------------
st.subheader("Streaming speed")
delay = st.slider(
    "Delay between frames (seconds)",
    min_value=0.0,
    max_value=5.0,
    value=float(current["stream_delay"]),
    step=0.05,
    help="0 = ingest as fast as possible; higher = slower replay.",
)

# --- clock mode -------------------------------------------------------------
st.subheader("Clock")
modes = {"data": "Data time (newest payload timestamp)", "wall": "Wall-clock (real time)"}
clock_mode = st.radio(
    "How the live views compute 'now'",
    options=list(modes),
    index=list(modes).index(current["clock_mode"]) if current["clock_mode"] in modes else 0,
    format_func=lambda m: modes[m],
    help="The feed replays 2025 data, so 'data time' is correct for a historical replay.",
)

if st.button("Apply", type="primary"):
    with psycopg.connect(PG_URI, connect_timeout=5) as conn:
        write_sim_control(conn, stream_delay=delay, clock_mode=clock_mode)
    st.success(f"Applied: delay={delay:.2f}s, clock={clock_mode}")

st.divider()
st.write("Current settings:", current)
