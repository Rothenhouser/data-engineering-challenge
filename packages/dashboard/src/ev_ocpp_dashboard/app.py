"""Streamlit dashboard entrypoint: navigation + shared sidebar.

Defines the page navigation explicitly (via ``st.navigation``) so the live
status view is a proper page named "Latest" rather than an unnamed home script,
and renders the shared auto-refresh control once in the sidebar so it appears on
every page. Each page's content lives in its own script under ``pages/``.
"""

from __future__ import annotations

import streamlit as st

from ev_ocpp_dashboard.ui import auto_refresh_sidebar

st.set_page_config(page_title="EV OCPP Dashboard", layout="wide")

_PAGES = "pages"
nav = st.navigation(
    [
        st.Page(f"{_PAGES}/0_Latest.py", title="Latest", icon="⚡", default=True),
        st.Page(f"{_PAGES}/1_Raw_data.py", title="Raw data", icon="🗃️"),
        st.Page(f"{_PAGES}/2_Sessions.py", title="Sessions", icon="🔌"),
        st.Page(f"{_PAGES}/3_Analytics.py", title="Analytics", icon="📊"),
        st.Page(f"{_PAGES}/4_DuckLake.py", title="DuckLake", icon="🦆"),
    ]
)

# Rendered once here so the control shows on every page and drives the timer
# globally. Must run before the page body so the rerun is scheduled each cycle.
auto_refresh_sidebar()

nav.run()
