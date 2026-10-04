"""Shared dashboard UI helpers."""

from __future__ import annotations

import streamlit as st
from streamlit_autorefresh import st_autorefresh


def auto_refresh_sidebar() -> None:
    """Render the auto-refresh toggle + interval slider in the sidebar.

    Rendered once from the navigation entrypoint so it shows on every page and
    re-runs the whole app on a timer. The cached readers (ttl ~5-30s) keep the
    reruns cheap, so live views stay current without a manual reload.
    """
    with st.sidebar:
        st.header("Auto-refresh")
        auto = st.toggle("Enabled", value=True, key="auto_refresh_enabled")
        every = st.slider("Interval (s)", 2, 60, 5, disabled=not auto, key="auto_refresh_interval")
    if auto:
        st_autorefresh(interval=every * 1000, key="auto_refresh")
