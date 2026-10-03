"""Property-based tests for daily analytics + the site_id dimension."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
from ev_ocpp_analysis.analytics import compute_daily_stats
from hypothesis import given, settings
from hypothesis import strategies as st

BASE = datetime(2025, 1, 1, tzinfo=UTC)


def _sessions(station_days: list[tuple[str, int, float]]) -> pl.DataFrame:
    """Build a minimal sessions frame: (station, day_offset, energy)."""
    rows = [
        {
            "session_id": f"{s}|1|{d}",
            "station_id": s,
            "site_id": None,
            "connector_id": 1,
            "status": "completed",
            "start_time": datetime(2025, 1, 1 + d, 12, tzinfo=UTC),
            "end_time": datetime(2025, 1, 1 + d, 13, tzinfo=UTC),
            "duration": 3600.0,
            "total_energy_kwh": e,
            "avg_power": e,
            "peak_power": e,
            "event_count": 3,
            "stop_reason": "Local",
        }
        for s, d, e in station_days
    ]
    return pl.DataFrame(rows)


def _faults(station_fault_days: list[tuple[str, int]]) -> pl.DataFrame:
    rows = [
        {
            "station_id": s,
            "msg_type": 2,
            "unique_id": f"f{i}",
            "action": "StatusNotification",
            "payload": {
                "errorCode": "InternalError",
                "status": "Faulted",
                "timestamp": datetime(2025, 1, 1 + d, 9, tzinfo=UTC).isoformat(),
            },
            "ingest_ts": BASE,
        }
        for i, (s, d) in enumerate(station_fault_days)
    ]
    return pl.DataFrame(rows, schema_overrides={"payload": pl.Object})


@settings(max_examples=50, deadline=None)
@given(
    sess=st.lists(
        st.tuples(
            st.sampled_from(["c1", "c2"]),
            st.integers(0, 3),
            st.floats(0.0, 100.0),
        ),
        min_size=1,
        max_size=8,
    ),
    flt=st.lists(
        st.tuples(st.sampled_from(["c1", "c2", "c3"]), st.integers(0, 3)),
        max_size=8,
    ),
)
def test_fault_count_reconciles(sess, flt):
    """Total fault_count across the daily table equals the raw fault frames."""
    sessions = _sessions(sess)
    faults = _faults(flt)
    daily = compute_daily_stats(sessions, faults)
    assert int(daily["fault_count"].sum()) == len(flt)


@settings(max_examples=50, deadline=None)
@given(
    sess=st.lists(
        st.tuples(st.sampled_from(["c1", "c2"]), st.integers(0, 3), st.floats(0.0, 100.0)),
        min_size=1,
        max_size=8,
    )
)
def test_utilization_within_bounds_and_energy_conserved(sess):
    """Utilization is a valid percentage; daily energy sums to the input total."""
    sessions = _sessions(sess)
    daily = compute_daily_stats(sessions, pl.DataFrame())
    util = daily["utilization_pct"].drop_nulls().to_list()
    assert all(0.0 <= u <= 100.0 for u in util)
    # Daily energy conserves the input total up to the table's 3-decimal rounding
    # (one rounded row per station-day).
    diff = abs(daily["total_energy_kwh"].sum() - sessions["total_energy_kwh"].sum())
    assert diff <= 0.0005 * daily.height + 1e-9


def test_site_id_passthrough():
    """site_id from the mapping flows into the daily rows."""
    sessions = _sessions([("c1", 0, 10.0)]).with_columns(pl.lit("site-a").alias("site_id"))
    daily = compute_daily_stats(sessions, pl.DataFrame())
    assert daily["site_id"].to_list() == ["site-a"]
