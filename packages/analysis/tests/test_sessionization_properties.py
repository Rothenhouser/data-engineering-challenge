"""Property-based tests for the shared sessionization fold.

Maps to the design's Correctness Properties:
- Property 2 — duplicate-tolerant derived layer (dedup-at-read)
- Property 6 — energy derivation (register delta; power-integral fallback)
- Property 7 — status is total and exclusive (completed/active/incomplete)
- Property 8 — gold is a pure, rebuildable function of the input (determinism)
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import polars as pl
from ev_ocpp_analysis.sessionization import (
    reconstruct_sessions,
    reconstruct_sessions_and_readings,
)
from hypothesis import given, settings
from hypothesis import strategies as st

BASE = datetime(2025, 1, 1, tzinfo=UTC)


def _meter_payload(connector, ts, power, soc, register):
    return {
        "connectorId": connector,
        "transactionId": connector,
        "meterValue": [
            {
                "timestamp": ts.isoformat(),
                "sampledValue": [
                    {"measurand": "Power.Active.Import", "value": str(power), "unit": "kW"},
                    {"measurand": "SoC", "value": str(soc), "unit": "Percent"},
                    {
                        "measurand": "Energy.Active.Import.Register",
                        "value": str(register),
                        "unit": "kWh",
                    },
                ],
            }
        ],
    }


def _build_events(sessions_spec: list[dict], ingest_start: datetime) -> pl.DataFrame:
    """Turn a list of session specs into an ordered raw-event frame.

    Each spec: {charger, connector, start_offset_s, powers: [kw...], step_s,
    stop: bool}. Rows get a monotonically increasing ingest_ts.
    """
    rows: list[dict] = []
    seq = 0
    uid = 0

    def add(charger, msg_type, action, payload):
        nonlocal seq, uid
        rows.append(
            {
                "charger_id": charger,
                "msg_type": msg_type,
                "unique_id": f"u{uid}",
                "action": action,
                "payload": payload,
                "ingest_ts": ingest_start + timedelta(seconds=seq),
            }
        )
        seq += 1

    for spec in sessions_spec:
        charger, connector = spec["charger"], spec["connector"]
        t0 = BASE + timedelta(seconds=spec["start_offset_s"])
        uid += 1
        add(charger, 2, "StartTransaction", {"connectorId": connector, "timestamp": t0.isoformat()})
        # Start CallResult carries transactionId (links txn -> connector).
        add(charger, 3, None, {"transactionId": connector})
        register = spec["reg0"]
        t = t0
        for i, power in enumerate(spec["powers"]):
            t = t0 + timedelta(seconds=(i + 1) * spec["step_s"])
            register += power * (spec["step_s"] / 3600.0)
            uid += 1
            payload = _meter_payload(connector, t, power, 50 + i, round(register, 4))
            add(charger, 2, "MeterValues", payload)
            add(charger, 3, None, {})
        if spec["stop"]:
            t = t + timedelta(seconds=spec["step_s"])
            uid += 1
            add(
                charger,
                2,
                "StopTransaction",
                {"transactionId": connector, "timestamp": t.isoformat(), "reason": "Local"},
            )
            add(charger, 3, None, {})
    return pl.DataFrame(rows, schema_overrides={"payload": pl.Object})


# Strategy: a handful of sessions on a few chargers/connectors.
_session = st.fixed_dictionaries(
    {
        "charger": st.sampled_from(["c1", "c2", "c3"]),
        "connector": st.sampled_from([1, 2]),
        "start_offset_s": st.integers(min_value=0, max_value=5000),
        "powers": st.lists(st.floats(min_value=0.0, max_value=150.0), min_size=0, max_size=6),
        "step_s": st.sampled_from([30, 60, 300]),
        "reg0": st.floats(min_value=0.0, max_value=50000.0),
        "stop": st.booleans(),
    }
)
_specs = st.lists(_session, min_size=1, max_size=5)


def _dedup_distinct(spec_charger_connector_starts: list[dict]) -> list[dict]:
    """Keep specs with a unique (charger, connector, start) so sessions are distinct."""
    seen = set()
    out = []
    for s in spec_charger_connector_starts:
        k = (s["charger"], s["connector"], s["start_offset_s"])
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


@settings(max_examples=60, deadline=None)
@given(specs=_specs)
def test_property2_duplicate_tolerant(specs):
    """Property 2: content-identical duplicates do not change the result."""
    specs = _dedup_distinct(specs)
    events = _build_events(specs, BASE)
    now = BASE + timedelta(days=400)  # far future -> open sessions are 'incomplete'
    once = reconstruct_sessions(events, now=now)
    # Interleave an exact duplicate of every row.
    doubled = pl.concat([events, events])
    twice = reconstruct_sessions(doubled, now=now)
    assert once.sort("session_id").equals(twice.sort("session_id"))


@settings(max_examples=60, deadline=None)
@given(specs=_specs)
def test_property6_energy_register_delta(specs):
    """Property 6: energy == last-first register for sessions with >=2 readings."""
    specs = _dedup_distinct(specs)
    events = _build_events(specs, BASE)
    now = BASE + timedelta(days=400)
    sessions, readings = reconstruct_sessions_and_readings(events, now=now)
    for row in sessions.iter_rows(named=True):
        reg = (
            readings.filter(pl.col("session_id") == row["session_id"])
            .sort("timestamp")["energy_register_kwh"]
            .drop_nulls()
        )
        if reg.len() >= 2:
            expected = max(reg[-1] - reg[0], 0.0)
            assert row["total_energy_kwh"] is not None
            assert abs(row["total_energy_kwh"] - expected) < 1e-6


@settings(max_examples=60, deadline=None)
@given(specs=_specs)
def test_property7_status_total_and_exclusive(specs):
    """Property 7: every status is exactly one of completed/active/incomplete."""
    specs = _dedup_distinct(specs)
    events = _build_events(specs, BASE)
    now = BASE + timedelta(seconds=10000)
    sessions = reconstruct_sessions(events, now=now)
    statuses = set(sessions["status"].to_list())
    assert statuses <= {"completed", "active", "incomplete"}
    # A session with a Stop must be completed; completed implies a non-null end.
    for row in sessions.iter_rows(named=True):
        if row["status"] == "completed":
            assert row["end_time"] is not None
        else:
            assert row["end_time"] is None


@settings(max_examples=40, deadline=None)
@given(specs=_specs, seed=st.integers(min_value=0, max_value=10_000))
def test_property8_pure_function(specs, seed):
    """Property 8: shuffling arrival order of distinct frames is deterministic.

    The fold orders by ingest_ts, so re-running over the same events (and even a
    re-sorted copy) yields field-for-field equal sessions.
    """
    specs = _dedup_distinct(specs)
    events = _build_events(specs, BASE)
    now = BASE + timedelta(days=400)
    a = reconstruct_sessions(events, now=now)
    rng = random.Random(seed)
    order = list(range(events.height))
    rng.shuffle(order)
    shuffled = events[order]
    b = reconstruct_sessions(shuffled, now=now)
    assert a.sort("session_id").equals(b.sort("session_id"))
