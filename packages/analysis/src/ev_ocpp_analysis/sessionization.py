"""Shared, source-agnostic Polars sessionization fold.

``reconstruct_sessions`` is the single fold both the live path (over Postgres)
and the historical path (over the Parquet archive) call, so a session
reconstructed live matches the same session reconstructed from the archive
(Requirement 7). It is an ordered, stateful fold rather than a GROUP BY because
the session identity key only exists after the opening StartTransaction.

Correctness properties upheld here:
- Property 2 (duplicate-tolerant): content-identical frames are collapsed before
  folding, so replays never double-count.
- Property 6 (energy): ``total_energy_kwh`` = mean(Power.Active.Import) x duration.
- Property 7 (status): every session is exactly one of completed/active/incomplete.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from .measurands import Measurand, read_measurand

# Columns a raw-event frame must carry for the fold (the RawEvent shape).
RAW_COLUMNS = ("station_id", "msg_type", "unique_id", "action", "payload", "ingest_ts")
SESSION_COLUMNS = (
    "session_id",
    "station_id",
    "connector_id",
    "status",
    "start_time",
    "end_time",
    "duration",
    "total_energy_kwh",
    "avg_power",
    "peak_power",
    "event_count",
    "stop_reason",
)
# Per-session time-series readings (the charging curve), one row per MeterValues.
READING_COLUMNS = (
    "session_id",
    "station_id",
    "timestamp",
    "power_kw",
    "soc_pct",
    "energy_register_kwh",
)
READING_SCHEMA = {
    "session_id": pl.Utf8,
    "station_id": pl.Utf8,
    "timestamp": pl.Datetime,
    "power_kw": pl.Float64,
    "soc_pct": pl.Float64,
    "energy_register_kwh": pl.Float64,
}
# Default bounded recent window: open sessions newer than this are "active".
DEFAULT_ACTIVE_WINDOW_SECONDS = 3600.0


def _as_dict(payload: Any) -> dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _coerce_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        try:
            # Python 3.11+ fromisoformat accepts a trailing "Z".
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _payload_time(payload: dict[str, Any]) -> datetime | None:
    """The event's own timestamp from the payload, if any (used for boundaries)."""
    return _coerce_ts(payload.get("timestamp") or payload.get("currentTime"))


def _metervalue_time(payload: dict[str, Any]) -> datetime | None:
    """The sample timestamp of a MeterValues frame (nested under meterValue[])."""
    for mv in payload.get("meterValue") or []:
        t = _coerce_ts(mv.get("timestamp"))
        if t is not None:
            return t
    return None


def _power_sample(payload: dict[str, Any]) -> float | None:
    """Pull the Power.Active.Import sample from a MeterValues payload."""
    return read_measurand(payload, Measurand.POWER_ACTIVE_IMPORT)


def _dedup(events: pl.DataFrame) -> list[dict[str, Any]]:
    """Collapse content-identical frames (Property 2) and order by arrival.

    Rows are folded in ingestion/arrival order (``ingest_ts``, falling back to
    original row order) so request/response pairs stay adjacent — the payload
    timestamp only fixes session boundary times, not fold order.
    """
    cols = [c for c in RAW_COLUMNS if c in events.columns]
    rows = events.select(cols).to_dicts()
    seen: set[tuple] = set()
    out: list[dict[str, Any]] = []
    for i, r in enumerate(rows):
        payload = _as_dict(r.get("payload"))
        key = (
            r.get("station_id"),
            r.get("msg_type"),
            r.get("unique_id"),
            r.get("action"),
            str(payload),
        )
        if key in seen:
            continue
        seen.add(key)
        r["_payload"] = payload
        r["_order"] = (_coerce_ts(r.get("ingest_ts")) or _EPOCH, i)
        # Boundary time: payload timestamp if present, else arrival time.
        r["_time"] = _payload_time(payload) or _coerce_ts(r.get("ingest_ts")) or _EPOCH
        out.append(r)
    out.sort(key=lambda r: r["_order"])
    return out


def reconstruct_sessions(
    events: pl.DataFrame,
    now: datetime | None = None,
    active_window_seconds: float = DEFAULT_ACTIVE_WINDOW_SECONDS,
) -> pl.DataFrame:
    """Reconstruct charging-session facts from a raw-event frame.

    Thin wrapper over :func:`reconstruct_sessions_and_readings` returning only the
    session facts (unchanged contract for existing callers).
    """
    return reconstruct_sessions_and_readings(events, now, active_window_seconds)[0]


def reconstruct_sessions_and_readings(
    events: pl.DataFrame,
    now: datetime | None = None,
    active_window_seconds: float = DEFAULT_ACTIVE_WINDOW_SECONDS,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reconstruct sessions *and* their per-session time-series readings.

    Returns ``(sessions, readings)``. ``readings`` has one row per MeterValues
    sample for an open session (the charging curve): ``session_id``,
    ``station_id``, ``timestamp``, ``power_kw``, ``soc_pct``,
    ``energy_register_kwh``. Each reading links to its session by ``session_id``.

    Opens a session on StartTransaction, accumulates Power.Active.Import samples
    from MeterValues while open, and closes on StopTransaction. Sessions are keyed
    by ``station_id + connector_id + start_time``. Open sessions get ``active`` or
    ``incomplete`` status depending on whether they fall in the recent window.
    """
    now = now or datetime.now(UTC)
    rows = _dedup(events)

    # One open session per (station, connector); closed sessions are emitted.
    open_sessions: dict[tuple[str, Any], dict[str, Any]] = {}
    done: list[dict[str, Any]] = []
    # OCPP correlation: a StartTransaction Call carries connectorId but no
    # transactionId; its CallResult (same unique_id) carries the transactionId;
    # the StopTransaction carries transactionId but no connectorId. We thread the
    # two maps so a Stop can find the connector its session is keyed by.
    uid_to_connector: dict[tuple[str, str], Any] = {}  # (station, unique_id) -> connector
    txn_to_connector: dict[tuple[str, Any], Any] = {}  # (station, transactionId) -> connector

    readings: list[dict[str, Any]] = []  # per-session charging-curve samples

    def _session_id(station: Any, connector: Any, start: datetime) -> str:
        return f"{station}|{connector}|{start.isoformat()}"

    def finalize(s: dict[str, Any], end: datetime | None, reason: str | None) -> dict[str, Any]:
        start = s["start_time"]
        close = end or now
        duration = max((close - start).total_seconds(), 0.0)
        powers = s["powers"]
        avg_power = sum(powers) / len(powers) if powers else 0.0
        peak_power = max(powers) if powers else 0.0
        # Energy (kWh) = mean power (kW) x duration (hours).
        energy = avg_power * (duration / 3600.0)
        if end is not None:
            status = "completed"
        elif (now - start).total_seconds() <= active_window_seconds:
            status = "active"
        else:
            status = "incomplete"
        return {
            "session_id": s["session_id"],
            "station_id": s["station_id"],
            "connector_id": s["connector_id"],
            "status": status,
            "start_time": start,
            "end_time": end,
            "duration": duration,
            "total_energy_kwh": energy,
            "avg_power": avg_power,
            "peak_power": peak_power,
            "event_count": s["event_count"],
            "stop_reason": reason,
        }

    for r in rows:  # type: ignore[assignment]
        action = r.get("action")
        payload = r["_payload"]
        station = r.get("station_id")
        connector = payload.get("connectorId")

        if action == "StartTransaction":
            key = (station, connector)
            # A new Start closes any dangling open session on the same connector.
            if key in open_sessions:
                done.append(finalize(open_sessions.pop(key), None, None))
            open_sessions[key] = {
                "session_id": _session_id(station, connector, r["_time"]),
                "station_id": station,
                "connector_id": connector,
                "start_time": r["_time"],
                "powers": [],
                "event_count": 1,
            }
            uid_to_connector[(station, r.get("unique_id"))] = connector
        elif action is None and payload.get("transactionId") is not None:
            # StartTransaction CallResult: links transactionId -> connector.
            conn = uid_to_connector.get((station, r.get("unique_id")))
            if conn is not None:
                txn_to_connector[(station, payload.get("transactionId"))] = conn
        elif action == "StopTransaction":
            conn = txn_to_connector.get((station, payload.get("transactionId")))
            key = (station, conn)
            if key in open_sessions:
                s = open_sessions.pop(key)
                s["event_count"] += 1
                done.append(finalize(s, r["_time"], payload.get("reason")))
        elif action == "MeterValues":
            key = (station, connector)
            if key in open_sessions:
                s = open_sessions[key]
                s["event_count"] += 1
                p = _power_sample(payload)
                if p is not None:
                    s["powers"].append(p)
                # Record the charging-curve sample linked to this session,
                # timestamped by the MeterValues sample time where present.
                readings.append(
                    {
                        "session_id": s["session_id"],
                        "station_id": station,
                        "timestamp": _metervalue_time(payload) or r["_time"],
                        "power_kw": p,
                        "soc_pct": read_measurand(payload, Measurand.SOC),
                        "energy_register_kwh": read_measurand(payload, Measurand.ENERGY_REGISTER),
                    }
                )

    # Any still-open sessions are active/incomplete.
    for s in open_sessions.values():
        done.append(finalize(s, None, None))

    sessions_schema = {
        "session_id": pl.Utf8,
        "station_id": pl.Utf8,
        "connector_id": pl.Int64,
        "status": pl.Utf8,
        "start_time": pl.Datetime,
        "end_time": pl.Datetime,
        "duration": pl.Float64,
        "total_energy_kwh": pl.Float64,
        "avg_power": pl.Float64,
        "peak_power": pl.Float64,
        "event_count": pl.Int64,
        "stop_reason": pl.Utf8,
    }
    sessions = (
        pl.DataFrame(done).select(SESSION_COLUMNS)
        if done
        else pl.DataFrame(schema=sessions_schema)
    )
    readings_df = (
        pl.DataFrame(readings, schema_overrides=READING_SCHEMA).select(READING_COLUMNS)
        if readings
        else pl.DataFrame(schema=READING_SCHEMA)
    )
    return sessions, readings_df
