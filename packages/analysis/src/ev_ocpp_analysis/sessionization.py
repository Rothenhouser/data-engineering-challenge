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
from .sites import site_for

# Columns a raw-event frame must carry for the fold (the RawEvent shape).
RAW_COLUMNS = ("charger_id", "msg_type", "unique_id", "action", "payload", "ingest_ts")
SESSION_COLUMNS = (
    "session_id",
    "charger_id",
    "site_id",
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
    "charger_id",
    "connector_id",
    "timestamp",
    "power_kw",
    "soc_pct",
    "energy_register_kwh",
)
READING_SCHEMA = {
    "session_id": pl.Utf8,
    "charger_id": pl.Utf8,
    "connector_id": pl.Int64,
    "timestamp": pl.Datetime(time_zone="UTC"),
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
            r.get("charger_id"),
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
    sites: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Reconstruct charging-session facts from a raw-event frame.

    Thin wrapper over :func:`reconstruct_sessions_and_readings` returning only the
    session facts (unchanged contract for existing callers).
    """
    return reconstruct_sessions_and_readings(events, now, active_window_seconds, sites)[0]


def reconstruct_sessions_and_readings(
    events: pl.DataFrame,
    now: datetime | None = None,
    active_window_seconds: float = DEFAULT_ACTIVE_WINDOW_SECONDS,
    sites: dict[str, str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reconstruct sessions *and* their per-session time-series readings.

    Returns ``(sessions, readings)``. ``readings`` has one row per MeterValues
    sample for an open session (the charging curve): ``session_id``,
    ``charger_id``, ``connector_id``, ``timestamp``, ``power_kw``, ``soc_pct``,
    ``energy_register_kwh``. Each reading links to its session by ``session_id``.

    Opens a session on StartTransaction, accumulates Power.Active.Import samples
    from MeterValues while open, and closes on StopTransaction. Sessions are keyed
    by ``charger_id + connector_id + start_time``. Open sessions get ``active`` or
    ``incomplete`` status depending on whether they fall in the recent window.
    """
    now = now or datetime.now(UTC)
    rows = _dedup(events)

    # One open session per (charger, connector); closed sessions are emitted.
    open_sessions: dict[tuple[str, Any], dict[str, Any]] = {}
    done: list[dict[str, Any]] = []
    # OCPP correlation: a StartTransaction Call carries connectorId but no
    # transactionId; its CallResult (same unique_id) carries the transactionId;
    # the StopTransaction carries transactionId but no connectorId. We thread the
    # two maps so a Stop can find the connector its session is keyed by.
    uid_to_connector: dict[tuple[str, str], Any] = {}  # (charger, unique_id) -> connector
    txn_to_connector: dict[tuple[str, Any], Any] = {}  # (charger, transactionId) -> connector

    readings: list[dict[str, Any]] = []  # per-session charging-curve samples

    def _session_id(charger: Any, connector: Any, start: datetime) -> str:
        return f"{charger}|{connector}|{start.isoformat()}"

    def finalize(s: dict[str, Any], end: datetime | None, reason: str | None) -> dict[str, Any]:
        start = s["start_time"]
        powers = s["powers"]
        avg_power = sum(powers) / len(powers) if powers else 0.0
        peak_power = max(powers) if powers else 0.0
        # Duration only exists once the session has closed (a StopTransaction);
        # for open/incomplete sessions leave it None rather than guessing to-now.
        duration = (end - start).total_seconds() if end is not None else None
        # Energy charged = meter register delta (the charger's metered truth),
        # which works for both completed and still-open sessions. Fall back to the
        # power-integral only when no register readings were seen.
        if s["reg_first"] is not None and s["reg_last"] is not None:
            energy = max(s["reg_last"] - s["reg_first"], 0.0)
        elif duration is not None:
            energy = avg_power * (duration / 3600.0)
        else:
            energy = None
        if end is not None:
            status = "completed"
        elif (now - start).total_seconds() <= active_window_seconds:
            status = "active"
        else:
            status = "incomplete"
        return {
            "session_id": s["session_id"],
            "charger_id": s["charger_id"],
            "site_id": site_for(s["charger_id"], sites),
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
        charger = r.get("charger_id")
        connector = payload.get("connectorId")

        if action == "StartTransaction":
            key = (charger, connector)
            # A new Start closes any dangling open session on the same connector.
            if key in open_sessions:
                done.append(finalize(open_sessions.pop(key), None, None))
            open_sessions[key] = {
                "session_id": _session_id(charger, connector, r["_time"]),
                "charger_id": charger,
                "connector_id": connector,
                "start_time": r["_time"],
                "powers": [],
                "event_count": 1,
                "reg_first": None,  # first/last Energy.Active.Import.Register seen
                "reg_last": None,
            }
            uid_to_connector[(charger, r.get("unique_id"))] = connector
        elif action is None and payload.get("transactionId") is not None:
            # StartTransaction CallResult: links transactionId -> connector.
            conn = uid_to_connector.get((charger, r.get("unique_id")))
            if conn is not None:
                txn_to_connector[(charger, payload.get("transactionId"))] = conn
        elif action == "StopTransaction":
            conn = txn_to_connector.get((charger, payload.get("transactionId")))
            key = (charger, conn)
            if key in open_sessions:
                s = open_sessions.pop(key)
                s["event_count"] += 1
                done.append(finalize(s, r["_time"], payload.get("reason")))
        elif action == "MeterValues":
            key = (charger, connector)
            if key in open_sessions:
                s = open_sessions[key]
                s["event_count"] += 1
                p = _power_sample(payload)
                if p is not None:
                    s["powers"].append(p)
                reg = read_measurand(payload, Measurand.ENERGY_REGISTER)
                if reg is not None:
                    if s["reg_first"] is None:
                        s["reg_first"] = reg
                    s["reg_last"] = reg
                # Record the charging-curve sample linked to this session,
                # timestamped by the MeterValues sample time where present.
                readings.append(
                    {
                        "session_id": s["session_id"],
                        "charger_id": charger,
                        "connector_id": s["connector_id"],
                        "timestamp": _metervalue_time(payload) or r["_time"],
                        "power_kw": p,
                        "soc_pct": read_measurand(payload, Measurand.SOC),
                        "energy_register_kwh": reg,
                    }
                )

    # Any still-open sessions are active/incomplete.
    for s in open_sessions.values():
        done.append(finalize(s, None, None))

    sessions_schema = {
        "session_id": pl.Utf8,
        "charger_id": pl.Utf8,
        "site_id": pl.Utf8,
        "connector_id": pl.Int64,
        "status": pl.Utf8,
        # Boundaries are tz-aware UTC: the fold works with UTC-aware datetimes
        # (see _coerce_ts), so pin the dtype to UTC rather than letting frame
        # construction strip the tz to naive — otherwise downstream comparisons
        # against tz-aware bounds raise a Polars supertype error.
        "start_time": pl.Datetime(time_zone="UTC"),
        "end_time": pl.Datetime(time_zone="UTC"),
        "duration": pl.Float64,
        "total_energy_kwh": pl.Float64,
        "avg_power": pl.Float64,
        "peak_power": pl.Float64,
        "event_count": pl.Int64,
        "stop_reason": pl.Utf8,
    }
    sessions = (
        # schema_overrides pins nullable columns (e.g. an all-None site_id or
        # duration) to their real dtype instead of Polars inferring Null.
        pl.DataFrame(done, schema_overrides=sessions_schema).select(SESSION_COLUMNS)
        if done
        else pl.DataFrame(schema=sessions_schema)
    )
    readings_df = (
        pl.DataFrame(readings, schema_overrides=READING_SCHEMA).select(READING_COLUMNS)
        if readings
        else pl.DataFrame(schema=READING_SCHEMA)
    )
    return sessions, readings_df
