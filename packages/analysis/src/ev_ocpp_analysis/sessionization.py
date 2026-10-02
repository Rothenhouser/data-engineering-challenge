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


def _power_sample(payload: dict[str, Any]) -> float | None:
    """Pull the Power.Active.Import sample from a MeterValues payload."""
    for mv in payload.get("meterValue") or []:
        for sv in mv.get("sampledValue") or []:
            if sv.get("measurand") == "Power.Active.Import":
                try:
                    return float(sv["value"])
                except (KeyError, TypeError, ValueError):
                    return None
    return None


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
    """Reconstruct charging sessions from a raw-event frame.

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
            "session_id": f"{s['station_id']}|{s['connector_id']}|{start.isoformat()}",
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

    # Any still-open sessions are active/incomplete.
    for s in open_sessions.values():
        done.append(finalize(s, None, None))

    if not done:
        schema = {
            "session_id": pl.Utf8, "station_id": pl.Utf8, "connector_id": pl.Int64,
            "status": pl.Utf8, "start_time": pl.Datetime, "end_time": pl.Datetime,
            "duration": pl.Float64, "total_energy_kwh": pl.Float64, "avg_power": pl.Float64,
            "peak_power": pl.Float64, "event_count": pl.Int64, "stop_reason": pl.Utf8,
        }
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(done).select(SESSION_COLUMNS)
