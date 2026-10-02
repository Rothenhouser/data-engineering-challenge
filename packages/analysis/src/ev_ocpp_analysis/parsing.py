"""Parse raw OCPP log lines into structured records.

Each line is one OCPP-J message `stationId : [...]`:
    chargerN : [2, "<UniqueId>", "<Action>", {payload}]              # Call
    chargerN : [3, "<UniqueId>", {payload}]                          # CallResult
    chargerN : [4, "<UniqueId>", "<errCode>", "<errDesc>", {detail}] # CallError

arr[0] is the MessageTypeId (2/3/4); arr[1] is the UniqueId, used only to match a
response to its request (a CallResult reuses its Call's UniqueId). UniqueId is
unique per sender+connection, NOT globally — do not use it as a row key.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator

# OCPP message type ids
CALL = 2  # request: [2, id, action, payload]
CALL_RESULT = 3  # response: [3, id, payload]
CALL_ERROR = 4  # error response: [4, id, errorCode, errorDescription, details]


@dataclass
class ParsedEvent:
    station_id: str
    msg_type: int
    unique_id: str  # request-correlation id; unique per sender+connection, not global
    action: str | None  # None for CallResults/CallErrors
    payload: dict[str, Any]
    # Common fields lifted from the payload when present
    connector_id: int | None = None
    transaction_id: int | None = None
    timestamp: str | None = None
    # Pivoted MeterValues measurands: measurand -> float value
    measurands: dict[str, float] = field(default_factory=dict)
    meter_context: str | None = None
    # CallError only
    error_code: str | None = None
    error_description: str | None = None


@dataclass
class ParseStats:
    total: int = 0
    parsed: int = 0
    skipped: int = 0


def _split_line(line: str) -> tuple[str, str] | None:
    # Station and the JSON array are separated by " : "
    sep = line.find(" : ")
    if sep == -1:
        return None
    station = line[:sep].strip()
    body = line[sep + 3 :].strip()
    if not station or not body:
        return None
    return station, body


def _pivot_meter_values(payload: dict[str, Any]) -> tuple[dict[str, float], str | None, str | None]:
    """Flatten the first meterValue entry into {measurand: value}, plus its
    timestamp and sampling context."""
    measurands: dict[str, float] = {}
    ts: str | None = None
    context: str | None = None
    meter_values = payload.get("meterValue") or []
    if not meter_values:
        return measurands, ts, context
    first = meter_values[0]
    ts = first.get("timestamp")
    for sv in first.get("sampledValue", []):
        name = sv.get("measurand", "Energy.Active.Import.Register")
        context = context or sv.get("context")
        try:
            measurands[name] = float(sv["value"])
        except (KeyError, TypeError, ValueError):
            continue
    return measurands, ts, context


def parse_line(line: str) -> ParsedEvent | None:
    """Parse one raw line. Return None if it is blank or malformed."""
    line = line.strip()
    if not line:
        return None
    split = _split_line(line)
    if split is None:
        return None
    station, body = split
    try:
        arr = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(arr, list) or len(arr) < 3:
        return None

    msg_type = arr[0]
    unique_id = str(arr[1])
    action = None
    error_code = None
    error_description = None

    if msg_type == CALL and len(arr) >= 4:
        action = arr[2]
        payload = arr[3] if isinstance(arr[3], dict) else {}
    elif msg_type == CALL_RESULT:
        payload = arr[2] if isinstance(arr[2], dict) else {}
    elif msg_type == CALL_ERROR and len(arr) >= 5:
        error_code = arr[2]
        error_description = arr[3]
        payload = arr[4] if isinstance(arr[4], dict) else {}
    else:
        return None

    event = ParsedEvent(
        station_id=station,
        msg_type=msg_type,
        unique_id=unique_id,
        action=action,
        payload=payload,
        connector_id=payload.get("connectorId"),
        transaction_id=payload.get("transactionId"),
        timestamp=payload.get("timestamp") or payload.get("currentTime"),
        error_code=error_code,
        error_description=error_description,
    )

    if action == "MeterValues":
        measurands, ts, context = _pivot_meter_values(payload)
        event.measurands = measurands
        event.meter_context = context
        if ts:
            event.timestamp = ts

    return event


def parse_lines(lines: Iterable[str]) -> tuple[list[ParsedEvent], ParseStats]:
    """Parse an iterable of lines, collecting skip stats."""
    stats = ParseStats()
    events: list[ParsedEvent] = []
    for line in lines:
        if not line.strip():
            continue
        stats.total += 1
        event = parse_line(line)
        if event is None:
            stats.skipped += 1
            continue
        stats.parsed += 1
        events.append(event)
    return events, stats


def iter_file(path: str) -> Iterator[str]:
    with open(path, "r", encoding="utf-8") as fh:
        yield from fh
