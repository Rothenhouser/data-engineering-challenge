"""Parse raw OCPP log lines into the raw-landing storage shape.

Each line is one OCPP-J message `chargerId : [...]`:
    chargerN : [2, "<UniqueId>", "<Action>", {payload}]              # Call
    chargerN : [3, "<UniqueId>", {payload}]                          # CallResult
    chargerN : [4, "<UniqueId>", "<errCode>", "<errDesc>", {detail}] # CallError

arr[0] is the MessageTypeId (2/3/4); arr[1] is the UniqueId, used only to match a
response to its request (a CallResult reuses its Call's UniqueId). UniqueId is
unique per sender+connection, NOT globally — do not use it as a row key.

The parser produces exactly the five raw-landing columns both ingestion paths
persist (``charger_id``, ``msg_type``, ``unique_id``, ``action``, ``payload``).
``payload`` is carried through AS-IS; no convenience fields are lifted into
columns and no MeterValues pivot is performed — those are derived later by the
sessionization fold. ``event_id`` and ``ingest_ts`` are supplied by the
persistence layer at write time, so they are not part of :class:`RawRow`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

# OCPP message type ids
CALL = 2  # request: [2, id, action, payload]
CALL_RESULT = 3  # response: [3, id, payload]
CALL_ERROR = 4  # error response: [4, id, errorCode, errorDescription, details]


@dataclass(slots=True, frozen=True)
class RawRow:
    """The five raw-landing storage columns, carried through AS-IS.

    ``action`` is the OCPP Action for a Call and ``None`` for a CallResult or
    CallError. ``event_id`` and ``ingest_ts`` are NOT held here — the persistence
    layer supplies them at write time.
    """

    charger_id: str
    msg_type: int
    unique_id: str  # request-correlation id; unique per sender+connection, not global
    action: str | None  # None for CallResults/CallErrors
    payload: dict[str, Any]

    def as_row(self) -> dict[str, Any]:
        """Project onto the storage-column mapping the pg writers consume."""
        return {
            "charger_id": self.charger_id,
            "msg_type": self.msg_type,
            "unique_id": self.unique_id,
            "action": self.action,
            "payload": self.payload,
        }


def _split_line(line: str) -> tuple[str, str] | None:
    # Charger id and the JSON array are separated by " : "
    sep = line.find(" : ")
    if sep == -1:
        return None
    charger = line[:sep].strip()
    body = line[sep + 3 :].strip()
    if not charger or not body:
        return None
    return charger, body


def parse_raw_row(line: str) -> RawRow | None:
    """Parse one raw line into a :class:`RawRow`. Return None if blank/malformed."""
    line = line.strip()
    if not line:
        return None
    split = _split_line(line)
    if split is None:
        return None
    charger, body = split
    try:
        arr = json.loads(body)
    except json.JSONDecodeError:
        return None
    if not isinstance(arr, list) or len(arr) < 3:
        return None

    msg_type = arr[0]
    unique_id = str(arr[1])
    action = None

    if msg_type == CALL and len(arr) >= 4:
        action = arr[2]
        payload = arr[3] if isinstance(arr[3], dict) else {}
    elif msg_type == CALL_RESULT:
        payload = arr[2] if isinstance(arr[2], dict) else {}
    elif msg_type == CALL_ERROR and len(arr) >= 5:
        payload = arr[4] if isinstance(arr[4], dict) else {}
    else:
        return None

    return RawRow(
        charger_id=charger,
        msg_type=msg_type,
        unique_id=unique_id,
        action=action,
        payload=payload,
    )


def iter_file(path: str) -> Iterator[str]:
    with open(path, encoding="utf-8") as fh:
        yield from fh
