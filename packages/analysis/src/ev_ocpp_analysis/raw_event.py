"""Reconcile a :class:`ParsedEvent` to the persisted ``RawEvent`` storage shape.

A ``RawEvent`` row is the single raw-landing contract both ingestion paths (the
standalone stream consumer and the Dagster batch loader) write to. Keeping the
mapping in one place guarantees an identical field shape across sources
(Requirement 1.5).

The row carries exactly the structural OCPP fields:

    station_id, msg_type, unique_id, action, payload

The ``payload`` is carried through AS-IS — the uninterpreted payload dict exactly
as the parser split it out of the frame. No convenience fields (connector_id,
transaction_id, measurands, timestamp, error_code, ...) are lifted into columns;
those are derived later by the sessionization fold from the stored payload
(Requirements 1.6, 2.3).

``action`` is the OCPP Action for a Call and ``None`` for a CallResult or
CallError — a distinction the parser already encodes on ``ParsedEvent.action``,
so it is carried through unchanged (Requirement 1.7).

``event_id`` (a surrogate key) and ``ingest_ts`` (wall-clock ingest time) are
NOT produced here: they are supplied by the persistence layer at write time, so
they deliberately do not appear in the returned mapping (Requirement 2.1, 2.4).
"""

from __future__ import annotations

from typing import Any

from .parsing import ParsedEvent

# The persisted raw-landing columns this helper is responsible for, in order.
# event_id and ingest_ts are added by the persistence layer, not here.
RAW_EVENT_FIELDS: tuple[str, ...] = (
    "station_id",
    "msg_type",
    "unique_id",
    "action",
    "payload",
)


def to_raw_event_row(event: ParsedEvent) -> dict[str, Any]:
    """Project a :class:`ParsedEvent` onto the ``RawEvent`` storage shape.

    Returns a mapping with exactly the five storage columns
    (``station_id``, ``msg_type``, ``unique_id``, ``action``, ``payload``).
    ``payload`` is the same uninterpreted dict the parser produced, carried
    through as-is with no fields lifted into columns. ``action`` is already the
    OCPP Action for a Call and ``None`` for a CallResult/CallError on the
    ParsedEvent, so it is passed through unchanged.

    ``event_id`` and ``ingest_ts`` are intentionally absent — the persistence
    layer supplies them at write time.
    """
    return {
        "station_id": event.station_id,
        "msg_type": event.msg_type,
        "unique_id": event.unique_id,
        "action": event.action,
        "payload": event.payload,
    }
