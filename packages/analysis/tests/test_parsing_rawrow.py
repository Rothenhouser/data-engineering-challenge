"""Unit tests for the lean raw-landing parser and RawRow storage shape."""

from __future__ import annotations

from ev_ocpp_analysis import CALL, CALL_ERROR, CALL_RESULT, RawRow, parse_raw_row


def test_parse_call_line() -> None:
    line = 'charger1 : [2, "uid-1", "BootNotification", {"chargePointModel": "X"}]'
    row = parse_raw_row(line)
    assert row is not None
    assert row.charger_id == "charger1"
    assert row.msg_type == CALL
    assert row.unique_id == "uid-1"
    assert row.action == "BootNotification"
    assert row.payload == {"chargePointModel": "X"}


def test_parse_call_result_line_has_no_action() -> None:
    line = 'charger2 : [3, "uid-2", {"status": "Accepted"}]'
    row = parse_raw_row(line)
    assert row is not None
    assert row.charger_id == "charger2"
    assert row.msg_type == CALL_RESULT
    assert row.unique_id == "uid-2"
    assert row.action is None
    assert row.payload == {"status": "Accepted"}


def test_parse_call_error_line_uses_detail_payload() -> None:
    line = 'charger3 : [4, "uid-3", "NotSupported", "nope", {"detail": 1}]'
    row = parse_raw_row(line)
    assert row is not None
    assert row.charger_id == "charger3"
    assert row.msg_type == CALL_ERROR
    assert row.unique_id == "uid-3"
    assert row.action is None
    assert row.payload == {"detail": 1}


def test_blank_and_malformed_lines_return_none() -> None:
    assert parse_raw_row("") is None
    assert parse_raw_row("   ") is None
    assert parse_raw_row("charger4 : not-json") is None
    assert parse_raw_row("no-separator-here") is None
    assert parse_raw_row('charger4 : [2, "uid"]') is None  # too short


def test_as_row_returns_exactly_five_storage_columns() -> None:
    row = RawRow(
        charger_id="charger5",
        msg_type=CALL,
        unique_id="uid-5",
        action="Heartbeat",
        payload={"k": "v"},
    )
    assert row.as_row() == {
        "charger_id": "charger5",
        "msg_type": CALL,
        "unique_id": "uid-5",
        "action": "Heartbeat",
        "payload": {"k": "v"},
    }
