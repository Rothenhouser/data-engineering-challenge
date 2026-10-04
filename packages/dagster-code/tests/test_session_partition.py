"""Content-day partitioning + idempotent readings replace for gold_sessions."""

from __future__ import annotations

import polars as pl
from ev_ocpp_analysis import connect, reconstruct_sessions_and_readings
from ev_ocpp_dagster.session import _replace_readings


def _txn(charger: str, start: str, stop: str, uid: str, txn: int) -> list[dict]:
    """Minimal Start/StartResult/Meter/Stop frames for one complete session."""
    return [
        {
            "charger_id": charger,
            "msg_type": 2,
            "unique_id": uid,
            "action": "StartTransaction",
            "payload": {"connectorId": 1, "timestamp": start},
            "ingest_ts": start,
        },
        {
            "charger_id": charger,
            "msg_type": 3,
            "unique_id": uid,
            "action": None,
            "payload": {"transactionId": txn},
            "ingest_ts": start,
        },
        {
            "charger_id": charger,
            "msg_type": 2,
            "unique_id": f"{uid}-mv",
            "action": "MeterValues",
            "payload": {
                "connectorId": 1,
                "meterValue": [
                    {
                        "timestamp": start,
                        "sampledValue": [
                            {"measurand": "Power.Active.Import", "value": "7.0"},
                        ],
                    }
                ],
            },
            "ingest_ts": start,
        },
        {
            "charger_id": charger,
            "msg_type": 2,
            "unique_id": f"{uid}-stop",
            "action": "StopTransaction",
            "payload": {"transactionId": txn, "timestamp": stop, "reason": "Local"},
            "ingest_ts": stop,
        },
    ]


def _archive_frame() -> pl.DataFrame:
    rows = _txn("c1", "2025-08-20T10:00:00+00:00", "2025-08-20T11:00:00+00:00", "u1", 1) + _txn(
        "c1", "2025-08-21T10:00:00+00:00", "2025-08-21T11:00:00+00:00", "u2", 2
    )
    return pl.DataFrame(rows, schema_overrides={"payload": pl.Object})


def test_day_filter_selects_one_content_day() -> None:
    """Folding then filtering by start-time day isolates that day's sessions."""
    sessions, _ = reconstruct_sessions_and_readings(_archive_frame())
    day20 = sessions.filter(pl.col("start_time").dt.date().cast(pl.Utf8) == "2025-08-20")
    assert day20.height == 1
    assert day20["session_id"].to_list()[0].startswith("c1|1|2025-08-20")


def test_replace_readings_idempotent(tmp_path) -> None:
    """Re-writing a day's readings replaces by session_id without duplicating."""
    sessions, readings = reconstruct_sessions_and_readings(_archive_frame())
    ids20 = sessions.filter(pl.col("start_time").dt.date().cast(pl.Utf8) == "2025-08-20")[
        "session_id"
    ].to_list()
    r20 = readings.filter(pl.col("session_id").is_in(ids20))

    catalog = str(tmp_path / "catalog.ducklake")
    data = str(tmp_path / "data")
    with connect(catalog, data) as con:
        _replace_readings(con, ids20, r20)
        _replace_readings(con, ids20, r20)  # re-run same day
        n = con.execute("SELECT count(*) FROM lake.main.gold_session_readings").fetchone()[0]
    assert n == r20.height
