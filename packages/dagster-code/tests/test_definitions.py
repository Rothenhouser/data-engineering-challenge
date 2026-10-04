"""The code location loads and wires the expected jobs/schedules/sensors."""

from __future__ import annotations

from ev_ocpp_dagster.definitions import defs
from ev_ocpp_dagster.partitions import CONTENT_PARTITIONS, INGESTION_PARTITIONS


def test_definitions_resolve() -> None:
    repo = defs.get_repository_def()
    assert {"archive_job", "session_job", "load_file_job"} <= {j.name for j in repo.get_all_jobs()}
    assert {"archive_schedule"} == {s.name for s in repo.schedule_defs}
    assert {
        "archive_to_sessions_sensor",
        "automation_sensor",
        "historical_file_sensor",
    } == {s.name for s in repo.sensor_defs}


def test_assets_have_expected_partitions() -> None:
    from ev_ocpp_dagster.analytics import gold_analytics_daily
    from ev_ocpp_dagster.dump import raw_events_archive
    from ev_ocpp_dagster.session import gold_sessions

    assert raw_events_archive.partitions_def == INGESTION_PARTITIONS
    assert gold_sessions.partitions_def == CONTENT_PARTITIONS
    assert gold_analytics_daily.partitions_def == CONTENT_PARTITIONS
