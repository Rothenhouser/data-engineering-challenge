"""Core OCPP analysis: parsing, session reconstruction, readers, persistence."""

from __future__ import annotations

from .analytics import DAILY_COLUMNS, compute_daily_stats
from .ducklake import (
    ANALYTICS_TABLE,
    ARCHIVE_TABLE,
    READINGS_TABLE,
    SESSIONS_TABLE,
    append_frame,
    catalog_overview,
    connect,
    read_gold_table,
    replace_table,
)
from .measurands import Measurand, read_measurand
from .parsing import (
    CALL,
    CALL_ERROR,
    CALL_RESULT,
    RawRow,
    iter_file,
    parse_raw_row,
)
from .pg import CREATE_TABLE_SQL, init_schema, write_raw_event, write_raw_events
from .readers import (
    read_archive_ducklake,
    read_latest_metervalues_per_station_postgres,
    read_latest_per_station_postgres,
    read_recent_window_postgres,
)
from .sessionization import (
    READING_COLUMNS,
    SESSION_COLUMNS,
    reconstruct_sessions,
    reconstruct_sessions_and_readings,
)
from .sim_control import (
    CREATE_SIM_CONTROL_SQL,
    init_sim_control,
    read_sim_control,
    write_sim_control,
)
from .sites import site_for

__all__ = [
    "CALL",
    "CALL_ERROR",
    "CALL_RESULT",
    "RawRow",
    "iter_file",
    "parse_raw_row",
    "Measurand",
    "read_measurand",
    "compute_daily_stats",
    "DAILY_COLUMNS",
    "site_for",
    "reconstruct_sessions",
    "reconstruct_sessions_and_readings",
    "SESSION_COLUMNS",
    "READING_COLUMNS",
    "read_archive_ducklake",
    "read_recent_window_postgres",
    "connect",
    "append_frame",
    "replace_table",
    "read_gold_table",
    "catalog_overview",
    "ARCHIVE_TABLE",
    "SESSIONS_TABLE",
    "READINGS_TABLE",
    "ANALYTICS_TABLE",
    "read_latest_per_station_postgres",
    "read_latest_metervalues_per_station_postgres",
    "CREATE_TABLE_SQL",
    "init_schema",
    "write_raw_event",
    "write_raw_events",
    "CREATE_SIM_CONTROL_SQL",
    "init_sim_control",
    "read_sim_control",
    "write_sim_control",
]
