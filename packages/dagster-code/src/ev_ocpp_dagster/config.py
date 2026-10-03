"""Shared paths and connection config for the Dagster code location.

All values come from environment variables so the same code runs locally and in
the docker-compose topology against the shared volume.
"""

from __future__ import annotations

import os

# Postgres landing-zone connection URI (psycopg / ConnectorX form).
PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")

# Shared-volume layout.
DATA_DIR = os.environ.get("OCPP_DATA_DIR", "data")  # watched input directory
ARCHIVE_DIR = os.environ.get("OCPP_ARCHIVE_DIR", "data/archive")  # cold Parquet archive
GOLD_DIR = os.environ.get("OCPP_GOLD_DIR", "data/gold")  # Parquet session facts

ARCHIVE_GLOB = os.path.join(ARCHIVE_DIR, "*.parquet")
GOLD_PATH = os.path.join(GOLD_DIR, "sessions.parquet")
GOLD_READINGS_PATH = os.path.join(GOLD_DIR, "session_readings.parquet")
WATERMARK_PATH = os.path.join(ARCHIVE_DIR, "_watermark.txt")
