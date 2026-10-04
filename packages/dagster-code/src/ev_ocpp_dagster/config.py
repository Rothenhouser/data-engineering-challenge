"""Shared paths and connection config for the Dagster code location.

All values come from environment variables so the same code runs locally and in
the docker-compose topology against the shared volume.

The cold archive and gold layers live in a DuckLake whose catalog metadata is
kept in the shared Postgres service (``LAKE_CATALOG`` is the DuckLake
``postgres:`` catalog spec) and whose table Parquet data lives under
``LAKE_DATA`` (``data/lake/data`` on the shared volume).
"""

from __future__ import annotations

import os

# Postgres landing-zone connection URI (psycopg / ConnectorX form).
PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")

# Watched input directory for the historical file-drop loader.
DATA_DIR = os.environ.get("OCPP_DATA_DIR", "data")

# DuckLake catalog: Postgres-backed metadata (shared, multi-client safe). The
# ``ducklake_catalog`` database must exist (create it with `poe bootstrap`).
LAKE_CATALOG = os.environ.get(
    "OCPP_LAKE_CATALOG",
    "postgres:dbname=ducklake_catalog host=localhost user=ocpp password=ocpp",
)
# DuckLake table data (Parquet files) directory on the shared volume.
LAKE_DATA = os.environ.get("OCPP_LAKE_DATA", "data/lake/data")
