"""Shared paths and connection config for the Dagster code location.

All values come from environment variables so the same code runs locally and in
the docker-compose topology against the shared volume.

The cold archive and gold layers live in a local DuckLake: ``LAKE_CATALOG`` is
the DuckDB catalog file and ``LAKE_DATA`` is the directory holding the table
Parquet data, both under ``data/lake/`` on the shared volume.
"""

from __future__ import annotations

import os

# Postgres landing-zone connection URI (psycopg / ConnectorX form).
PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")

# Watched input directory for the historical file-drop loader.
DATA_DIR = os.environ.get("OCPP_DATA_DIR", "data")

# DuckLake cold-archive + gold storage (embedded; catalog file + data dir).
LAKE_CATALOG = os.environ.get("OCPP_LAKE_CATALOG", "data/lake/catalog.ducklake")
LAKE_DATA = os.environ.get("OCPP_LAKE_DATA", "data/lake/data")
