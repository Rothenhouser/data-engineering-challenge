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
