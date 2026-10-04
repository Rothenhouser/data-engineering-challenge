"""Dashboard config (shared-volume paths + Postgres URI) from env."""

from __future__ import annotations

import os

PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")
# Cold-archive + gold DuckLake: Postgres-backed catalog (shared, multi-client
# safe) + Parquet table data on the shared volume.
LAKE_CATALOG = os.environ.get(
    "OCPP_LAKE_CATALOG",
    "postgres:dbname=ducklake_catalog host=localhost user=ocpp password=ocpp",
)
LAKE_DATA = os.environ.get("OCPP_LAKE_DATA", "data/lake/data")
# How far back the in-flight live window reaches, in minutes.
WINDOW_MINUTES = int(os.environ.get("OCPP_WINDOW_MINUTES", "60"))
