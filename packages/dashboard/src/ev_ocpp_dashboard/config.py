"""Dashboard config (shared-volume paths + Postgres URI) from env."""

from __future__ import annotations

import os

PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")
# Cold-archive + gold DuckLake (embedded; catalog file + data dir).
LAKE_CATALOG = os.environ.get("OCPP_LAKE_CATALOG", "data/lake/catalog.ducklake")
LAKE_DATA = os.environ.get("OCPP_LAKE_DATA", "data/lake/data")
# How far back the in-flight live window reaches, in minutes.
WINDOW_MINUTES = int(os.environ.get("OCPP_WINDOW_MINUTES", "60"))
