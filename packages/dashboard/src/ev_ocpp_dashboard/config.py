"""Dashboard config (shared-volume paths + Postgres URI) from env."""

from __future__ import annotations

import os

PG_URI = os.environ.get("OCPP_PG_URI", "postgresql://ocpp:ocpp@localhost:5432/ocpp")
GOLD_PATH = os.environ.get("OCPP_GOLD_PATH", "data/gold/sessions.parquet")
# Cold Parquet archive directory (one immutable file per dump).
ARCHIVE_DIR = os.environ.get("OCPP_ARCHIVE_DIR", "data/archive")
# How far back the in-flight live window reaches, in minutes.
WINDOW_MINUTES = int(os.environ.get("OCPP_WINDOW_MINUTES", "60"))
