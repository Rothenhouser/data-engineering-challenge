"""Manual one-time bootstrap for the Postgres-backed cold+gold DuckLake.

DuckLake catalog metadata lives in a dedicated
``ducklake_catalog`` database inside the existing Postgres service (so the
dashboard, Dagster and stream share one multi-client-safe catalog instead of
contending on an embedded DuckDB file). This script:

1. creates the ``ducklake_catalog`` database in Postgres if it does not exist
   (connecting to the server via ``OCPP_PG_URI`` and issuing ``CREATE DATABASE``
   against the default ``postgres`` maintenance DB), then
2. opens the shared ``connect`` to the configured catalog + data dir, which
   attaches the Postgres catalog (auto-creating the empty DuckLake on first
   attach) and the local data directory, and prints the tables currently in the
   catalog — empty on a fresh bootstrap, because the archive and gold tables are
   created lazily by the first Dagster write (``append_frame`` / ``replace_table``).

"""

from __future__ import annotations

import psycopg
from ev_ocpp_analysis import connect
from psycopg import sql

from .config import LAKE_CATALOG, LAKE_DATA, PG_URI

# Name of the Postgres database that holds the DuckLake catalog metadata. Kept
# in sync with the ``dbname=`` in the default LAKE_CATALOG spec in config.py.
CATALOG_DB = "ducklake_catalog"


def _ensure_catalog_db(pg_uri: str, dbname: str) -> bool:
    """Create ``dbname`` in Postgres if absent. Returns True if newly created.

    Connects to the server's default ``postgres`` maintenance database (not the
    app DB) in autocommit mode — ``CREATE DATABASE`` cannot run inside a
    transaction block. The connection info (host/user/password/port) is reused
    from ``pg_uri`` with only the database name swapped.
    """
    maint_uri = psycopg.conninfo.make_conninfo(pg_uri, dbname="postgres")
    with psycopg.connect(maint_uri, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", [dbname]).fetchone()
        if exists:
            return False
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        return True


def main() -> None:
    """Create (if absent) the catalog DB, then attach and inspect the DuckLake."""
    created = _ensure_catalog_db(PG_URI, CATALOG_DB)
    print(f"Postgres catalog database '{CATALOG_DB}': {'created' if created else 'already exists'}")

    with connect(LAKE_CATALOG, LAKE_DATA) as con:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = 'lake' ORDER BY table_name"
        ).fetchall()
    print(f"DuckLake ready: catalog={LAKE_CATALOG} data={LAKE_DATA}")
    if rows:
        print("Tables:")
        for (name,) in rows:
            print(f"  - {name}")
    else:
        print("No tables yet — they are created lazily by the first archive/gold write.")


if __name__ == "__main__":
    main()
