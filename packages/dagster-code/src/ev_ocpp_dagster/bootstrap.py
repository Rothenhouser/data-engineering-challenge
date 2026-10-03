"""Manual one-time bootstrap for the cold+gold DuckLake.

First-time setup is manual: this script opens the shared ``connect`` to the
configured catalog + data dir, which auto-creates the DuckLake catalog file and
the data directory (and the parent dirs). It then prints the tables currently in
the catalog — empty on a fresh bootstrap, because the archive and gold tables
are created lazily by the first Dagster write (``append_frame`` /
``replace_table``). Schema migration is likewise manual and out of scope here.

Run it with ``uv run poe bootstrap`` (or ``python -m ev_ocpp_dagster.bootstrap``)
once before running the Dagster pipeline.
"""

from __future__ import annotations

from ev_ocpp_analysis import connect

from .config import LAKE_CATALOG, LAKE_DATA


def main() -> None:
    """Create (if absent) and inspect the DuckLake catalog."""
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
