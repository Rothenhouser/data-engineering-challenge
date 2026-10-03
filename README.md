# EV Coorp OCPP Analytics

Ingestion + analytics prototype for EV charging infrastructure emitting OCPP 1.6
messages. The original challenge brief is in [`CHALLENGE.md`](CHALLENGE.md); the
full design and requirements live in [`.kiro/specs/ocpp-ingestion-architecture`](.kiro/specs/ocpp-ingestion-architecture).

## Architecture

Both sources — a live stream and historical file drops — are parsed by one
shared library and land in one append-only Postgres table. From there the data
tiers hot → cold → gold:

```
many-chargers.txt ─(stream consumer)─┐
                                      ├─> Postgres raw_events ─> DuckLake archive ─> DuckLake gold (sessions, readings, daily analytics)
many-days.txt ─────(Dagster loader)──┘       (hot, short retention)   (cold, immutable)         (Streamlit dashboard)
```

Cold archive and gold are stored in an embedded **DuckLake**: table data stays
in Parquet files, metadata lives in a local DuckDB catalog file. It runs
in-process (no service), on the local filesystem (no object store), under
`data/lake/`.

- **Shared library** (`ev_ocpp_analysis`): OCPP parsing + one Polars
  `reconstruct_sessions` fold that powers both live and historical views.
- **Postgres** `raw_events`: durable, append-only landing zone (psycopg, no ORM).
- **Dagster jobs**: dump-to-DuckLake (archive), session reconstruction (gold
  sessions + per-session readings), daily analytics (energy / utilization / faults).
- **Streamlit dashboard**: live charger overview, raw-data inspector, session
  explorer with charging curves, fleet analytics, and a simulation control page.
- All business logic is Polars; the only SQL is parameterized inserts, ConnectorX
  live reads, and DuckLake ATTACH / CREATE / INSERT / SELECT.

## Run it

```bash
wsl sudo docker compose -f deploy/docker-compose.yml up --build
```

- Dashboard: http://localhost:8501 · Dagster UI: http://localhost:3000 · Postgres: `localhost:5432`
- The stream replays `many-chargers.txt`; drop `many-days.txt` is loaded by the
  Dagster sensor. Trigger `session_job` then `analytics_job` to populate gold.
- The DuckLake extension is a DuckDB native extension (not a pip package): the
  containers fetch it over the network on first run via `INSTALL ducklake` and
  cache it. First boot therefore needs outbound network; it is not baked into
  the image.

First-time DuckLake setup is manual: run `uv run poe bootstrap` once to create
the cold+gold catalog + data dir before the pipeline runs (archive/gold tables
are created lazily by the first run).

Local dashboard dev (hot reload) against the containerized Postgres:
`uv run poe sync && uv run poe dashboard`.

## Notes / shortcuts

- **Energy** uses the meter register delta (`Energy.Active.Import.Register`
  last − first), which is more accurate than the brief's sanctioned
  `avg(power) × duration` approximation; that approximation remains the fallback
  when no register readings exist.
- **Status** is explicit: `completed` / `active` / `incomplete`; duration is null
  until a StopTransaction closes the session.
- **Live clock**: the feed replays historical timestamps, so live views derive
  "now" from the newest payload time (toggleable on the Simulation page).
- **Sites**: sessions and daily analytics carry a nullable `site_id`, populated
  from an optional `station_id -> site_id` map (`OCPP_SITE_MAP` env, JSON); unset
  means `None`. Ready for multi-site rollups.
- **Tests**: property-based tests (hypothesis) cover the fold's correctness
  properties — duplicate tolerance, energy derivation, status totality, and
  determinism — plus daily-analytics fault reconciliation. Run with `uv run poe test`.
- **DuckLake**: cold archive + gold are DuckLake tables = Parquet data + a local
  DuckDB catalog file, embedded (no service), on the local filesystem (no object
  store). First init is the manual `uv run poe bootstrap`; schema migration is
  manual and out of scope for this prototype.
- **Deferred for production** (see design doc): object-store DuckLake data path,
  partitioned archive, managed Postgres, separate Dagster metadata DB, and
  source-level dedup.
