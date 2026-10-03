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
                                      ├─> Postgres raw_events ─> Parquet archive ─> gold (sessions, readings, daily analytics)
many-days.txt ─────(Dagster loader)──┘        (hot, short retention)   (cold, immutable)        (Streamlit dashboard)
```

- **Shared library** (`ev_ocpp_analysis`): OCPP parsing + one Polars
  `reconstruct_sessions` fold that powers both live and historical views.
- **Postgres** `raw_events`: durable, append-only landing zone (psycopg, no ORM).
- **Dagster jobs**: dump-to-Parquet (archive), session reconstruction (gold
  sessions + per-session readings), daily analytics (energy / utilization / faults).
- **Streamlit dashboard**: live charger overview, raw-data inspector, session
  explorer with charging curves, fleet analytics, and a simulation control page.
- All business logic is Polars; the only SQL is parameterized inserts, ConnectorX
  live reads, and DuckDB archive scans.

## Run it

```bash
docker compose -f deploy/docker-compose.yml up --build
```

- Dashboard: http://localhost:8501 · Dagster UI: http://localhost:3000 · Postgres: `localhost:5432`
- The stream replays `many-chargers.txt`; drop `many-days.txt` is loaded by the
  Dagster sensor. Trigger `session_job` then `analytics_job` to populate gold.

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
- **Deferred for production** (see design doc): object-store Parquet, partitioned
  archive, managed Postgres, separate Dagster metadata DB, and source-level dedup.
