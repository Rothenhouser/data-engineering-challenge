# EV Coorp OCPP Analytics

Ingestion + analytics prototype for EV charging infrastructure emitting OCPP 1.6
messages. Turns raw charger traffic into queryable charging-session facts and
fleet analytics, with both live and historical views.

- Challenge brief: [`CHALLENGE.md`](CHALLENGE.md)
- Architecture & design decisions: [`docs/architecture/ocpp-ingestion-architecture.md`](docs/architecture/ocpp-ingestion-architecture.md)
- How to read the raw data: [`docs/ocpp-message-format.md`](docs/ocpp-message-format.md)

## How this answers the brief

- **One system, two sources, one shape** — a live stream and historical file
  drops are parsed by one shared library into one append-only Postgres table.
- **Real-time + historical** — the dashboard reads Postgres for live status and
  in-flight sessions, and the DuckLake gold tables for completed-session history
  and fleet rollups.
- **Aggregation across dimensions** — analytics roll up per charger, per
  connector, and per day, with a nullable `site_id` ready for multi-site.
- **Handles growing volume** — hot data in Postgres (short retention), cold data
  in an immutable DuckLake lakehouse; daily partitioning bounds each run.
- **Robust to messy data** — explicit `completed` / `active` / `incomplete`
  session status; duplicate-tolerant; unparseable frames counted, never crash.
- **Portable deployment** — the whole topology is docker-compose; the design doc
  lists the exact swaps for a production (object-store + managed Postgres) path.

## Architecture

Both sources are parsed by one shared library and land in one append-only
Postgres table. From there the data tiers hot → cold → gold:

```
many-chargers.txt ─(stream consumer)─┐
                                      ├─> Postgres raw_events ─> DuckLake archive ─> DuckLake gold (sessions, readings, daily analytics)
many-days.txt ─────(Dagster loader)──┘       (hot, short retention)   (cold, immutable)         (Streamlit dashboard)
```

- **Shared library** (`ev_ocpp_analysis`) — OCPP parsing + one Polars
  `reconstruct_sessions` fold powering both live and historical views. Our schema
  calls each unit a **charger** (OCPP: *charge point*), `charger_id`; a charger
  may expose several connectors.
- **Postgres `raw_events`** — durable, append-only landing zone (psycopg, no ORM).
- **DuckLake** (cold archive + gold) — a lakehouse: Parquet table data + a SQL
  catalog. The catalog metadata lives in the shared **Postgres** (database
  `ducklake_catalog`) so dashboard, Dagster and stream attach concurrently; the
  Parquet data sits under `data/lake/data`.
- **Dagster** — archive (partitioned by ingestion day), session reconstruction
  (sessions + per-session readings, by content day), daily analytics (energy /
  utilization / faults, by content day). See [Partitioning](#partitioning).
- **Streamlit dashboard** — live charger overview, raw-data inspector, session
  explorer with charging curves, and per-charger/connector fleet analytics.
- All business logic is Polars; the only SQL is parameterized inserts, ConnectorX
  live reads, and DuckLake ATTACH / CREATE / INSERT / SELECT. No ORM, no Redis.

## Sessionization

One shared Polars fold (`reconstruct_sessions`) rebuilds sessions from raw events,
so a session reconstructed live matches the same session from the archive. It is
an ordered, stateful fold (not a `GROUP BY`) because the session key only exists
once the opening `StartTransaction` arrives.

- **Order** — events are deduplicated (identical frames collapsed, so replays
  never double-count). Boundary/reading *times* come from the payload timestamp;
  the fold's *processing order* is arrival order (`ingest_ts`, then row index) so
  a Call and its CallResult stay adjacent for OCPP correlation.
- **Lifecycle** — `StartTransaction` opens, `MeterValues` accumulate
  power/SoC/register, `StopTransaction` closes. Keyed by
  `charger_id + connector_id + start_time`. A Stop carries only `transactionId`,
  so the fold threads `unique_id → connector` (from the Start CallResult) and
  `transactionId → connector` to re-link it.
- **Parallel sessions** — one open session per `(charger_id, connector_id)`, so a
  charger's connectors reconstruct independently and concurrently.
- **Energy** — meter register delta (`Energy.Active.Import.Register` last − first);
  falls back to the brief's `avg(power) × duration` when no register readings exist.
- **Status** — exactly one of `completed` (saw a Stop), `active` (open, recent),
  `incomplete` (open, older). Duration is null until a Stop closes it.
- **Output** — session facts + a per-session readings frame (charging curve, one
  row per `MeterValues`: `charger_id`, `connector_id`, timestamp, power/SoC/
  register), linked by `session_id`. Analytics roll up per `(charger_id,
  connector_id, day)`.

## Partitioning

Two independent daily grains keep each run bounded:

- **Raw archive — ingestion day.** `raw_events_archive` partitioned by `ingest_ts`
  (wall-clock), open-ended; `archive_schedule` materializes today's partition
  every 5 min, idempotently replacing that day's slice (no duplicates on re-run).
- **Gold — content day.** `gold_sessions` / `gold_analytics_daily` partitioned by
  session `start_time`, over a static range (`2025-08-20` … `2025-08-31`).
- **Fan-out.** An asset sensor on `raw_events_archive` maps a newly-archived
  ingestion day to the content days it touched and requests `session_job` for
  each; analytics re-materialize those days eagerly (`AutomationCondition`).

Tradeoff: `gold_sessions` still reads the *full* archive per run — fine for the
demonstrator; the scalable two-stage design is in the architecture doc.

## Run it

```bash
wsl sudo docker compose -f deploy/docker-compose.yml up --build
uv run poe bootstrap   # once: create the ducklake_catalog DB + raw_events table
```

- Dashboard http://localhost:8501 · Dagster http://localhost:3000 · Postgres `localhost:5432`
- **Live** — the stream auto-replays `many-chargers.txt` into `raw_events`.
- **Historical** — run `load_file_job` manually (Dagster UI → Jobs →
  `load_file_job` → Launchpad), op config path `/app/data/ocpp-data-many-days.txt`.
  A file-drop sensor exists but manual launch is the intended path for the demo.
- Gold then populates on its own (archive schedule → session fan-out → eager
  analytics; see [Partitioning](#partitioning)).

Useful `poe` tasks (`uv run poe <task>`):

- `bootstrap` — create the DuckLake catalog DB + `raw_events` table.
- `reset` — DESTRUCTIVE: wipe all local state (Postgres tables, catalog DB, lake
  data, Dagster runs) and re-bootstrap a clean slate.
- `test` — property-based + unit tests. `lint` — ruff format + check.
- `sync && dashboard` — run the dashboard locally with hot reload against the
  containerized Postgres.

> First boot needs outbound network: the DuckLake DuckDB extension is fetched via
> `INSTALL ducklake` on first run (not baked into the image).

## Inspecting the lake

Small tables may be stored *inline in the catalog* rather than as Parquet, so a
table's data dir can be empty even with rows — query the table (or the dashboard
DuckLake page), don't `ls` the data dir. Interactive read-only session:

```sql
INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;
SET TimeZone = 'UTC';
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost user=ocpp password=ocpp' AS lake
  (DATA_PATH 'data/lake/data', OVERRIDE_DATA_PATH TRUE, READ_ONLY);
SELECT * FROM lake.main.gold_analytics_daily;
```

`READ_ONLY` lets you inspect while the app holds the catalog open. `duckdb -ui`
opens the same in a browser.

## Notes & production concerns

- **Sites** — sessions and analytics carry a nullable `site_id` from an optional
  `charger_id → site_id` map (`OCPP_SITE_MAP` env, JSON); unset means `None`.
- **Live clock** — the feed replays historical timestamps, so live views derive
  "now" from the newest payload time in the window, keeping duration/status right.
- **Tests** — hypothesis property tests cover the fold (duplicate tolerance,
  energy, status totality, determinism) plus fault reconciliation (`poe test`).
- **Deferred for production** (see design doc) — object-store DuckLake data path,
  partitioned/compacted archive, managed Postgres, separate Dagster metadata DB,
  the two-stage sessionization shuffle, and source-level dedup.
