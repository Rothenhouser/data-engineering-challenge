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
  `reconstruct_sessions` fold that powers both live and historical views. Our
  schema calls each unit a charger (OCPP: charge point), identified by
  `charger_id`; a charger may expose several connectors.
- **Postgres** `raw_events`: durable, append-only landing zone (psycopg, no ORM).
- **Dagster jobs**: dump-to-DuckLake (archive), session reconstruction (gold
  sessions + per-session readings), daily analytics (energy / utilization / faults).
- **Streamlit dashboard**: live charger overview, raw-data inspector, session
  explorer with charging curves, fleet analytics, and a simulation control page.
- All business logic is Polars; the only SQL is parameterized inserts, ConnectorX
  live reads, and DuckLake ATTACH / CREATE / INSERT / SELECT.

## Sessionization

One shared Polars fold (`reconstruct_sessions`) rebuilds charging sessions from
raw OCPP events, so a session reconstructed live matches the same session
reconstructed from the archive. It is an ordered, stateful fold (not a GROUP BY)
because the session key only exists once the opening `StartTransaction` arrives.

- **Order**: events are deduplicated (content-identical frames collapsed, so
  replays never double-count). Session boundary and reading times come from the
  payload timestamp when present; the *processing order* of the fold is arrival
  order (`ingest_ts`, then original row index as a stable tiebreak) so a Call and
  its CallResult stay adjacent — the OCPP correlation below depends on it.
- **Lifecycle**: `StartTransaction` opens a session; `MeterValues` accumulate
  power/SoC/register samples while open; `StopTransaction` closes it. A session
  is keyed by `charger_id + connector_id + start_time`. Because a Stop carries a
  `transactionId` but no connector, the fold threads `unique_id -> connector`
  (from the Start CallResult) and `transactionId -> connector` maps to re-link it.
- **Parallel sessions**: a charger (OCPP: charge point) has one or more
  connectors, so a single charger can run concurrent transactions. The fold holds
  one open session per `(charger_id, connector_id)`, so sessions on different
  connectors of the same charger reconstruct independently and in parallel.
- **Energy**: meter register delta (`Energy.Active.Import.Register` last − first),
  falling back to `avg(power) × duration` when no register readings exist.
- **Status**: every session is exactly one of `completed` (saw a Stop), `active`
  (still open, within the recent window), or `incomplete` (still open, older).
  Duration is null until a Stop closes the session.
- **Output**: session facts plus a per-session readings frame (the charging
  curve, one row per `MeterValues`, carrying `charger_id`, `connector_id`,
  timestamp, power/SoC/register), each reading linked by `session_id`. Daily
  analytics then roll sessions up per `(charger_id, connector_id, day)`.

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
  from an optional `charger_id -> site_id` map (`OCPP_SITE_MAP` env, JSON); unset
  means `None`. Ready for multi-site rollups.
- **Tests**: property-based tests (hypothesis) cover the fold's correctness
  properties — duplicate tolerance, energy derivation, status totality, and
  determinism — plus daily-analytics fault reconciliation. Run with `uv run poe test`.
- **DuckLake**: cold archive + gold are DuckLake tables = Parquet data + a local
  DuckDB catalog file, embedded (no service), on the local filesystem (no object
  store). First init is the manual `uv run poe bootstrap`; schema migration is
  manual and out of scope for this prototype.
  - **Inlining**: small tables may be stored *inline in the catalog* rather than
    written as Parquet files, so their directory under `data/lake/data/main/`
    can be empty even though the table has rows (e.g. the tiny
    `gold_analytics_daily`). Don't verify a run by `ls`-ing the data dir — query
    the table (see below) or use the dashboard's DuckLake page.
  - **Inspect the lake** in an interactive DuckDB session (reads the same
    catalog the app writes):

    ```bash
    duckdb
    ```
    ```sql
    INSTALL ducklake; LOAD ducklake;
    SET TimeZone = 'UTC';
    ATTACH 'ducklake:data/lake/catalog.ducklake' AS lake
      (DATA_PATH 'data/lake/data', OVERRIDE_DATA_PATH TRUE, READ_ONLY);

    SELECT * FROM lake.main.gold_analytics_daily;
    -- table list + per-table file counts (file_count 0 == inlined):
    SELECT * FROM ducklake_table_info('lake');
    ```

    `READ_ONLY` lets you inspect safely while the app (Dagster / dashboard) may
    hold the catalog open; drop it if you want to write.

    Add the DuckDB web UI with `duckdb -ui` instead of `duckdb` (opens a browser
    UI; run the same SQL there).
- **Deferred for production** (see design doc): object-store DuckLake data path,
  partitioned archive, managed Postgres, separate Dagster metadata DB, and
  source-level dedup.
