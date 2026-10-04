# EV Coorp OCPP Analytics

Ingestion + analytics prototype for EV charging infrastructure emitting OCPP 1.6
messages.

- Challenge brief: [`CHALLENGE.md`](CHALLENGE.md)
- Architecture, design decisions & how it answers the brief: [`docs/ocpp-ingestion-architecture.md`](docs/ocpp-ingestion-architecture.md)
- How to read the raw data: [`docs/ocpp-message-format.md`](docs/ocpp-message-format.md)

## Architecture

Both sources — a live stream and historical file drops — are parsed by one shared
library (`ev_ocpp_analysis`) and land in one append-only Postgres table, from
where the data tiers hot → cold → gold. The full component breakdown, data
models, storage rationale, and production evolution path are in the
[architecture doc](docs/ocpp-ingestion-architecture.md).

```
many-chargers.txt ─(stream consumer)─┐
                                      ├─> Postgres raw_events ─> DuckLake archive ─> DuckLake gold (sessions, readings, daily analytics)
many-days.txt ─────(Dagster loader)──┘       (hot, short retention)   (cold, immutable)         (Streamlit dashboard)
```

## Sessionization

One shared Polars fold (`reconstruct_sessions`) rebuilds sessions from raw events —
an ordered, stateful fold (not a `GROUP BY`) because the session key only exists
once the opening `StartTransaction` arrives — so a session reconstructed live
matches the same session from the archive. Sessions are keyed by
`charger_id + connector_id + start_time`, carry an explicit
`completed` / `active` / `incomplete` status, and derive energy from the meter
register delta (falling back to `avg(power) × duration`). The fold's ordering,
OCPP Call/CallResult correlation, parallel-connector handling, and readings
output are detailed in the [architecture doc](docs/ocpp-ingestion-architecture.md).

## Partitioning

The pipeline runs on two independent daily grains so each run reprocesses a
bounded slice: the archive by *ingestion day* (`raw_events_archive`, materialized
every 5 min) and gold by *content day* (`gold_sessions` / `gold_analytics_daily`),
with an asset sensor fanning a newly-archived ingestion day out to the content
days it touched. The grains, fan-out, and the scalable two-stage design are in
the [architecture doc](docs/ocpp-ingestion-architecture.md).

## Run it

**Prerequisites:**

- Docker + docker-compose (invoked here as `wsl sudo docker` on Windows/WSL).
- [`uv`](https://docs.astral.sh/uv/) for the `poe` tasks and local dashboard dev.
- Outbound network on first boot (the DuckLake DuckDB extension is fetched via
  `INSTALL ducklake`, not baked into the image).
- The two sample files (`ocpp-data-many-chargers.txt`, `ocpp-data-many-days.txt`)
  present under `data/`.

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
