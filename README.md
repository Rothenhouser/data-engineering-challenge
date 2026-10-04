# EV Coorp OCPP Analytics

Ingestion + analytics prototype for EV charging infrastructure emitting OCPP 1.6
messages.

- Challenge brief: [`docs/00-CHALLENGE.md`](docs/00-CHALLENGE.md)
- Full architecture, design decisions & how it answers the brief:
  [`docs/01-architecture.md`](docs/01-architecture.md)
- Known shortcuts & path to production:
  [`docs/02-technical-debt.md`](docs/02-technical-debt.md)

## Architecture

For the sake of this demonstrator, `data/ocpp-data-many-chargers.txt` is treated
as a 'live' stream, while `data/ocpp-data-many-days.txt` is treated as batch
backfill/historical data.

Both sources land in the same **Postgres** table, from which they are dumped
into a **DuckLake** (i.e. parquet) archive. The raw events are then regularly
processed into charging sessions, and aggregated into summary statistics. A
**Streamlit** dashboard shows 'live' charger status from Postgres as well as the
analytics results. Processing is orchestrated with **Dagster**.

The entire project is set up as a **uv workspace**, where functionality is
encapsulated into independent Python packages, and deployed locally as one
**Docker compose** stack.

## Run it

**Prerequisites:**

- Docker + docker-compose (invoked here as `wsl sudo docker` on Windows/WSL).
- [`uv`](https://docs.astral.sh/uv/) for Python dependencies.

```bash
uv sync --all-packages --all-groups
wsl sudo docker compose -f deploy/docker-compose.yml up --build -d
uv run poe bootstrap   # DB setup — only once
```

Services should then be running:

- Dashboard: http://localhost:8501
- Dagster: http://localhost:3000 (check the 'Lineage' page)
- Postgres on `localhost:5432`

**Live** data ingestion happens via a stream service that consumes
`ocpp-data-many-chargers.txt` line by line with a small delay and writes to
Postgres.

**Historical** data backfill happens when you run the `load_file_job` manually
(Dagster UI → Jobs → `load_file_job` → Launchpad); the op config path defaults
to `/app/data/ocpp-data-many-days.txt`.

Ingestion from Postgres into DuckLake runs every 5 minutes, so you may have to
wait or trigger the `archive_job` manually in Dagster. Parsing into sessions and
analytics then happens automatically (event-driven) — the dashboard's analytics
page shows results after the first successful run.
