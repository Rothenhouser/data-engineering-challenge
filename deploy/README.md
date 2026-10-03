# Deployment

Portable local topology for the OCPP ingestion architecture.

```
docker compose -f deploy/docker-compose.yml up --build
```

Services:

- **postgres** — hot raw landing zone (`raw_events`).
- **dagster** — webserver + daemon (`dagster dev`) serving the `ev_ocpp_dagster`
  code location: historical loader sensor, dump-to-DuckLake job, session job.
  UI at http://localhost:3000.
- **stream** — standalone `ev_ocpp_stream` consumer. Scale horizontally:
  `docker compose -f deploy/docker-compose.yml up --scale stream=3`.
- **streamlit** — dashboard at http://localhost:8501.

The host `data/` directory is bind-mounted at `/app/data` in every app
container: it holds the input `.txt` files and receives the cold archive and
gold, which live in an embedded DuckLake under `data/lake/` (a DuckDB catalog
file `data/lake/catalog.ducklake` plus the table Parquet data in
`data/lake/data/`). There is no separate lake service — DuckLake loads in-process
as a DuckDB extension.

First-time setup is manual: run `uv run poe bootstrap` once to create the
DuckLake catalog + data dir before running the Dagster pipeline (the archive and
gold tables are then created lazily by the first pipeline run).

All services share one image (`deploy/Dockerfile`) built from the uv workspace;
each service only overrides the command.
