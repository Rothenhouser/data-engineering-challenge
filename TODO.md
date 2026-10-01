# EV Coorp OCPP Prototype — Scope Roadmap

We build in scopes. **Option A is the current target.** B and C are recorded
here so the extension path is explicit and nothing is lost.

## Decisions locked in

- **Stack:** Dagster (orchestration), DuckDB (query), Parquet (storage),
  Streamlit (dashboard). uv workspace, Python 3.14, docker-compose for portability.
- **Energy metric:** power-curve integral approximation — average
  `Power.Active.Import` × session duration (README-sanctioned).
- **Session boundaries:** explicit `StartTransaction` / `StopTransaction`;
  open/incomplete sessions are flagged.
- **Session key:** `station + connectorId + startTime` (transactionId resets per
  charger, so it is not globally unique).

## Option A — Core Pipeline (current)

Minimum that fully answers the challenge.

- Dagster asset graph: raw files → parsed `raw_events` (Parquet) →
  `charging_sessions` (Parquet).
- DuckDB query layer over the Parquet.
- Single Streamlit page: headline KPIs, filterable session table, energy chart.
- Energy via avg power × duration.
- docker-compose running the Dagster host and Streamlit against a shared volume.
- Architecture README.

## Option B — Analytics Platform (next)

Adds the data-engineering rigor reviewers look for.

- Medallion layering: bronze (raw parsed) / silver (typed, deduped, pivoted) /
  gold (session facts + daily & per-charger aggregates).
- Robust session reconstruction: StatusNotification state machine and
  MeterValues gaps as fallbacks; explicit handling of orphan stops and faults.
- Dual energy methods: avg-power×duration vs. meter-register delta, shown
  side by side as a trust/quality signal.
- Dagster asset checks: schema, null thresholds, energy sanity, monotonic meters.
- Day-partitioned assets for incremental processing (demoed on the many-days file).
- Multi-page dashboard: fleet overview, charger drill-down, session explorer,
  data-quality panel.

## Option C — Platform + Streaming / Observability (stretch)

Production-leaning extras.

- Simulated near-real-time ingestion (Dagster sensor / replayer feeding the
  pipeline incrementally) for the real-time monitoring need.
- Alerting on faults/anomalies; freshness checks.
- Multi-site dimensional model (seeded station dimension) for future sites.
- Fuller test suite and CI.

## Deferred production concerns (document, not built in A)

- Swap local Parquet for object store (S3/GCS/ABFS) via a storage abstraction.
- Partitioning + file compaction strategy for growing volumes.
- Secrets/config management; separate Dagster storage (Postgres) in prod.
- Schema evolution handling for new OCPP measurands/actions.
