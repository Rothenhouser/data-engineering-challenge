# Technical Debt & Path to Production

Deliberate shortcuts for the demonstrator and what to change before production.

## Known debt

- **Postgres `raw_events` grows unpruned.** The landing zone is append-only with
  no retention job, so it grows without bound. Intended design is short hot
  retention (drop rows once archived); the pruning step is not built. Would be something like: record rows which were successfully dumped to archive, prune these when older than 1 day (so not needed for live dashboard)
- **Full-archive read per session run.** `gold_sessions` re-analyzes the *entire*
  archive on every content-day run, does not scale. Bit tricky to get right with backfill data.
- **Manual historical load.** `load_file_job` is launched by hand; the file-drop
  sensor exists but is not relied on.
- **Manual DuckLake schema migration.** Additive changes need a hand-run
  `ALTER TABLE ADD COLUMN`; there is no migration tooling.

## Path to production

- **Storage.** Swap the local DuckLake data dir for an object store (S3 / GCS /
  ABFS); the catalog is already Postgres-backed for concurrent multi-client builds.
- **Postgres.** Managed Postgres with backups and pooling; a separate metadata DB
  for Dagster; add the hot-window retention/pruning job.
- **Deployment / CI.** Proper Dagster deployment (not `dagster dev`); CI running
  lint + tests; externalize connection strings to a secrets manager.
- **Ingestion scale.** More stream replicas; replace file replay with a real
  queue/broker. Would depend heavily on real needs.
