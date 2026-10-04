# Technical Debt & Path to Production

Deliberate shortcuts for the demonstrator and what to change before production.

## Known debt

- **Postgres `raw_events` grows unpruned.** The landing zone is append-only with
  no retention job, so it grows without bound. Intended design is short hot
  retention (drop rows once archived); the pruning step is not built.
- **Full-archive read per session run.** `gold_sessions` re-folds the *entire*
  archive on every content-day run. Correct and simple (a content day is a pure
  function of all events), but it does not scale as the archive grows.
- **Archive not physically partitioned.** The ingestion-day grain is logical
  only; DuckLake tables are not physically partitioned or compacted, so file
  counts and scan cost rise over time.
- **Manual historical load.** `load_file_job` is launched by hand; the file-drop
  sensor exists but is not relied on.
- **Manual DuckLake schema migration.** Additive changes need a hand-run
  `ALTER TABLE ADD COLUMN`; there is no migration tooling.

## Path to production

- **Scalable sessionization (two-stage map/reduce).** Replace the full-archive
  read: *map* per ingestion day (tag each session event with its
  `StartTransaction` content day → content-day intermediate), *reduce* per content
  day (fold only that day, upsert `gold_sessions`). Both read a bounded slice; the
  only carried state is a `transaction → start_day` lookup for sessions whose
  Start and Stop land on different ingestion days.
- **Storage.** Swap the local DuckLake data dir for an object store (S3 / GCS /
  ABFS); the catalog is already Postgres-backed for concurrent multi-client builds.
- **Partitioned archive.** Physically partition + compact the DuckLake tables.
- **Postgres.** Managed Postgres with backups and pooling; a separate metadata DB
  for Dagster; add the hot-window retention/pruning job.
- **Deployment / CI.** Proper Dagster deployment (not `dagster dev`); CI running
  lint + tests; externalize connection strings to a secrets manager.
- **Ingestion scale.** More stream replicas; replace file replay with a real
  queue/broker — the library and landing contract stay the same.
