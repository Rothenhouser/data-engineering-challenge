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
