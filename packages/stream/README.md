# ev-ocpp-stream

Standalone always-on stream consumer. Reads `ocpp-data-many-chargers.txt`
line-by-line as a simulated live feed, parses each frame through
`ev_ocpp_analysis`, and writes raw events to the Postgres landing zone with
per-message committed parameterized INSERTs.

Run:

```
ev-ocpp-stream --source data/ocpp-data-many-chargers.txt --conn-uri postgresql://...
```

Environment overrides: `OCPP_SOURCE`, `OCPP_PG_URI`, `OCPP_STREAM_DELAY`.
