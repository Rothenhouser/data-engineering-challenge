# ev-ocpp-server

Dagster host package. Runs the webserver and daemon that serve the
`ev-ocpp-dagster` code location. Kept separate so it gets its own container
image and deployment lifecycle, matching a production Dagster topology.
