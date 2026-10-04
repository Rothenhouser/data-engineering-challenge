"""Historical / batch loader: a directory-scan sensor + loading job.

Models "a charger onboarded with backfilled data". The sensor scans the watched
directory; a file is new when its filename is not in the Scan_Cursor (the sensor
cursor). The job parses the file through the shared library and
bulk-INSERTs raw events into the same append-only ``raw_events`` table the stream
writes to.
"""

import glob
import json
import os

import psycopg
from dagster import (
    AssetKey,
    AssetMaterialization,
    Config,
    MetadataValue,
    Output,
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    job,
    op,
    sensor,
)
from ev_ocpp_analysis import init_schema, iter_file, parse_raw_row, write_raw_events

from .config import DATA_DIR, PG_URI

# Only these inputs are treated as historical drops to ingest.
_INPUT_GLOB = "ocpp-data-*.txt"


class LoadFileConfig(Config):
    path: str


@op
def load_file_op(context, config: LoadFileConfig):
    """Parse one historical file and bulk-append its frames to raw_events.

    Reports an ``AssetMaterialization`` for the ``raw_events`` landing-zone asset
    (the Postgres table both ingestion paths write to) so a historical load
    shows up as a materialization event in Dagster's asset catalog, with the
    source file and row counts as metadata.
    """
    rows, skipped = [], 0
    for line in iter_file(config.path):
        row = parse_raw_row(line)
        if row is None:
            if line.strip():
                skipped += 1
            continue
        rows.append(row)
    with psycopg.connect(PG_URI) as conn:  # fails the run if unreachable (12.3)
        init_schema(conn)
        written = write_raw_events(conn, rows)
    context.log.info("loaded %s: ingested=%d skipped=%d", config.path, written, skipped)
    yield AssetMaterialization(
        asset_key=AssetKey("raw_events"),
        description="Historical file drop appended to the raw_events landing zone.",
        metadata={
            "source_file": MetadataValue.path(config.path),
            "rows_ingested": MetadataValue.int(written),
            "rows_skipped": MetadataValue.int(skipped),
        },
    )
    yield Output(None)


@job
def load_file_job() -> None:
    load_file_op()


@sensor(job=load_file_job, minimum_interval_seconds=30)
def historical_file_sensor(context: SensorEvaluationContext) -> SensorResult:
    """Request one load run per newly-seen input filename."""
    seen = set(json.loads(context.cursor) if context.cursor else [])
    requests = []
    for path in sorted(glob.glob(os.path.join(DATA_DIR, _INPUT_GLOB))):
        name = os.path.basename(path)
        if name in seen:
            continue
        seen.add(name)
        requests.append(
            RunRequest(
                run_key=name,
                run_config=RunConfig(ops={"load_file_op": LoadFileConfig(path=path)}),
            )
        )
    return SensorResult(run_requests=requests, cursor=json.dumps(sorted(seen)))
