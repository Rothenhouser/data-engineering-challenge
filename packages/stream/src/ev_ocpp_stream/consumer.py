"""The stream consume loop.

Models a permanent live feed: reads the chargers file line-by-line, parses each
frame through the shared library, and writes a per-message committed INSERT to
the append-only ``raw_events`` landing zone (Requirements 1.1, 1.3, 2.6).

- Blank/malformed lines are skipped and counted, never crash ingestion; the skip
  count is returned as an observable run output (Requirements 10.1-10.3, 10.5).
- On a Postgres write failure the error is surfaced (logged) and the consume loop
  retries rather than losing the stream (Requirements 12.1, 12.2).
- On restart the consumer resumes reading the feed with no manual intervention;
  re-ingested frames are appended again by design and collapsed later by the fold
  (Requirements 11.2, 11.3).
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass

import psycopg
from ev_ocpp_analysis import (
    init_schema,
    init_sim_control,
    iter_file,
    parse_line,
    read_sim_control,
    to_raw_event_row,
    write_raw_event,
)

log = logging.getLogger("ev_ocpp_stream")


@dataclass
class StreamStats:
    ingested: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.ingested + self.skipped


def run_stream_consumer(
    source_path: str,
    conn_uri: str,
    delay: float = 0.0,
    retry_wait: float = 2.0,
) -> StreamStats:
    """Consume ``source_path`` once into Postgres; return per-run stats.

    One committed INSERT per frame keeps the landing zone crash-safe. A write
    error reconnects and retries the same frame instead of dropping it.
    """
    stats = StreamStats()
    conn = _reconnect(None, conn_uri)
    init_schema(conn)
    init_sim_control(conn)
    # Live-adjustable inter-frame delay; refreshed from sim_control periodically.
    cur_delay = _refresh_delay(conn, delay)
    log.info("stream consumer started: source=%s delay=%.3fs", source_path, cur_delay)
    try:
        for line in iter_file(source_path):
            event = parse_line(line)
            if event is None:
                if line.strip():  # count only non-blank malformed lines
                    stats.skipped += 1
                continue
            row = to_raw_event_row(event)
            while True:
                try:
                    write_raw_event(conn, row)
                    stats.ingested += 1
                    break
                except psycopg.Error as exc:
                    log.error("raw_events write failed, retrying: %s", exc)
                    time.sleep(retry_wait)
                    conn = _reconnect(conn, conn_uri)
            # Every 100 frames: heartbeat + re-read the live-adjustable delay.
            if stats.ingested % 100 == 0:
                cur_delay = _refresh_delay(conn, delay)
                log.info(
                    "ingested=%d skipped=%d delay=%.3fs",
                    stats.ingested, stats.skipped, cur_delay,
                )
            if cur_delay:
                time.sleep(cur_delay)
    finally:
        conn.close()
    log.info("stream run done: ingested=%d skipped=%d", stats.ingested, stats.skipped)
    return stats


def _refresh_delay(conn: psycopg.Connection, fallback: float) -> float:
    """Read the current stream delay from sim_control, falling back on error."""
    try:
        return float(read_sim_control(conn).get("stream_delay", fallback))
    except psycopg.Error:
        return fallback


def _reconnect(conn: psycopg.Connection | None, conn_uri: str) -> psycopg.Connection:
    """(Re)connect to Postgres, retrying until it succeeds.

    A ``connect_timeout`` is set so a stalled connect (e.g. Postgres restarting)
    fails fast and the loop can retry+log, rather than blocking indefinitely.
    """
    if conn is not None:
        try:
            conn.close()
        except psycopg.Error:
            pass
    attempt = 0
    while True:
        try:
            return psycopg.connect(conn_uri, connect_timeout=5)
        except psycopg.Error as exc:
            attempt += 1
            log.error("connect failed (attempt %d), retrying: %s", attempt, exc)
            time.sleep(2.0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="OCPP stream consumer")
    default_source = os.environ.get("OCPP_SOURCE", "data/ocpp-data-many-chargers.txt")
    default_delay = float(os.environ.get("OCPP_STREAM_DELAY", "0"))
    parser.add_argument("--source", default=default_source)
    parser.add_argument("--conn-uri", default=os.environ.get("OCPP_PG_URI", ""))
    parser.add_argument("--delay", type=float, default=default_delay)
    parser.add_argument("--loop", action="store_true", help="restart the feed after reaching EOF")
    args = parser.parse_args()
    if not args.conn_uri:
        parser.error("--conn-uri or OCPP_PG_URI is required")

    # Crash-safe resume: a wrapping loop re-reads the feed with no manual step.
    while True:
        stats = run_stream_consumer(args.source, args.conn_uri, delay=args.delay)
        print(f"skip_count={stats.skipped} ingested={stats.ingested} total={stats.total}")
        if not args.loop:
            break
        time.sleep(1.0)


if __name__ == "__main__":
    main()
