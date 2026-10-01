# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # OCPP Charging Data — Exploratory Analysis
#
# **Goal:** understand the raw OCPP message stream from EV Coorp's chargers well
# enough to design the ingestion + session-reconstruction pipeline.
#
# Two source files:
# - `ocpp-data-many-chargers.txt` — many chargers, ~1 day (breadth)
# - `ocpp-data-many-days.txt` — one charger, several days (depth)
#
# Uses the reusable parser in `ev_ocpp_analysis.parsing` so exploration and the
# pipeline share one source of truth. Convert to a notebook with:
# `uv run jupytext --to notebook 20261001-analysis.py`

# %%
import sys
from pathlib import Path

import plotly.express as px
import polars as pl

from ev_ocpp_analysis import parsing

# polars tables use box-drawing glyphs; force UTF-8 so the plain script also
# runs on a non-UTF-8 Windows console (no-op in a notebook kernel).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Resolve the repo-level data dir regardless of where the kernel starts
DATA = Path.cwd()
while not (DATA / "data").exists() and DATA != DATA.parent:
    DATA = DATA.parent
DATA = DATA / "data"
FILES = {
    "many_chargers": DATA / "ocpp-data-many-chargers.txt",
    "many_days": DATA / "ocpp-data-many-days.txt",
}
FILES

# %% [markdown]
# ## 1. Parse both files
#
# The parser turns each line `station : [type, id, action, payload]` into a
# structured record, lifts common fields, and pivots `MeterValues` measurands
# into a flat dict. It also counts skipped lines — a first data-quality signal.

# %%
parsed = {}
for name, path in FILES.items():
    events, stats = parsing.parse_lines(parsing.iter_file(str(path)))
    parsed[name] = events
    print(f"{name}: {stats.total} lines, {stats.parsed} parsed, {stats.skipped} skipped")


# %% [markdown]
# Flatten the Call events into polars DataFrames. (CallResults carry only an id
# + ack; the analytically interesting content is in the Calls.)

# %%
# Explicit schema: columns are null for most message types, so inference from
# the first rows is unreliable — pin the types up front.
_SCHEMA = {
    "station_id": pl.Utf8,
    "action": pl.Utf8,
    "connector_id": pl.Int64,
    "transaction_id": pl.Int64,
    "timestamp": pl.Utf8,
    "status": pl.Utf8,
    "error_code": pl.Utf8,
    "reason": pl.Utf8,
    "power_kw": pl.Float64,
    "energy_register_kwh": pl.Float64,
    "soc_pct": pl.Float64,
    "meter_context": pl.Utf8,
}


def to_frame(events) -> pl.DataFrame:
    rows = [
        {
            "station_id": e.station_id,
            "action": e.action,
            "connector_id": e.connector_id,
            "transaction_id": e.transaction_id,
            "timestamp": e.timestamp,
            "status": e.payload.get("status"),
            "error_code": e.payload.get("errorCode"),
            "reason": e.payload.get("reason"),
            "power_kw": e.measurands.get("Power.Active.Import"),
            "energy_register_kwh": e.measurands.get("Energy.Active.Import.Register"),
            "soc_pct": e.measurands.get("SoC"),
            "meter_context": e.meter_context,
        }
        for e in events
        if e.action is not None  # drop CallResults
    ]
    return pl.DataFrame(rows, schema=_SCHEMA).with_columns(
        # ISO8601 timestamps carry a Z/offset; parse as tz-aware UTC
        pl.col("timestamp").str.to_datetime(
            time_unit="us", time_zone="UTC", strict=False
        )
    )


frames = {name: to_frame(events) for name, events in parsed.items()}
frames["many_chargers"].head()

# %% [markdown]
# ## 2. What message types are present?
#
# Tells us which OCPP actions we must handle and which drive session logic.

# %%
for name, df in frames.items():
    print(f"\n=== {name} ===")
    print(df["action"].value_counts(sort=True))

# %% [markdown]
# **Observations**
#
# - `Heartbeat` and `MeterValues` dominate volume — expected for periodic telemetry.
# - Full transaction lifecycle present: `Authorize` -> `StartTransaction` ->
#   `MeterValues` -> `StopTransaction`. These are our primary session boundaries.
# - `StatusNotification` tracks connector state; fallback signal for sessions
#   missing a clean start/stop.
# - Control-plane messages (`SetChargingProfile`, `RemoteStopTransaction`,
#   `GetConfiguration`, `Reset`) exist but are out of scope for Option A.

# %% [markdown]
# ## 3. Fleet shape: stations, connectors, time span

# %%
for name, df in frames.items():
    span = df["timestamp"].max() - df["timestamp"].min()
    print(f"\n=== {name} ===")
    print("stations:", sorted(df["station_id"].unique().to_list()))
    print("connectors:", sorted(df["connector_id"].drop_nulls().unique().to_list()))
    print("time span:", df["timestamp"].min(), "->", df["timestamp"].max())
    print("span (days):", round(span.total_seconds() / 86400, 2))

# %% [markdown]
# Station ids are not contiguous (no `charger4`/`charger9`) — don't assume dense
# ids. `transaction_id` is small and resets per charger, so a **global session
# key must be `station + connector + startTime`**, not transactionId alone.

# %% [markdown]
# ## 4. Transaction balance (start vs. stop)
#
# starts != stops means open sessions at the window edge or orphaned stops — the
# incomplete scenarios the brief asks us to handle.

# %%
for name, df in frames.items():
    starts = df.filter(pl.col("action") == "StartTransaction").height
    stops = df.filter(pl.col("action") == "StopTransaction").height
    print(f"{name}: {starts} starts, {stops} stops, delta={starts - stops}")

# %% [markdown]
# ## 5. Connector state machine & faults
#
# `StatusNotification` states. `Faulted` with a real `errorCode` is an
# operational-health signal worth surfacing on the dashboard.

# %%
for name, df in frames.items():
    sn = df.filter(pl.col("action") == "StatusNotification")
    print(f"\n=== {name} ===")
    print(sn["status"].value_counts(sort=True))
    faults = sn.filter(pl.col("status") == "Faulted")
    if faults.height:
        print("fault error codes:", faults["error_code"].value_counts(sort=True).to_dicts())

# %% [markdown]
# ## 6. Stop reasons
#
# Why sessions end — useful as a session attribute and a quality dimension.

# %%
for name, df in frames.items():
    st = df.filter(pl.col("action") == "StopTransaction")
    print(f"{name}:", st["reason"].value_counts(sort=True).to_dicts())

# %% [markdown]
# ## 7. MeterValues: the measurands we can analyze
#
# `Power.Active.Import` is integrated for energy (avg power x duration).
# `Energy.Active.Import.Register` is a cumulative meter for a future cross-check.

# %%
mv = frames["many_chargers"].filter(pl.col("action") == "MeterValues")
mv.select(["power_kw", "energy_register_kwh", "soc_pct"]).describe()

# %%
# Sampling contexts: Sample.Periodic during charging, Transaction.End at close
mv["meter_context"].value_counts(sort=True)

# %% [markdown]
# ## 8. Preview: energy for one session
#
# A single transaction's power curve to sanity-check the avg-power x duration
# energy estimate the pipeline will compute per session.

# %%
days = frames["many_days"]
mvd = (
    days.filter((pl.col("action") == "MeterValues") & pl.col("power_kw").is_not_null())
    .sort("timestamp")
)
window = mvd.filter(pl.col("connector_id") == 1).head(60)
fig = None
if window.height:
    dur_h = (window["timestamp"].max() - window["timestamp"].min()).total_seconds() / 3600
    avg_kw = window["power_kw"].mean()
    print(f"samples={window.height} duration={dur_h:.3f}h avg_power={avg_kw:.2f}kW")
    print(f"estimated energy = {avg_kw * dur_h:.2f} kWh")
    # Build the figure but don't render here; the notebook displays it as the
    # last expression, and running as a plain script stays non-blocking.
    fig = px.line(
        window.select(["timestamp", "power_kw"]),
        x="timestamp",
        y="power_kw",
        title="Power curve (sample window)",
        labels={"power_kw": "kW"},
    )
fig

# %% [markdown]
# ## Takeaways for the pipeline
#
# 1. **Format is clean** — zero unparseable lines across ~210k messages.
# 2. **Sessions are event-driven** — `StartTransaction`/`StopTransaction` primary
#    boundaries; `StatusNotification` + MeterValues gaps as fallbacks.
# 3. **Session key** = station + connector + start time (transactionId resets).
# 4. **Incomplete sessions are real** (starts != stops) — model an explicit
#    status: completed / active / incomplete.
# 5. **Energy** = mean `Power.Active.Import` x duration; register reading kept for
#    a future cross-check.
# 6. **Health signals** — `Faulted` states + stop reasons are cheap, high-value
#    dashboard additions.
