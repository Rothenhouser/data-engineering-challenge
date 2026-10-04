# Single image for the whole uv workspace; each service overrides the command.
FROM python:3.14-slim

# uv for fast, reproducible installs.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# --- Dependency layer (cached across source edits) -------------------------
# Copy only the lockfile + every workspace member's manifest/README (hatchling
# reads `readme = "README.md"`), WITHOUT the package sources, then install just
# the third-party dependencies. This heavy layer (Polars/DuckDB/Dagster/etc.)
# is keyed on the lockfiles + manifests, so editing code under packages/*/src
# does NOT invalidate it — only a real dependency change does. uv's download
# cache is mounted so re-runs never re-download wheels.
COPY pyproject.toml uv.lock ./
COPY packages/analysis/pyproject.toml packages/analysis/README.md ./packages/analysis/
COPY packages/dagster-code/pyproject.toml packages/dagster-code/README.md ./packages/dagster-code/
COPY packages/dagster-server/pyproject.toml packages/dagster-server/README.md ./packages/dagster-server/
COPY packages/dashboard/pyproject.toml packages/dashboard/README.md ./packages/dashboard/
COPY packages/stream/pyproject.toml packages/stream/README.md ./packages/stream/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-packages --no-install-workspace

# --- Source layer (cheap; re-exported on every code edit) ------------------
# Now bring in the actual sources and link the local workspace packages into
# the venv. This step touches only the small first-party wheels, so an edit to
# packages/*/src re-exports a few KB instead of the whole dependency venv.
COPY packages ./packages
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-packages

ENV PATH="/app/.venv/bin:$PATH"
