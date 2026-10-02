# Single image for the whole uv workspace; each service overrides the command.
FROM python:3.14-slim

# uv for fast, reproducible installs.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer-cached), then the workspace sources.
COPY pyproject.toml uv.lock ./
COPY packages ./packages
RUN uv sync --frozen --all-packages

ENV PATH="/app/.venv/bin:$PATH"
