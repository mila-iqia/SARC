FROM python:3.14-slim

# `iguane` is pulled from a git repo via uv → we need git in the build image.
# (No libpq needed: the DB driver is pg8000, pure Python; psycopg is a
# dev-only dependency for fixes/ scripts and is excluded by --no-dev.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install uv (project's dep manager).
RUN pip install --no-cache-dir uv

WORKDIR /app

# Install Python deps first (without the project itself) so this layer is
# cached when only sarc/ changes. README.md is required by hatchling to
# validate the project metadata even when we skip installing the project.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# Now add the app code + demo config and install the project itself.
COPY sarc/ ./sarc/
COPY config-cloud-run.yaml ./config-cloud-run.yaml
RUN uv sync --frozen --no-dev

# NOTE: the app does NOT create the schema at startup — tables, indexes and
# job_series_view must already exist in the target DB (alembic upgrade head
# + init_insert(), see secrets/sql-alembic-migration-from-reset.md).
ENV SARC_MODE=scraping \
    SARC_CONFIG=/app/config-cloud-run.yaml

# Cloud Run injects $PORT (defaults to 8080) and expects the app to listen on it.
CMD ["sh", "-c", "uv run uvicorn sarc.api.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
