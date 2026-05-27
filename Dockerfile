FROM python:3.14-slim

# `iguane` is pulled from a git repo via uv → we need git in the build image.
# `libpq5` is the PostgreSQL client runtime library; psycopg in "python"
# mode (pure-Python wrapper) loads libpq at import time and crashes
# without it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git libpq5 \
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

# Scraping mode so db_upgrade() runs at startup: it creates tables, indexes
# (idx_jobstats_job_id, idx_slurm_jobs_submit_time), and the job_series_view.
# With clusters: {} in the config, insert_clusters/insert_rgu are no-ops, so
# this is safe even though the service only reads.
ENV SARC_MODE=scraping \
    SARC_CONFIG=/app/config-cloud-run.yaml

# Cloud Run injects $PORT (defaults to 8080) and expects the app to listen on it.
CMD ["sh", "-c", "uv run uvicorn sarc.api.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
