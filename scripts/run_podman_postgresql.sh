# Start a local PostgreSQL container for SARC tests.
# The test conftest connects to postgresql+psycopg://localhost/postgres
# (without credentials) and creates/drops one database per test.

# Abort the script on the first unhandled error.
set -e

# Remove any previous container with the same name.
# `2>/dev/null || true` ignores the error when no such container exists.
podman stop testenv_pg 2>/dev/null || true
podman rm testenv_pg 2>/dev/null || true

# Run PostgreSQL 17 in the background:
#   -dt                                  : detached + allocated TTY.
#   --name testenv_pg                    : fixed name so we can stop/remove it later.
#   -p 5432:5432/tcp                     : expose the standard PostgreSQL port on localhost.
#   -e POSTGRES_HOST_AUTH_METHOD=trust   : allow connections without a password,
#                                          which matches the credential-less URL used by
#                                          tests/conftest.py.
#   -e POSTGRES_USER="$USER"             : create the superuser role named after the host OS
#                                          user, so libpq's default user (the OS user) just works
#                                          without having to put a username in the connection URL.
#   -e POSTGRES_DB=postgres              : keep the default admin database named "postgres"
#                                          (otherwise the image would name it after POSTGRES_USER),
#                                          which is what tests/conftest.py connects to.
podman run -dt --name testenv_pg \
    -p 5432:5432/tcp \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -e POSTGRES_USER="$USER" \
    -e POSTGRES_DB=postgres \
    docker.io/library/postgres:17

# Wait for the PostgreSQL server to accept connections before returning.
# `pg_isready` returns 0 when the server is ready; otherwise we sleep 0.2s and retry.
# Without this loop, the first test can fail because postgres takes ~1s to start.
until podman exec testenv_pg pg_isready -h localhost -U "$USER" >/dev/null 2>&1; do
    sleep 0.2
done
