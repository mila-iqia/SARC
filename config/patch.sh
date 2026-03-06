export SARC_DB_CONNECTION=""
export SARC_DB_NAME=""
export SARC_CACHE_PATH="/tmp"
uv run serieux patch --model sarc.config:Config $1
