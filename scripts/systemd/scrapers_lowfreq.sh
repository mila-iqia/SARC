#!/bin/sh
source envvars.sh
export SARC_CONFIG="/home/sarc/SARC/config/sarc-config/fetch_daily_sarc01-dev.yaml"
# fetch prometheus
../.local/bin/uv run sarc fetch prometheus -c narval mila rorqual -a 1440
../.local/bin/uv run sarc parse prometheus
# fetch users
../.local/bin/uv run sarc fetch users
# parse users
../.local/bin/uv run sarc parse users
# health run
SARC_CONFIG="/home/sarc/SARC/config/sarc-config/health_check_sarc01-dev.yaml" ../.local/bin/uv run sarc health run --all
