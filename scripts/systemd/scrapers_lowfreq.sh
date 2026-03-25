#!/bin/sh
source envvars.sh
export SARC_CONFIG="/home/sarc/SARC/config/sarc-config/fetch_hourly.yaml"
# fetch prometheus
../.local/bin/uv run sarc acquire prometheus -c narval mila rorqual -a 1440
# fetch users
../.local/bin/uv run sarc fetch users
# parse users
../.local/bin/uv run sarc parse users --since 2025-11-28
# health run
SARC_CONFIG="/home/sarc/SARC/config/sarc-config/health_check.yaml" ../.local/bin/uv run sarc health run --all
