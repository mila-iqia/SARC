#!/bin/sh
source scripts/systemd/envvars.sh
export SARC_CONFIG="/home/sarc/SARC/config/sarc-config/fetch_jobs_manual_sarc01-dev.yaml"
# fetch jobs
../.local/bin/uv run sarc fetch jobs -c tamia -a 60 --max_intervals 24
../.local/bin/uv run sarc parse jobs
