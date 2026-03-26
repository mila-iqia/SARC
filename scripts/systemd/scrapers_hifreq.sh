#!/bin/sh
source scripts/systemd/envvars.sh
export SARC_CONFIG="/home/sarc/SARC/config/sarc-config/fetch_hourly_sarc01-dev.yaml"
# fetch jobs
../.local/bin/uv run sarc fetch jobs -c narval fir nibi rorqual mila -a 60
../.local/bin/uv run sarc parse jobs
