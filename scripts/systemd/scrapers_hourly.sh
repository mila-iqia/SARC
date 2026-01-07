#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
# fetch jobs
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc acquire jobs -c narval fir mila -a 60
