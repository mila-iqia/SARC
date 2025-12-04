#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
# fetch jobs
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc acquire jobs -c narval fir mila -a 1440
# fetch prometheus
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc acquire prometheus -c narval mila -a 1440
# fetch users
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc fetch users
# parse users
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc parse users --since 2025-11-28
