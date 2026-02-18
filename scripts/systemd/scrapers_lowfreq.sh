#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
# fetch prometheus
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc acquire prometheus -c narval mila rorqual -a 1440
# fetch users
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc fetch users
# parse users
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc parse users --since 2025-11-28
# health run
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc health run --all
    