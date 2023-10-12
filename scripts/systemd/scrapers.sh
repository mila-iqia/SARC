#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
sudo -u sarc SARC_MODE=scraping SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.json ../.local/bin/poetry run sarc acquire jobs -c narval cedar beluga graham mila -d auto
