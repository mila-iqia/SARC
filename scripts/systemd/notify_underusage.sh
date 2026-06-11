#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
# send weekly underusage notifications (digest + researcher DMs when send_dms: true)
sudo -u sarc SARC_MODE=client SARC_CONFIG=$SCRIPTPATH/../../config/sarc-prod.yaml ../.local/bin/uv run sarc notify underusage --send
