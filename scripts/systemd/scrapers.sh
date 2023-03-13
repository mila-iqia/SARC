#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
sudo -u sarc PATH=/home/sarc/.local/bin:$PATH poetry run sarc acquire jobs -c narval

