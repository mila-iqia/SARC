#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../
# these hard-coded paths are so disgusting...
sudo -u sarc ../.local/bin/poetry run sarc acquire jobs -c narval