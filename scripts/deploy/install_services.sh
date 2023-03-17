#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
#rm /etc/systemd/system/sarc_scrapers.service
#rm /etc/systemd/system/sarc_scrapers.timer
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers.service /etc/systemd/system/sarc_scrapers.service
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers.timer /etc/systemd/system/sarc_scrapers.timer
systemctl daemon-reload
systemctl start sarc_scrapers.timer
systemctl enable sarc_scrapers.timer

