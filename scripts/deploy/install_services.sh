#!/bin/sh
SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers_hifreq.service /etc/systemd/system/sarc_scrapers_hifreq.service
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers_hifreq.timer /etc/systemd/system/sarc_scrapers_hifreq.timer
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers_lowfreq.service /etc/systemd/system/sarc_scrapers_lowfreq.service
ln -s -f $SCRIPTPATH/../systemd/sarc_scrapers_lowfreq.timer /etc/systemd/system/sarc_scrapers_lowfreq.timer
ln -s -f $SCRIPTPATH/../systemd/sarc_containers.service /etc/systemd/system/sarc_containers.service
ln -s -f $SCRIPTPATH/../systemd/sarc_backup.service /etc/systemd/system/sarc_backup.service
ln -s -f $SCRIPTPATH/../systemd/sarc_backup.timer /etc/systemd/system/sarc_backup.timer
systemctl daemon-reload
systemctl start sarc_scrapers_hifreq.timer
systemctl enable sarc_scrapers_hifreq.timer
systemctl start sarc_scrapers_lowfreq.timer
systemctl enable sarc_scrapers_lowfreq.timer
systemctl start sarc_containers.service
systemctl enable sarc_containers.service
systemctl start sarc_backup.timer
systemctl enable sarc_backup.timer
