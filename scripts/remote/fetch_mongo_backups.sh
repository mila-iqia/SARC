#!/bin/sh

# backup mongodb nightly dumps from server $1


if [ "$#" -lt 2 ]
then
    echo "Usage : `basename $0` <server> <destination> [remote path (default: /home/sarc/mongo_backups)]"
    echo Example: `basename $0` sarcvm .
    exit 1
fi

if [ -z "$3" ]
  then
    echo "No remote path supplied, using default path '/home/sarc.'"
    SOURCE_PATH='/home/sarc/mongo_backups'
  else
    SOURCE_PATH=$3
fi

# first step: rsync /home/sarc/mongo_backups with ˜/mongo_backups on remote machine
echo 'Copying remotely to user home folder...'
ssh $1 "sudo rsync -av $SOURCE_PATH ."

# step two: rsync ˜/mongo_backups on remote with %2/mongo_backups
echo Copying locally to folder $2
rsync -av sarc:mongo_backups $2
