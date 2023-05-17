#!/bin/sh

# This scripts makes a backup of the mongo database

# usage :
# ./mongo_backup.sh <container> <database> <dest_path>

if [ -z "$1" ]
  then
    echo "No container name supplied, using default name 'sarc_mongo'"
    CONTAINER='sarc_mongo'
  else
    CONTAINER=$1
fi
if [ -z "$2" ]
  then
    echo "No database name supplied, using default name 'sarc'"
    DB='sarc'
  else
    DB=$2
fi
if [ -z "$3" ]
  then
    echo "No destination path supplied, using default path './'"
    DEST_PATH='./'
  else
    DEST_PATH=$3
fi
echo "dump database $CONTAINER:$DB ..."
podman exec $CONTAINER rm -rf temp_dump
podman exec $CONTAINER mongodump -d $DB -o temp_dump --gzip

FILENAME="$DEST_PATH$CONTAINER.$DB.$(date +"%Y-%m-%d")"
echo "retrieve the db dump files to $FILENAME..."
podman cp $CONTAINER:temp_dump $FILENAME
podman exec $CONTAINER rm -rf temp_dump
cd $FILENAME
find -mtime +28 -exec rm {} \;
