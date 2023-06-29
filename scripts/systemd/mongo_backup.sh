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
podman exec $CONTAINER rm -rf /temp_dump/
podman exec $CONTAINER mongodump -d $DB -o /temp_dump --gzip

FILENAME="$DEST_PATH$CONTAINER.$DB.$(date +"%Y-%m-%d")"
echo "retrieve the db dump files to $DEST_PATH ..."
podman cp $CONTAINER:temp_dump $DEST_PATH
echo "cleaning $CONTAINER:/temp_dump/ ..."
podman exec $CONTAINER rm -rf /temp_dump/
cd $DEST_PATH
echo "renaming $DEST_PATHtemp_dump to $FILENAME ..."
mv temp_dump $FILENAME
echo "cleaning old backups ..."
find -mtime +28 -exec rm -rf {} \;
