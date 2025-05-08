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
if [ -z "$4" ]
  then
    SCRIPT=$(readlink -f "$0")
    SCRIPTPATH=$(dirname "$SCRIPT")
    SECRET_PATH=$SCRIPTPATH/../../../SARC_secrets/secrets/mongo_writeuser_password.txt
    echo "No secret file path supplied, using default path $SECRET_PATH"
  else
    SECRET_PATH=$4
fi

# retrieve the password from the secret file
PASSWORD=$(cat $SECRET_PATH)
echo "password is $PASSWORD"

echo "dump database $CONTAINER:$DB ..."
podman exec $CONTAINER rm -rf /temp_dump/
podman exec $CONTAINER mongodump -d $DB --username=writeuser --password=$PASSWORD -o /temp_dump --gzip

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
TGZ_FILE="$DEST_PATH./daily_backup.tar.gz"
echo "Creating daily backup file $TGZ_FILE ..."
tar czf $TGZ_FILE $FILENAME
