#!/bin/bash

PORT=${MONGO_PORT:-"8123"}
ADDRESS=${MONGO_ADDRESS:-"localhost"}
ADMIN=${MONGO_ADMIN:-"god"}
PASSWORD=${MONGO_PASS:-"god123"}
DB_PATH=${MONGO_PATH:-"/tmp/db"}


function start {
    #
    #   starts a new mongodb instance running locally at a specified location
    #
    #   Usage:
    #       
    #       start
    #
    mkdir -p $DB_PATH
    mongod --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid 
}

function stop {
    #
    #   stop the mongodb instance running at the current DB path
    #
    #   Usage:
    #   
    #       stop
    #
    mongod --dbpath $DB_PATH/ --shutdown
}

function restore {
    #
    #   restore mongodb collections from a backup for a given database
    #
    #   Usage:
    #
    #       restore /home/sarc/mongo_backups/sarc_mongo.sarc.2023-07-12 sarc
    #
    path=$1
    db=$2

    collections=("allocations" "diskusage" "jobs" "users")

    for collection in collections; do
        mongorestore --gzip --port=$PORT -d $db $path/$db/$collection.bson.gz --gzip
    done
}