#!/bin/bash

PORT=${MONGO_PORT:-"8123"}
ADDRESS=${MONGO_ADDRESS:-"localhost"}
ADMIN=${MONGO_ADMIN:-"god"}
PASSWORD=${MONGO_PASS:-"god123"}
DB_PATH=${MONGO_PATH:-"/tmp/db"}


function start {
    mkdir -p $DB_PATH
    mongod --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid 
}

function stop {
    mongod --dbpath $DB_PATH/ --shutdown
}

function restore {
    path=$1
    db=$2

    collections=("allocations" "diskusage" "jobs" "users")

    for collection in collections; do
        mongorestore --gzip --port=$PORT -d $db $path/$db/$collection.bson.gz
    done
}