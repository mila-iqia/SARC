#!/bin/bash

# Database config
# ===============
PORT=${MONGO_PORT:-"8123"}
ADDRESS=${MONGO_ADDRESS:-"localhost"}
DB_PATH=${MONGO_PATH:-"/tmp/db"}
DBNAME=${MONGO_DB:-"sarc"}

# Users
# =====
ADMIN=${MONGO_ADMIN:-"admin"}
PASSWORD=${MONGO_PASS:-"pass0"}

WRITEUSER_NAME=${WRITEUSER_NAME:-"sarc"}
WRITEUSER_PWD=${WRITEUSER_PWD:-"pass1"}

READUSER_NAME=${READUSER_NAME:-"readuser"}
READUSER_PWD=${READUSER_PWD:-"pass2"}

# Options
# =======
MONGOD_CMD=${MONGOD_CMD:-"mongod"}
USE_PODMAN=${USE_PODMAN:-"0"}
LAUNCH_MONGO=${LAUNCH_MONGO:-"0"}

echo "====================================="
echo "PORT: $PORT" > dbconfig.txt
echo "ADDRESS: $ADDRESS" >> dbconfig.txt
echo "DB_PATH: $DB_PATH" >> dbconfig.txt
echo "DBNAME: $DBNAME" >> dbconfig.txt

echo "ADMIN: $ADMIN" >> dbconfig.txt
echo "PASSWORD: $PASSWORD"  >> dbconfig.txt

echo "WRITEUSER_NAME: $WRITEUSER_NAME"  >> dbconfig.txt
echo "WRITEUSER_PWD: $WRITEUSER_PWD"  >> dbconfig.txt

echo "READUSER_NAME: $READUSER_NAME"  >> dbconfig.txt
echo "READUSER_PWD: $READUSER_PWD"  >> dbconfig.txt
echo "====================================="

# Constants
# =========

ASCENDING=1
DESCENDING=-1

set -vm


if ! which sarc >/dev/null 2>&1; then
    echo "sarc commandline is not installed. Please install it before running this script."
    exit 1 
fi

function _mongo_no_auth {
    #
    #   Starts mongodb without Access Control, this is used to insert the admin user
    #
    # 

    rm -rf $DB_PATH
    mkdir -p $DB_PATH

    $MONGOD_CMD --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip $ADDRESS --pidfilepath $DB_PATH/pid 2>&1 > $DB_PATH/mongo_1.log
}


function _fetch_mongo_version_auth {
    mongosh --norc "mongodb://$ADDRESS:$PORT" --authenticationDatabase "admin" -u $ADMIN -p $PASSWORD --eval "db.version()"
}

function wait_mongo_auth {

    until _fetch_mongo_version_auth 
    do
        echo "Failed"
    done
}

function _fetch_mongo_version {
    mongosh --norc "mongodb://$ADDRESS:$PORT" --eval "db.version()"
}

function wait_mongo {

    until _fetch_mongo_version 
    do
        echo "Failed"
    done
}

function mongo_launch {
    #
    #   starts a new mongodb instance running locally at a specified location
    #
    #   Usage:
    #       
    #       mongo_launch
    #
    mkdir -p $DB_PATH
    $MONGOD_CMD --auth --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip $ADDRESS --pidfilepath $DB_PATH/pid 2>&1 > $DB_PATH/mongo_2.log
}


function mongo_stop {
    #
    #   stop the mongodb instance running at the current DB path
    #
    #   Usage:
    #   
    #       mongo_stop
    #

    if [ "$USE_PODMAN" = "1" ]; then
        sudo -H -u sarc podman stop sarc_mongo --log-level error
    else
        PID="$(cat $DB_PATH/pid)"
        echo "PID: $PID"
        kill -s SIGTERM $PID
    fi
}


function mongo_start {
    #
    #   Starts a clean mongodb instance
    #

    # Initialize mongodb
    _mongo_no_auth &
    wait_mongo

    SARC_MODE=scraping sarc db init --database $DBNAME --url "mongodb://$ADDRESS:$PORT" --username $ADMIN --password $PASSWORD --account admin
    SARC_MODE=scraping sarc db init --database $DBNAME --url "mongodb://$ADDRESS:$PORT" --username $WRITEUSER_NAME --password $WRITEUSER_PWD --account write
    SARC_MODE=scraping sarc db init --database $DBNAME --url "mongodb://$ADDRESS:$PORT" --username $READUSER_NAME --password $READUSER_PWD --account read

    mongo_stop
    fg

    # Starts mongodb with auth mode
    mongo_launch &
    wait_mongo_auth

    echo "Setup Done"
    fg
}


function mongo_restore {
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

    for collection in "${collections[@]}"; do
        cmd="mongorestore --gzip --port=$PORT -d $db $path/$db/$collection.bson.gz"

        echo "$cmd"
        $cmd
    done
}


function get_backups {

    scp -R sarc:/home/sarc/mongo_backups/sarc_mongo.sarc.2023-07-19 

}

if [ "$LAUNCH_MONGO" = "1" ]; then
    mongo_start
fi
