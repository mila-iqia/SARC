#!/bin/bash

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

function _mongo_no_auth {
    #
    #   Starts mongodb without Access Control, this is used to insert the admin user
    #
    # 

    rm -rf $DB_PATH
    mkdir -p $DB_PATH

    mongod --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid 2>&1 > $DB_PATH/mongo_1.log
    sleep 1
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
    mongod --auth --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid 2>&1 > $DB_PATH/mongo_2.log
}


function mongo_stop {
    #
    #   stop the mongodb instance running at the current DB path
    #
    #   Usage:
    #   
    #       mongo_stop
    #
    mongod --dbpath $DB_PATH/ --shutdown
}


function _add_admin_user {
    #
    #   create an admin user
    #
    #   Usage:
    #
    #       _add_admin_user username password
    #

    username=$1
    password=$2

    # Create Read Only role
    CMD="
    use $DBNAME;
    db.createRole({
        role: \"sarcReadOnly\",
        roles: [],
        privileges: [
            {resource: {db: \"$DBNAME\", collection: \"allocations\"}, actions: [\"find\"]},
            {resource: {db: \"$DBNAME\", collection: \"diskusage\"}, actions: [\"find\"]},
            {resource: {db: \"$DBNAME\", collection: \"users\"}, actions: [\"find\"]},
            {resource: {db: \"$DBNAME\", collection: \"jobs\"}, actions: [\"find\"]},
        ]
    })"
    
    echo "$CMD" | mongosh --norc "mongodb://$ADDRESS:$PORT"

    CMD="
    use admin;
    db.createUser({
        user: \"$username\",
        pwd: \"$password\",
        roles: [
            { role: \"userAdminAnyDatabase\", db: \"admin\" },
            { role: \"readWriteAnyDatabase\", db: \"admin\" },
        ]
    })"

    echo "$CMD" | mongosh --norc "mongodb://$ADDRESS:$PORT"
}


function add_read_write_user {
    #
    #   Create a user for the sarc database
    #
    #   Usage:
    #
    #       add_user username password
    #

    username=$1
    password=$2

    CMD=$(cat << EOM
    use $DBNAME
    db.createUser({
        user: "$username",
        pwd: "$password",
        roles: [
            { role: "readWrite", db: "$DBNAME" }
        ]
    })
EOM
    )

    echo "$CMD" | mongosh --norc "mongodb://$ADDRESS:$PORT" --authenticationDatabase "admin" -u $ADMIN -p $PASSWORD
}

function add_readonly_user {
    #
    #   Create a user for the sarc database
    #
    #   Usage:
    #
    #       add_readonly_user username password
    #

    username=$1
    password=$2

    CMD=$(cat << EOM
    use $DBNAME
    db.createUser({
        user: "$username",
        pwd: "$password",
        roles: [
            { role: "sarcReadOnly", db: "$DBNAME" }
        ]
    })
EOM
    )

    echo "$CMD" | mongosh --norc "mongodb://$ADDRESS:$PORT" --authenticationDatabase "admin" -u $ADMIN -p $PASSWORD
}

function _ensure_indexes {
    CMD=$(cat << EOM
    use $DBNAME

    db.clusters.createIndex({"cluster_name": $ASCENDING}, { unique: true})

    db.users.createIndex({"mila_ldap.mila_email_username": $ASCENDING})
    db.users.createIndex({"mila_ldap.mila_cluster_username": $ASCENDING})
    db.users.createIndex({"drac_roles.username": $ASCENDING, "drac_members.username": $ASCENDING})
    
    db.allocations.createIndex({"cluster_name": $ASCENDING, "start": $ASCENDING, "end": $ASCENDING})
    db.allocations.createIndex({"start": $ASCENDING, "end": $ASCENDING})
    
    db.diskusage.createIndex({"cluster_name": $ASCENDING, "groups.group_name": $ASCENDING, "timestamp": $ASCENDING})
    db.diskusage.createIndex({"cluster_name": $ASCENDING, "timestamp": $ASCENDING})
    db.diskusage.createIndex({"timestamp": $ASCENDING})

    db.jobs.createIndex({"job_id": $ASCENDING, "cluster_name": $ASCENDING, "submit_time": $ASCENDING}, { unique: true})
    db.jobs.createIndex({"cluster_name": $ASCENDING, "job_state": $ASCENDING, "submit_time": $ASCENDING, "end_time": $ASCENDING})
    db.jobs.createIndex({"cluster_name": $ASCENDING, "submit_time": $ASCENDING, "end_time": $ASCENDING})
    db.jobs.createIndex({"job_state": $ASCENDING, "submit_time": $ASCENDING, "end_time": $ASCENDING})
    db.jobs.createIndex({"submit_time": $ASCENDING, "end_time": $ASCENDING})
EOM
    )

    echo "$CMD" | mongosh  --norc "mongodb://$ADDRESS:$PORT"
}


function mongo_init {
    #
    #   Starts mongo without auth to create an admin user
    #   Initialize the indexes and stop the db
    #
    _mongo_no_auth &
    wait_mongo

    _ensure_indexes
    _add_admin_user $ADMIN $PASSWORD
   
    mongo_stop
    fg
}


function mongo_start {
    #
    #   Starts a clean mongodb instance
    #

    # Initialize mongodb
    mongo_init

    # Starts mongodb with auth mode
    mongo_launch &
    wait_mongo_auth

    add_read_write_user $ADMIN $PASSWORD

    # Use the admin account to add users
    add_read_write_user $WRITEUSER_NAME $WRITEUSER_PWD

    add_readonly_user $READUSER_NAME $READUSER_PWD

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



mongo_start
