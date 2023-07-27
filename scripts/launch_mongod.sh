#!/bin/bash

PORT=${MONGO_PORT:-"8123"}
ADDRESS=${MONGO_ADDRESS:-"localhost"}
DB_PATH=${MONGO_PATH:-"/tmp/db"}

ADMIN=${MONGO_ADMIN:-"god"}
PASSWORD=${MONGO_PASS:-"god123"}
DBNAME=${MONGO_DB:-"sarc"}

ASCENDING=1
DESCENDING=-1

function _mongo_no_auth {
    #
    #   Starts mongodb without Access Control, this is used to insert the admin user
    #
    # 

    rm -rf $DB_PATH
    mkdir -p $DB_PATH

    mongod --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid
    sleep 1
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
    mongod --auth --dbpath $DB_PATH/ --wiredTigerCacheSizeGB 1 --port $PORT --bind_ip localhost --pidfilepath $DB_PATH/pid 
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

    CMD=$(cat <<EOM
    use admin
    db.createUser({
        user: "$username",
        pwd: "$password",
        roles: [
            { role: "userAdminAnyDatabase", db: "admin" },
            { role: "readWriteAnyDatabase", db: "admin" },
        ]
    })
EOM
    )

    echo "$CMD" | mongosh --port $PORT

    add_user $username $password
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

    echo "$CMD" | mongosh "mongodb://$ADDRESS:$PORT" --authenticationDatabase "admin" -u $ADMIN -p $PASSWORD
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
        privileges: {
            {resource: {"db": "$DBNAME, collection: "allocations"}, actions: ["find"]},
            {resource: {"db": "$DBNAME, collection: "diskusage"}, actions: ["find"]},
            {resource: {"db": "$DBNAME, collection: "users"}, actions: ["find"]},
            {resource: {"db": "$DBNAME, collection: "jobs"}, actions: ["find"]},
        }
    })
EOM
    )

    echo "$CMD" | mongosh "mongodb://$ADDRESS:$PORT" --authenticationDatabase "admin" -u $ADMIN -p $PASSWORD
}

function _ensure_indexes {
    CMD=$(cat <<EOM
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

    echo "$CMD" | mongosh --port $PORT
}


function mongo_init {
    #
    #   Starts mongo without auth to create an admin user
    #   Initialize the indexes and stop the db
    #
    _mongo_no_auth &
    _add_admin_user $ADMIN $PASSWORD
    _ensure_indexes
    stop_mongo
}


function mongo_start {
    #
    #   Starts a clean mongodb instance
    #

    # Initialize mongodb
    mongo_init

    # Starts mongodb with auth mode
    mongo_launch &

    add_read_write_user "user-readwrite" "password1"
    add_readonly_user "user-readonly" "password2"

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