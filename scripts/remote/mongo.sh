#!/bin/sh

# install/start/stop mongo image on server $1


if [ "$#" -lt 2 ]
then
    echo 'Usage : mongo.sh <server> [start|stop|status]'
    exit 1
fi

case $2 in
    start)
        echo "MongoDB start..."
        ssh $1 'sudo -H -u sarc podman restart sarc_mongo --log-level error'
        ;;
    stop)
        echo "MongoDB stop..."
        ssh $1 'sudo -H -u sarc podman stop sarc_mongo --log-level error'
        ;;
    status)
        echo "Checking MongoDB status..."
        status=$(ssh $1 'sudo -H -u sarc podman ps --log-level error | grep sarc_mongo') 
        if [[ -z $status ]];
        then
            echo 'MongoDB not started.'
        else
            echo 'MongoDB started:'
            echo $status
        fi
        ;;
    *)
        echo "unknown command: $2"
        ;;
esac
