#!/bin/sh

# install/start/stop mongo image on server $1


if [ "$#" -lt 2 ]
then
    echo 'Usage : mongo.sh <server> [install|start|stop|status]'
    exit 1
fi

case $2 in
    install)
        echo "MongoDB installation on $1..."
        status=$(ssh $1 'sudo -H -u sarc podman container list -a --log-level error | grep sarc_mongo') 
        if [[ -z $status ]];
        then
            echo 'Container sarc_mongo not found, creating it.'
            ssh $1 'sudo -H -u sarc podman run -dt --name sarc_mongo -p 27017:27017/tcp --log-level error docker.io/library/mongo:latest'
        else
            echo 'Container sarc_mongo already exists ! Skipping.'
        fi
        ;;
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
