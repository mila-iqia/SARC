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
        ssh $1 'podman pull mongo && podman images | grep mongo'
        ;;
    start)
        echo "MongoDB start..."
        ssh $1 'podman run -dt -p 27017:27017/tcp mongo'
        ;;
    stop)
        echo "MongoDB stop..."
        status=$(ssh $1 'podman ps | grep mongo') 
        if [[ -z $status ]];
        then
            echo 'MongoDB not started !'
        else
            id=(${status//" "/ })
            echo "Stopping MongoDB image:"
            ssh $1 "podman stop $id"
        fi
        ;;
    status)
        echo "MongoDB status..."
        status=$(ssh $1 'podman ps | grep mongo') 
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
