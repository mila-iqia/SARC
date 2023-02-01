#!/bin/sh

# Initial server configuration avec bare OS install
# Setup script for a bare ubuntu system

if [ -z "$1" ]
  then
    echo "No arg specified ; you must specify a server on which you have ssh access"
    exit
fi

ssh $1 'bash -s' << 'ENDSSH'
  sudo apt update
  sudo apt upgrade -y
  sudo apt install -y git podman
ENDSSH

