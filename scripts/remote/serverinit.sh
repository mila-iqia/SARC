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
  sudo touch /etc/containers/registries.conf
  if grep '\[registries.search\]' /etc/containers/registries.conf;
  then
    echo 'Containers registries already configured'
  else
    echo 'Configuring containers registries...'
    sudo printf "[registries.search]\nregistries=['quai.io','docker.io']\n" | sudo tee -a /etc/containers/registries.conf
  fi
ENDSSH

