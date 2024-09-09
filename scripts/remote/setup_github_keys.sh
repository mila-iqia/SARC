#!/bin/sh

# GitHub Deploy Keys creation script
# run this script as the 'sarc' user on the server

if [ -z "$1" ]
  then
    echo "No arg specified ; you must specify a server on which you have ssh access"
    exit
fi

ssh $1 'bash -s' << 'ENDSSH'
    # be the sarc user
    sudo su sarc
    cd

    #create github keys dir
    mkdir -p -m 700 ~/.ssh/github_keys

    # add entry to config
    echo 'Host github-sarc
  Hostname github.com
  IdentityFile ~/.ssh/github_keys/mila-sarc-id_rsa' >> ~/.ssh/config

    chmod 644 ~/.ssh/config

    # create keys
    ssh-keygen -f ~/.ssh/github_keys/mila-sarc-id_rsa -t ecdsa -b 521
    chmod -R 644 ~/.ssh/github_keys/*id_rsa.pub
    chmod -R 600 ~/.ssh/github_keys/*id_rsa
ENDSSH
