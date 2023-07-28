#!/bin/sh

# code checkout script
# run this script once you have the `secrets` folder deployed on the remote machine and the github key created

# usage :
# bash secrets_deploy.sh <server> 

if [ -z "$1" ]
  then
    echo "No arg specified ; you must specify a server on which you have ssh access (with a sudoer account)"
    exit
fi

DESTINATION_FOLDER="/home/sarc/"
# if [ -z "$2" ]
#   then
#     echo "Using default destination folder."
#   else
#     DESTINATION_FOLDER="/home/$2/"
# fi
echo "Destination folder = \"$DESTINATION_FOLDER\""

# do your magic, script !
echo "Deploying code to \"$1:$DESTINATION_FOLDER\" ..."
ssh $1 'bash -s' << 'ENDSSH'
    # su sarc
    sudo su sarc
    cd

    # checkout code
    ssh-keyscan github.com >> ~/.ssh/known_hosts
    git clone git@github-sarc:mila-iqia/SARC.git -q

    # move secrets folder
    if [ -d "SARC" ]
      then
        cp -r secrets SARC/
    fi

ENDSSH

