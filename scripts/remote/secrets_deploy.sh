#!/bin/sh

# secrets deployment script
# run this script once you have the `secrets` folder deployed locally

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


SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
cd $SCRIPTPATH/../../


# copy secrets folder in user directory on target system
echo "Copying secrets folder to \"$1\" system..."
if [ ! -d "secrets/" ]
  then
    echo "The secrets folder does not exist on your system."
    exit
fi
rsync -va secrets $1:~/

# copy secrets folder to sarc user folder and change file ownership
echo "Copying secrets folder to \"$DESTINATION_FOLDER\" ..."
ssh $1 'bash -s' << 'ENDSSH'
    # copy files
    sudo cp -rf secrets/ /home/sarc

    # change ownership
    sudo chown -R sarc:sarc /home/sarc/secrets

    # copy /ssh files
    echo "Deploy DRAC ssh keys and ssh config file ..."

    # su sarc
    sudo su sarc
 
    # copy ssh files
    mkdir -p -m 700 ~/.ssh
    cp ~/secrets/ssh/config ~/.ssh
    cp ~/secrets/ssh/id_sarc ~/.ssh
    cp ~/secrets/ssh/id_sarc.pub ~/.ssh
    chmod 700 ~/.ssh
    chmod 600 ~/.ssh/id_sarc
    chmod 644 ~/.ssh/id_sarc.pub
    chmod 644 ~/.ssh/config
ENDSSH


# important notice
echo "WARNING : you have to run \"bash setup_github_keys.sh $1\" and setup your github deploy key,"
echo "even if you've already done so, since .ssh/config file was just overwritten on \"$1\" by this script."
echo "Then, run the \"remote_checkout.sh\" script to deploy the codebase."