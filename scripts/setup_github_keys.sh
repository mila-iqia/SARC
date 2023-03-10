#!/bin/sh

# GitHub Deploy Keys creation script
# run this script as the 'sarc' user on the server

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

# display public key 
echo 'Paste this in the deploy keys settings of the SARC github repository:'
cat ~/.ssh/github_keys/mila-sarc-id_rsa.pub
