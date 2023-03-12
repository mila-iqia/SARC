# Deployment

## Server

Let's assume you have access to the sarc VM, and are a `sudoers` group member.

## User

The user running everything is `sarc`

```
cd /home
sudo su sarc
cd ~
```

## Access rights

### ssh folder

Before going anywhere, we need to copy the ssh keys and `config` files in our `~/.ssh` folder.
Get the ssh folder provided through "some secure external channel" as stated in [the ***"Secrets"*** document](secrets.md), and copy its content to the `~/.ssh/` folder of the `sarc` user.

### GitHub access

We need to add a "deploy key" to the GitHub repo. To do that, from your workstation, run the `SARC/scripts/remote/setup_github_keys.sh <server>` script.

Then, on the server:
```
sudo cat /home/sarc/.ssh/github_keys/mila-sarc-id_rsa.pub
```

It will display the content of the previously generated public key ; copy/paste this key in [the Deploy keys settings of the github project](https://github.com/mila-iqia/SARC/settings/keys).
(You don't need write access)

## Get SARC

### Get code

Once the deploy keys are set up, you can clone the repo:

```
# as sarc user
cd
git clone git@github-sarc:mila-iqia/SARC.git
```

### Dependencies
#### Poetry


As `sarc` user, install poetry:

(follow [this method](https://www.digitalocean.com/community/tutorials/how-to-install-poetry-to-manage-python-dependencies-on-ubuntu-22-04#step-1-installing-poetry), since the apt method seems broken someway)

```
curl -sSL https://install.python-poetry.org | python3 -
```

As told by the script itself: Add `export PATH="/home/sarc/.local/bin:$PATH"` to `.bashrc`.

### Setup code

As `sarc` user :

```
cd ~/SARC
poetry install
```

Test the sarc command: `poetry run sarc`

In the future, if necessary, use the $SARC_CONFIG environment variable to choose the config file.

## MongoDB

### Create the sarc_mongo container

Remotely (from your computer) you can use the script 
***TODO***

### Systemd file

***notes: https://www.howtogeek.com/687970/how-to-run-a-linux-program-at-startup-with-systemd/***

You need to link the service files to systemd; 

```
sudo sarc/scripts/deploy/install_service.sh
```


Copy the `sarc_mongo.service` file to systemd :
***TODO sarc_mongo.service***

```
sudo cp serverscripts/systemd/sarc_mongo.service /etc/systemd/system
sudo chmod 640 /etc/systemd/system/sarc_mongo.service
sudo systemctl daemon-reload
```
***TODO le service doit appeler un script, on le met où? on code en dur le chemin /home/sarc/SARC ?***

### Service setup to start on system startup
```
sudo systemctl enable sarc_mongo
```

### Service start
The service will start at system startup, but you must start it manually if you don't want to reboot the server:
```
sudo systemctl start sarc_mongo
```

## Cron jobs

This is indeed a bad name; in systemd, we use timer services.


`myService.service` goes to /etc/systemd/system
`myService.timer` goes to 

### jobs
1x par jour
### allocations
### storages
1x par jour
### account matching
(Ceci doit plutôt être fait manuellement)
EDIT: une fos par jour c'est ok ?

