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

Before going anywhere, we need to copy the ssh keys and `config` files in our `~/.ssh` folder.
Get the ssh folder provided through "some secure external channel" as stated in [the ***"Secrets"*** document](secrets.md), and copy its content to th `~/.ssh/` folder of the `sarc` user.

## Get SARC


### Get code

First things first, let's checkout the sarc code **in your own home folder**:

```
git clone git@github.com:mila-iqia/SARC.git -key <your-private-ssh-key-that-has-access-rights-to-the-repo>
```

...then copy it to the `sarc` user folder.

```
sudo cp SARC /home/sarc -r
sudo chown sarc:sarc /home/sarc/SARC -R
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

***TODO***

### Systemd file

***notes: https://www.howtogeek.com/687970/how-to-run-a-linux-program-at-startup-with-systemd/***


Copy the `sarc_mongo.service` file to systemd :

```
sudo cp serverscripts/systemd/sarc_mongo.service /etc/systemd/system
sudo chmod 640 /etc/systemd/system/sarc_mongo.service
sudo systemctl daemon-reload
```
### Service setup to start on system startup
```
sudo systemctl enable sarc_mongo
```

### Service start
The service will satrt at system startup, but you must start it manually if you don't want to reboot the server:
```
sudo systemctl start sarc_mongo
```

## Cron jobs

### jobs
### allocations
### storage
### account matching
(Ceci doit plutôt être fait manuellement)

