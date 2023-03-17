# Deployment

***NOTE: this file is still in a work-in-progress status***

## Server

Let's assume you have access to the sarc VM, and are a `sudoers` group member.

## User

The user running everything is `sarc`

```
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
cd ~
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

(Note: Later, when executed by `systemd` for example, the PATH won't be present so the scripts will use the complete path to `/home/sarc/.local/bin/poetry` )

### Setup code

As `sarc` user :

```
cd ~/SARC
poetry install
```

Test the sarc command: `poetry run sarc`

In the future, if necessary, use the $SARC_CONFIG environment variable to choose the config file.


## MongoDB

*** This whole MongoDB section is still in progress ***

### Create the sarc_mongo container

***TODO***
(note wip: `scripts/remote/mongo.sh`)

### Systemd file

MongoDB container must be automatically started on boot.
See following "systemd services" section;

***notes: https://www.howtogeek.com/687970/how-to-run-a-linux-program-at-startup-with-systemd/***

## systemd services

The service scripts located in `scripts/systemd/` are in charge of starting the scheduled scraping tasks, start containers on system boot, etc.

They are installed by the `install_services.sh` script mentionned earlier.

