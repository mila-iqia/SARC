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

Before going anywhere, we need to copy the ssh keys in our `~/.ssh` folder.
Get the ssh folder provided through "some secure external channel" as stated in [the ***"Secrets"*** document](secrets.md)

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
```
curl -sSL https://install.python-poetry.org | python3 -
```

As told by the script itself: Add `export PATH="/home/sarc/.local/bin:$PATH"` to your shell configuration file.

### Setup code

As `sarc` user :

```
cd ~/SARC
poetry install
```

Test the sarc command: `poetry run sarc`

In the future, if necessary, use the $SARC_CONFIG environment variable to choose the config file.

## MongoDB

## Cron jobs

### jobs
### allocations
### storage
### account matching
(Ceci doit plutôt être fait manuellement)

