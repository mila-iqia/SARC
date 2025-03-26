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

### Lingering

For `podman` to avoid throwing warnings under a `su sarc` session, you should enable login lingering:

`sudo loginctl enable-linger sarc`

## Get SARC

### Get code

Once the deploy keys are set up, you can clone the repo:

```
# as sarc user
cd ~
git clone git@github-sarc:mila-iqia/SARC.git
```

### Dependencies
#### uv

install uv:

See here for documentation: https://docs.astral.sh/uv/getting-started/installation/

Ensure that uv is either available on $PATH or is at a known location

## MongoDB

### Mongodb users credentials

You have 3 mongo users account; 

- `mongoadmin` (password: see `secrets/mongo_admin_password.txt`) is the global mongodb administrator account.
- `readuser`(password: `readpwd`, see `config/sarc-client.json`) is the user for general use (only reads data from the database)
- `writeuser` (password: see `secrets/mongo_writeuser_password.txt`) is used by the server during scraping

Therefore, if you want to admin the database with compass, the connection string will be (see `config/sarc-*.json`) :
```
mongodb://writeuser:<password>@localhost:27017/sarc
```
(adjust password and port to match your own setup)

### Create the sarc_mongo container (with authentication enabled)

A mongoDB container must be created on the VM prior systemd services deployment.

Things got a bit more complex with authentication. Here is a step-by-step mongodb creation procedure:

(from a session with `sarc` user on the server)

Container creation with authentication:
```
podman run -dt --name sarc_mongo -p 27017:27017/tcp \
    -e MONGO_INITDB_ROOT_USERNAME=mongoadmin \
    -e MONGO_INITDB_ROOT_PASSWORD=<admin password> \
    docker.io/library/mongo:4.4.7
```

note: version 4.4.7 is used to prevent compatibility issues with some x86_64 versions on virtual machines.

See https://www.mongodb.com/docs/manual/administration/production-notes/#x86_64 for more informations.

Users creation:

```
SARC_MODE=scraping uv run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username admin --password <admin password> --account admin
SARC_MODE=scraping uv run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username readuser --password readpwd --account read
SARC_MODE=scraping uv run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username writeuser --password <writeuser password> --account write
```

### (optionnal) database restoration

If you want to restore previously backuped files, follow these steps

- first connect to the database with Compass and empty the `clusters` collection. THIS IS IMPORTANT since the `mongorestore` utility only inserts documents, and cannot update them. The clusters were created by the `sarc db init` command above, so they must be removed to enable their restoration.

Now, say the backup folder is located in `~/mongo_backups/sarc_mongo.sarc.2023-08-14/sarc`, therefore it should contain (plus some other files):

```
allocations.bson.gz
clusters.bson.gz
diskusage.bson.gz
jobs.bson.gz
users.bson.gz
```

Restoring them is done in two steps:

- first, copy the folder to the container
- second, use `mongorestore` to insert them in the database

In our example:

```
# echo Copying backup file to container...
# echo copying $BACKUP to sarc_mongo:/
podman cp ~/mongo_backups/sarc_mongo.sarc.2023-08-14/sarc/ sarc_mongo:/

# echo Restoring backup file...
podman exec sarc_mongo mongorestore --username=writeuser --password=$PASSWORD -d sarc /sarc/jobs.bson.gz --gzip
podman exec sarc_mongo mongorestore --username=writeuser --password=$PASSWORD -d sarc /sarc/users.bson.gz --gzip
podman exec sarc_mongo mongorestore --username=writeuser --password=$PASSWORD -d sarc /sarc/allocations.bson.gz --gzip
podman exec sarc_mongo mongorestore --username=writeuser --password=$PASSWORD -d sarc /sarc/clusters.bson.gz --gzip
podman exec sarc_mongo mongorestore --username=writeuser --password=$PASSWORD -d sarc /sarc/diskusage.bson.gz --gzip
```

... and that should do. 

### MongoDB manual start

Once systemd scripts deployed (see following section), the database will be started automatically at boot time; but you can already start / stop it manually :

```
scripts/remote/mongo.sh <sarc-server> start 
```

```
scripts/remote/mongo.sh <sarc-server> stop 
```

### Systemd file

MongoDB container must be automatically started on boot.
See following "systemd services" section;

***notes: https://www.howtogeek.com/687970/how-to-run-a-linux-program-at-startup-with-systemd/***

## systemd services

The service scripts located in `scripts/systemd/` are in charge of starting the scheduled scraping tasks, start containers on system boot, etc.

They are installed by the `install_services.sh` script mentionned earlier.

from the server, in a `sarc` user script session :

```
cd ~/SARC
sudo scripts/deploy/install_services.sh
```

At any time, from a `sarc` session on the server, you can start or stop the containers (including mongoDB):

```
sudo systemctl start sarc_containers.service
```

```
sudo systemctl stop sarc_containers.service
```
(The `install_services.sh` should have configured it to automatically launch MongoDB at boot time, though.)

