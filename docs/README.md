
# SARC

SARC stands for "Supervision et Analyde des Resources de Calcul". It's a Mila in-house tool to monitor the usage of computational resources by Mila users. It is not meant to be deployed on the users workstations.


## Installation and setup

This is the guide to use the client. For deployment, see `docs/deployment.md`.

To install sarc, git clone the repo and install `sarc` using `poetry`.

```bash
$ git clone git@github.com:mila-iqia/SARC.git
$ cd SARC
$ poetry install
```

`sarc` will be looking into the current working directory to find the dev config file. To work with prod config or
from any directory set the environment variable as follow.

```bash
$ export SARC_CONFIG=/path/to/SARC/config/sarc-prod.json
```

To access the database, you need to setup the mila idt vpn and create an ssh
tunnel. If you never accessed the VM, [see documention here first](https://mila-iqia.atlassian.net/wiki/spaces/IDT/pages/2325840018/VM+sarc01-dev).

To create the ssh tunnel:


```bash
$ ssh -L 27017:localhost:27017 sarc
```

You can now test on your machine a simple example to see if `sarc` is able to access the database:

```bash
$ poetry run python example/waste_stats.py
```

## Contributing

### Before commits

Those commands are for the proper formatting.
```
black .
isort --profile black .
tox -e isort
tox -e black
tox -e lint
```

### How to add dependencies

TODO : How does poetry work? Needs a simple example here for the command to add a python module like `ldap3`. What are we supposed to type?


### How to run the tests suite

This runs the tests.
```
tox -e test
```

If you're running on Mac OS, you can install `podman` with `brew install podman`.
Later you can start the virtual machine with
```
podman machine init
podman machine start
```

### Scripts meant to be run on their own

```
sarc/account_matching/make_matches.py
sarc/account_matching/update_account_matches_in_database.py
sarc/inode_storage_scanner/get_diskusage.py  (stub)
```

### How to generate doc

To generate documentation in HTML format in folder `docs\_build`:

```
sphinx-build -b html docs/ docs/_build
```

You can then open `docs\_build\index.html` on a web browser.
