
# SARC

In-house tool to monitor the usage of various computational resources at Mila.

## Scripts meant to be run on their own

```
sarc/account_matching/make_matches.py
sarc/account_matching/update_account_matches_in_database.py
sarc/inode_storage_scanner/get_diskusage.py  (stub)
```

## Before commits

Those commands are for the proper formatting.
```
black .
isort --profile black .
tox -e isort
tox -e black
tox -e lint
```

## How to add dependencies

TODO : How does poetry work? Needs a simple example here for the command to add a python module like `ldap3`. What are we supposed to type?


## How to run the tests suite

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

