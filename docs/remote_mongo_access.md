# Remove Mongo access

## MongoDB port tunneling

With an SSH access to the production machine, you can easilly tunnel the mongoDB. Example in the `~/.ssh/config` file:

```
Host sarc
    (...)
    LocalForward 27018 127.0.0.1:27017
```

## SARC config file

Simply modify the config JSON file you use:

```
    "mongo": {
        "connection_string": "localhost:27018",
        "database_name": "sarc"
    },

```
