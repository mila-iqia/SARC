# Setup Scripts

## serverinit.sh
Installs base packages on a bare new OS.
SSH access and sudo rights on the target server needed.

```
$ chmod +x serverinit.sh
$ ./serverinit.sh <server>
```

## mongo.sh
Install, start and stop mongo package on the server

```
$ ./mongo.sh <server> [install|start|stop|status]
```
