# Remote scripts

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

You **MUST** run `./mongo.sh <server> install` once before deploying systemd scripts.

## setup_github_keys.sh
Generate the ssh "deploy keys" needed to deploy the code to the server
```
$ ./setup_github_keys.sh <server> 
```

See [The deployment doc](../../deployment.md) for more info.
