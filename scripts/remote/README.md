## serverinit.sh
Installs base packages on a bare new OS.
SSH access and sudo rights on the target server needed.

```
$ bash serverinit.sh <server>
```

## mongo.sh
Install, start and stop mongo package on the server

```
$ bash mongo.sh <server> [install|start|stop|status]
```

You **MUST** run `./mongo.sh <server> install` once before deploying systemd scripts.

## secrets_deploy.sh
Copy the secrets files and ssh keys to the server
```
$ bash setup_github_keys.sh <server> 
```

## setup_github_keys.sh
Generate the ssh "deploy keys" needed to deploy the code to the server
```
$ bash setup_github_keys.sh <server> 
```

## remote_checkout.sh
Checkout the codebase on the remote server
```
$ bash remote_checkout.sh <server>
```

See [The deployment doc](../../docs/deployment.md) for more info.