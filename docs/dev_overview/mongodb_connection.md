# Connect to mongoDB

SARC currently uses a mongoDB database. It can be hosted anywhere, and currently it is hosted on the `sarc01-dev` machine, where the SARC scraping code runs.
Currently there is no client API (REST for example) so you will have to clone the SARC repo on your client system and connect to mongoDB directly.

It means you need to have an access to the `sarc01-dev` machine on the mongodb port `21017` to be able to read data.

## Credentials

There is a section in the configuration files for the mongoDB credentials.
Example in remote ssh-tunneling client mode:
```
    "mongo": {
        "connection_string": "mongodb://readuser:readpwd@localhost:27018/sarc",
        "database_name": "sarc"
    },
```

There are three mongodb user roles: `read`, `write` and `admin`. `readuser:readpwd` above are the client credentials. 

The security is delegated to the ssh access to `sarc01-dev`.

reference: [Configuration files documentation](config_file.md)

## ssh tunnels

```mermaid
---
title: ssh tunnel to mongoDB database
---
flowchart LR
	subgraph Mila
		%% direction BT
		subgraph sarc01_dev
		 	%% direction LR
			mongodb[(mongoDB)]
			sarc["SARC (scraping)"]
		end
	end

    client["SARC (client)"]

	mongodb <==> sarc  

    client-."ssh forward port 27018".-mongodb


	%% Mila ~~~ DRAC
```

You can achieve that by setting a ssh tunnel and modify your `config/sarc-xxxx.json` file accordingly. Refer to the [documentation](../remote_mongo_access.md) page to do it.

## Future database migration

SARC will probably migrate its database to a mongoDB instance managed by IT, so that we won't have to manage the backups and security updates ourselves.

reference: [Backups](backups.md)
