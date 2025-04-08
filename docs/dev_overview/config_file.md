# Configuration file


The env var `SARC_MODE` specifies the mode (see [Client mode vs scraping mode](client_scraping_modes.md) for more informations on this topic); 

The env var `SARC_CONFIG` can be used to specify wich config file to use; example:
```
% SARC_CONFIG=config/sarc-client.json uv run sarc health history
% SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.json.json uv run sarc acquire jobs -d auto -c mila
```
The config file must match the wanted mode (client/scraping) otherwise `pydantic` will prevent the program to run (too many fields or missing fields, depending on the case). 

## client version

The client version of the file is quite short. Its only purpose is to provide read access to the database, and nothing else. 

see [sarc-client.json](../../config/sarc-client.json) for reference.

## scraping version

THis version is far more complete. It contains links and configuration infos for various purpose:
- db read/write credentials
- ldap credentials
- configuration settings for users account matching
- clusters configurations
    - ssh credentials
    - (optionnal) prometheus credentials
    - various settings

see [sarc-prod.json](../../config/sarc-prod.json) for reference.

## using custom config files

It is convenient to use a custom configuration file on dev system, for various reasons, for example:
- use of a different database, so that you can test things around
- adapt the mongodb connection string to your specific ssh tunneling setup
- ...

For example, there is usually a copy of the production database named "sarc-backup". 

An unversionned modified copy of `config/sarc-prod.json` with the corresponding mongodb connection string can be used to test wacky things:
```
% SARC_CONFIG=config/sarc-backup-rw.json SARC_MODE=scraping uv run sarc acquire jobs -c mila -d auto
```

Another good practice is to NEVER set the db write password in the `config/sarc-prod.json` on dev machines, to avoid any accident on the production database. The corresponding section of `sarc-prod.json` is set like so in the repository:
```json
    "mongo": {
        "connection_string": "mongodb://writeuser:REPLACEME@localhost:27017/sarc",
        "database_name": "sarc"
    },
```
==> never replace "REPLACEME" unless you know EXACTLY what you're doing !