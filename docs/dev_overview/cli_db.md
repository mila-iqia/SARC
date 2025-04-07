# Initialisation de la DB

## CLI

the `db init` command is used to initialise the database and the 3 db accounts (read, write and admin):

```
SARC_MODE=scraping poetry run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username admin --password <admin password> --account admin
SARC_MODE=scraping poetry run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username readuser --password readpwd --account read 
SARC_MODE=scraping poetry run sarc db init --database sarc --url "mongodb://mongoadmin:<admin password>@localhost:27017" --username writeuser --password <writeuser password> --account write
``` 

see [the mongodb section of deployment.db](../deployment.md#mongodb-users-credentials) for more info