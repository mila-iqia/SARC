# Configuration file


The env var `SARC_MODE` specifies the mode (see [Client mode vs scraping mode](client_scraping_modes.md) for more informations on this topic); 

The env var `SARC_CONFIG` can be used to specify wich config file to use; example:
```
# SARC_CONFIG=config/sarc-client.json poetry run sarc health history
# SARC_MODE=scraping SARC_CONFIG=config/sarc-dev.json.json poetry run sarc acquire jobs -d auto -c mila
```
The config file must match the wanted mode (client/scraping) otherwise `pydantic` will prevent the program to run (too many fields or missing fields, depending on the case). 

## client version

The client version of the file contains the following fields:
```

```

## scraping version
