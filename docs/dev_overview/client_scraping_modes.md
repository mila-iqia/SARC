# client mode vs. scraping mode

SARC has two modes: `client` (read-only) and `scraping` (r/w)

By default, the CLI uses the `client` mode. To enable the `scraping mode` you have to set the envvar `SARC_MODE=scraping` :

```
# SARC_MODE=scraping poetry run sarc acquire users
```

The two modes use different config files.  cf [Configuration file](config_file.md)

## client mode

This mode is the default one. It is meant to read data from the database and use it.

In this mode, the mongodb account has read-only access. This guaranties data security and prevents any disastrous mistake.

## scraping mode

This mode is meant to be used only by the SARC server, during data collection (aka. "scraping")

In this mode, the mongodb account has write permissions; therefore, use the `scraping mode` with caution.