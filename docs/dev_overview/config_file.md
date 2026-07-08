# Configuration file

The env var `SARC_CONFIG` can be used to specify wich config file to use; example:

```
% SARC_CONFIG=config/sarc-client.yaml uv run sarc health history
% SARC_CONFIG=config/sarc-dev.yaml uv run sarc acquire jobs -d auto -c mila
```

## Config reference

It contains links and configuration infos for various purpose:

- db credentials
- ldap credentials
- configuration settings for cluster users scraping
- clusters configurations
  - ssh credentials
  - (optionnal) prometheus credentials
  - various settings

see [sarc-ref.yaml](../../config/sarc-ref.yaml) for reference.

# Production config

The production config is available in the separate sarc-config repository and sensitive data inside that config is encrypted using serieux. The password for that is available in bitwarden.

If you just want to test things around you can build yourself a test config that points to a local database.
