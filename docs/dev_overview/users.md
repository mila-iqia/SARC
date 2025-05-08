# Users acquisition

## CLI

```
SARC_MODE=scraping uv run sarc acquire users
```

source code: [sarc/cli/acquire/users.py](../../sarc/cli/acquire/users.py)

The acocunt atching code meerges different sources of informations to match the mila credentials with the DRAC credentials:
- LDAP
- DRAC CSVs, manually downloaded and placed in the `../SARC_secrets/secrets/` folder
- mymila (deactivated until further notice)
- exception files (in the `../SARC_secrets/secrets/` folder, see [secrets](secrets.md) for more informations)

## Users revisions

In the database, each users entry has two optionnal fields: `record_start` and `record_end`. 

They are used to track user changes over time. 

In the sarc client API, the function `get_users` has a `latest=True` default parameter to filter out the outdated user records, whereas `get_user` always filters them out.

See [sarc/client/users/api.py](../../sarc/client/users/api.py)