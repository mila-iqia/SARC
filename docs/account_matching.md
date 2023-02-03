
# Quick guide on LDAP retrieval and account matching

## Structure in MongoDB

There is a collection called "user" in the "sarc" database in MongoDB.

Entries in that collection have the following form.
```
    {"mila_ldap": {
      "mila_email_username": "john.appleseed@mila.quebec",
      "mila_cluster_username": "applej",
      ...
    },
    "cc_roles": None,
    "cc_members": {
        "username": "johns",
        ...}
```

We will explain the pipeline from Mila LDAP and CC reports to populate those entries.

## Getting the information from the Mila LDAP

```
python3 read_mila_ldap.py \
    --local_private_key_file secrets/Google_2026_01_26_66827.key \
    --local_certificate_file secrets/Google_2026_01_26_66827.crt \
    --ldap_service_uri ldaps://ldap.google.com \
    --mongodb_connection_string ${MONGODB_CONNECTION_STRING} \
    --output_json_file mila_users.json
```

This command has two effects:
- It updates the values in the database collection "users".
- It generates a file like the one found in `secrets/account_matching/2022-11-26_mila_users.json`.

When the `--mongodb_connection_string` argument is omitted, nothing happens with the database.

The file is useful if we want to process to the manual task of matching accounts
with those from DRAC. The updates to the database are applied to "mila_ldap" of
the entries in the "users" collection.

This can be run as a cron job.

## Match accounts from Compute Canada / DRAC

The script `sarc/account_matching/make_matches.py` takes data from 3 sources
and performs the account matching. It takes two specific CSV files obtained
through the DRAC web site, and it uses the output file from `read_mila_ldap.py`
to match the users in the most accurate way possible.

There are a lot of edge cases to be handled manually
when new accounts are encountered (e.g. people with names that differ)
so this shouldn't be run automatically as a cron job.

Many of the arguments for `make_matches.py` are written in the source code
and/or refer to files found in the `sarc/secrets` folder.
This script could be rewritten to avoid such a situation.

```
python3 sarc/account_matching/make_matches.py \
    --config_path secrets/account_matching/make_matches_config.json \
    --mila_ldap_path secrets/account_matching/2022-11-26_mila_users.json \
    --cc_members_path secrets/account_matching/members-rrg-bengioy-ad-2022-11-25.csv \
    --cc_roles_path secrets/account_matching/sponsored_roles_for_Yoshua_Bengio_(CCI_jvb-000).csv \
    --output_path matches_done.json \
    --mongodb_connection_string ${MONGODB_CONNECTION_STRING}
```

This script will update the entries in the database when the
argument `--mongodb_connection_string` is specified.
The output of that script is a file with a name like "matches_done.json".

YOU ARE HERE:
  - Implement those arguments in make_matches.py
  - Write to the database for make_matches.py
  - Go back to read_mila_ldap.py and write to the database in that case also.