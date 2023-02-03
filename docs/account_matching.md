
# Quick guide on LDAP retrieval and account matching

- get the Mila user information from our LDAP using `read_mila_ldap.py`
- get the account information from DRAC by manually downloading two specific CSV files
- 

## Getting the information from the Mila LDAP

This command does two things, and we omit some arguments to only do one.
- It generates a file like the one found in `secrets/account_matching/2022-11-26_mila_users.json`.
- It updates the values in the database collection "users".

```
python3 read_mila_ldap.py \\
    --local_private_key_file secrets/Google_2026_01_26_66827.key \\
    --local_certificate_file secrets/Google_2026_01_26_66827.crt \\
    --ldap_service_uri ldaps://ldap.google.com \\
    --mongodb_connection_string ${MONGODB_CONNECTION_STRING} \\
    --output_json_file mila_users.json
```

The file is useful if we want to process to the manual task of matching accounts
with those from DRAC. The updates to the database are applied to "mila_ldap" of
the entries in the "users" collection.

## Match accounts from Compute Canada / DRAC

The script `sarc/account_matching/make_matches.py` takes data from 3 sources
and performs the account matching. That is, collection entries in "user"
have the form
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
and 