# `secrets` folder

Some files need to be available for SARC to work properly. These files are private and cannot be stored on this repository.

They must be stored in the `../SARC_secrets/secrets/` folder. It is important to keep it outside the `SARC` repo clone, especially if you use a LLM assistant like `Cursor`, `Copilot` or whatever else could scan the entirety of the project folder.

Many of these files are referenced from the sarc-xxxx.json config file; example:

```json
    "ldap": {
        "local_private_key_file": "../SARC_secrets/secrets/ldap/Google_2026_01_26_66827.key",
        "local_certificate_file": "../SARC_secrets/secrets/ldap/Google_2026_01_26_66827.crt",
        "ldap_service_uri": "ldaps://ldap.google.com",
        "mongo_collection_name": "users",
        "group_to_prof_json_path": "../SARC_secrets/secrets/group_to_prof.json",
        "exceptions_json_path": "../SARC_secrets/secrets/exceptions.json"
    },
    "account_matching": {
        "drac_members_csv_path": "../SARC_secrets/secrets/account_matching/members-rrg-bengioy-ad-2022-11-25.csv",
        "drac_roles_csv_path": "../SARC_secrets/secrets/account_matching/sponsored_roles_for_Yoshua_Bengio_(CCI_jvb-000).csv",
        "make_matches_config": "../SARC_secrets/secrets/account_matching/make_matches_config.json"
    },
```
... or in the `clusters` section:
```json
        "narval": {
            (...)
            "prometheus_headers_file": "../SARC_secrets/secrets/drac_prometheus/headers.json",
            (...)
            "gpu_to_rgu_billing": "../SARC_secrets/secrets/gpu_to_rgu_billing_narval.json"
        }
```

For the exact function of these files, refer to source code.

## Where to find the `secrets/` folder

The folder is zipped in a `secrets.zip` and stored in the `bitwarden` safe of the team. It has to be updated when modifications are made in the folder (ie. update of the files, new files added for new feature, unnecessary files removal,...)

---
additionnal reference: [../secrets.md](../secrets.md)