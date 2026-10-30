# CSV import procedure

After adding new users to the DRAC allocation, the list must be retrieved from the CCDB website in order to inject it into SARC.

> [!NOTE]
> In our example, we will use the date 2026-08-20

- go fetch the CSV from [the ccdb dashboard of a user with rights on the benjioy allocation](https://ccdb.alliancecan.ca/) ("My Projects/Manage project membership" section > "Download CSV" button)
- place/rename it as `drac_members.csv`
- use a configuration file such as this one, which we will name `import_drac_csv.yaml`:

```yaml
sarc:
  db:
    host: whocares
    name: whocares
  patches: whocares
  cache: sarc-cache
  users:
    scrapers:
      drac_member:
        csv:
          $include:
            path: drac_members.csv
            format: txt
        csv_date: "2026-08-20"
```

- then use this file to fetch the users (locally); this will create a cache file including the CSV

```bash
SARC_MODE=scraping SARC_CONFIG=import_drac_csv.yaml uv run sarc fetch users
```

_A cache entry is thus created; in our case: `sarc-cache/users/2026/08/20/18:51:07.207`_

- upload this file to the SARC project's cache bucket on GCP

_in our case, on 2026-08-20, upload the file to [this directory](https://console.cloud.google.com/storage/browser/sarc-cache/users/2026/08/20); adapt the directory as needed._

- the newly created cache entry will be processed during the next `parse users` run (typically, it is processed each hour)

> [!WARNING]
> The cache file must be generated and uploaded between two automatic fetches on GCP for it to be parsed. If it is older than the last cache file parsed at the time of upload, it will not be taken into account by SARC! The ideal time to do this is between "x"h20 and "x"h45. You might want to check if the uploaded file is the last on the list in GCP cache bucket.

# Slurm allocation members import

Some DRAC users are not listed in the CCDB CSV: for example the members of the `aip-*` allocations on tamia. For them we only know the name, the cluster username, the allocations and sometimes the email address. They are imported with the `slurm_allocation_members` scraper, from a CSV with these columns:

```csv
Slurm_Username,Nom,Email,Allocations,PI
jdoe,John Doe,john.doe@mila.quebec,aip-jsmith,
```

Such a CSV can be generated with a script like `local/tamia-aip/generate_drac_users_aip.py`. Only `Slurm_Username` and `Nom` are used for the import, and `Email` when it is not empty: the allocations are not stored in SARC.

The procedure is the same as above, with this configuration section:

```yaml
sarc:
  users:
    scrapers:
      slurm_allocation_members:
        csv:
          $include:
            path: tamia_aip_members.csv
            format: txt
        # Optional, domain of the accounts (default: drac)
        domain: drac
```

How these members are matched with other users:

- A member is matched with the `drac_member` user having the same DRAC username (the `drac_member` scraper declares the username under the `slurm_allocation_members` ID).
- A member is matched with the user already holding this DRAC username in the database, if any (e.g. imported by `legacy_dump`).
- Members are not matched with LDAP users automatically: use the [user matching interface](user_matching.md).

> [!NOTE]
> The CSV gives no dates: the DRAC accounts of these members are considered valid with no start or end date. Members with no email are created with an empty email, which will be filled if another source provides it.
