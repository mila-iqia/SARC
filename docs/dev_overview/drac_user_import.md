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

> [!WARNING]
> The cache file must be generated and uploaded between two automatic fetches on GCP for it to be parsed. If it is older than the last cache file parsed at the time of upload, it will not be taken into account by SARC! The ideal time to do this is between "x"h20 and "x"h45
