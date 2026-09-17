# Merging changed emails

When the user parse fails, the most likely cause is that someone changed their Mila email: the newly parsed user then conflicts with the one already stored, and the insert in the database fails.

To fix this, find the old and the new email addresses and record their association in the encrypted mapping file that lives in the config at `patches/scraping/mapping.json`.

## Finding the two emails

- The **old** email is the one already present in the database.
- The **new** email is the one in the first user-cache entry that failed the insert (i.e. the first entry after the last parsed date).
- The two entries usually share the same cluster username (`drac` or `mila`, or whatever is the source of the conflict).

> [!Tip]
> Once you have both addresses, double-check which one is the current one before recording the mapping.
> Useful sources to confirm it: the Gmail auto-complete suggestions, the [Mila directory](https://mila.quebec/en/directory), the users' Slack account emails, or any other trusted source.

## Recording the mapping

Add the pair to the mapping file with `sarc encrypt append`:

```bash
sarc encrypt append --path <config>/patches/scraping/mapping.json --key <old-email> --value <new-email>
```

This requires a valid [configuration file](config_file.md) and the encryption password available in the environment.
