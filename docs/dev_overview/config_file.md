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

# Changing the master password

`sarc encrypt rotate` re-encrypts a config and everything encrypted with the same master password using a new password:

```
SERIEUX_PASSWORD=<current password> uv run sarc encrypt rotate
```

The config defaults to `$SARC_CONFIG` (other files can be passed with `--file file1 file2 ...`). The new password is prompted for (or passed with `--new_password`). The command walks the config with serieux (all `Secret` fields, all included files, and encrypted values in otherwise untyped sections such as the user scrapers) and re-encrypts each value in place, preserving comments and formatting. The `sarc.client` section and whole-file encrypted files found in the config's `patches` directory (e.g. scraping `mapping.json` files) are rotated automatically; additional whole-file encrypted files can be passed with `--extra`. Use `--dry_run` to see what would be changed without writing anything. Everything is decrypted and verified before anything is written, so a wrong current password leaves all files untouched.

The config directory often holds several root files (files with a top-level `sarc:` key, e.g. `api.yaml`, `job.yaml`, `health_check.yaml`, ...) that share subfiles (`users.yaml`, the cluster files, ...). Those other roots are discovered automatically next to the given config and rotated in the same pass, so a shared file is re-encrypted exactly once and never decrypted with a stale password (pass `--single_root` to rotate only the given file). A root that fails to load (e.g. one referencing a file that does not exist here) is skipped with a warning; a failure on the main config aborts the whole rotation.
