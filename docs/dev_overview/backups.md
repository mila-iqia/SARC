# Backups

## Backup policy

On the production `sarc01-dev` machine, a snapshot is made of the database every day.
This snapshot is kept for 28 days (4 weeks) in the to limit disk usage.

A manual operation is needed if we want to keep db snapshots longer, to download them to a secure place.

## Backup script

the backup scritps are located in `script/systemd/`:
- [mongo_backup.sh](../../scripts/systemd/mongo_backup.sh)
- [sarc_backup.service](../../scripts/systemd/sarc_backup.service)

reference: [Scheduling](scheduling.md)