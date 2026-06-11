# systemd services

These service scripts must be linked from `/etc/systemd/system/`

See [The deploy README](../deploy/README.md) for more info.

- Execute jobs scraper with `systemctl start sarc_scrapers.service` (one-shot)
- Activate the timer with `systemctl start sarc_scrapers.timer`
- Enable at boot with `systemctl enable sarc_scrapers.timer`

## Underusage notifications (`sarc_notify_underusage`)

Runs weekly (Monday 09:00) via `notify_underusage.sh`.
Posts the admin digest every week; researcher DMs are gated by `send_dms: false` in
the config — keep this value until the digest has been validated.

Enable:

```sh
systemctl enable --now sarc_notify_underusage.timer
```

Initial deploy: leave `send_dms: false` in `config/sarc-prod.yaml`. Flip to `true`
only after reviewing digest output for ~2 weeks.
