# systemd services

These service scripts must be linked from `/etc/systemd/system/`

See [The deploy README](../deploy/README.md) for more info.

- Execute jobs scraper with `systemctl start sarc_scrapers.service` (one-shot)
- Activate the timer with `systemctl start sarc_scrapers.timer`
- Enable at boot with `systemctl enable sarc_scrapers.timer`
