# systemd services

These service scripts go to `/etc/systemd/system/`

## scraper services

- Execute jobs scraper with `systemctl start sarc_scrap_jobs.service` (one-shot)
- Activate the timer with `systemctl start sarc_scrap_jobs.timer`
- Enable at boot with `systemctl enable sarc_scrap_jobs.timer`

## MongoDB service

***TODO***