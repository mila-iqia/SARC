# Scheduling

The scheduling is done using `systemd` scripts. Current status:

- daily tasks:
	- jobs + users scraping; 
		- systemd service: [sarc_scrapers.service](../../scripts/systemd/sarc_scrapers.service)
		- script: [scrapers.sh](../../scripts/systemd/scrapers.sh)
	- db backups (see [Backups](backups.md)); 
		- systemd service: [sarc_backup.service](../../scripts/systemd/sarc_backup.service)
		- script: [mongo_backup.sh](../../scripts/systemd/mongo_backup.sh)


Reference : [systemd services in deployment.md](../deployment.md#systemd-services)
