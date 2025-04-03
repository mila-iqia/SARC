# Jobs scraping

During job scraping on a cluster, 2 passes are made:
- a `sacct` call for jobs informations during the specified time period
- (optional) a call to a `prometheus` server for each job to get more metrics

At the end, if everything wnt through without error, the field `end_date` of the cluster entry in the database is updated with the date of the last successful scraping. This is useful for the `-d auto` parameter of the `sarc acquire jobs` CLI command (see below). This way, the scraping is error-resilient, since an unsuccessful scraping, for any reason, will be retried the next day.

source code: [sarc/cli/acquire/jobs.py](../../sarc/cli/acquire/jobs.py)

## CLI
```
poetry sarc acquire jobs -d auto -c mila
poetry run sarc acquire jobs -d 2025-03-01 -c cedar narval
poetry run sarc acquire jobs -d 2025-03-01-2025-03-15 -c cedar narval

poetry sarc acquire jobs -d auto -c mila --no-prometheus
```

