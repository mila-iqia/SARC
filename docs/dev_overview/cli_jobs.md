# Jobs scraping

During job scraping on a cluster, 2 passes are made:
- a `sacct` call for jobs informations during the specified time period
- (optional) a call to a `prometheus` server for each job to get more metrics

source code: [sarc/cli/acquire/jobs.py](../../sarc/cli/acquire/jobs.py)

## CLI
```
poetry sarc acquire jobs -d auto -c mila
poetry run sarc acquire jobs -d 2025-03-01 -c cedar narval
poetry run sarc acquire jobs -d 2025-03-01-2025-03-15 -c cedar narval

poetry sarc acquire jobs -d auto -c mila --no-prometheus
```
