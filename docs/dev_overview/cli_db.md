# DB initialisation

There are a few steps to create a new database.

1.  Setup the database structure:  
    `SARC_CONFIG=config_file.yaml alembic upgrade head`
2.  Import initial data from the config:  
    `SARC_CONFIG=config_file.yaml python -m sarc.db`
3.  (optional) Import the slurmconfig cache  
    `SARC_CONFIG=config_file.yaml sarc parse slurmconfig`
4.  (optional) Import the users cache  
    `SARC_CONFIG=config_file.yaml sarc parse users`
5.  (optional) Import the jobs cache (needs users, and ideally slurmconfig)  
    `SARC_CONFIG=config_file.yaml sarc parse jobs`
6.  (optional) Import the prometheus cache (needs jobs)  
    `SARC_CONFIG=config_file.yaml sarc parse prometheus`

Note that the importations, especially jobs and prometheus, take multiple days for our current cache.

If you just want to play around with test data, the first two steps and enough and should only take a few seconds.

# slurmconfig

To fetch new data into the cache for the specified clusters, run:

`SARC_CONFIG=config_file.yaml sarc fetch slurmconfig -c cluster1 cluster2 ...`

The clusters must be present in the configuration.

# users

To fetch users data into the cache according to what is in the config file, run:

`SARC_CONFIG=config_file.yaml sarc fetch users`

# jobs

To fetch job data into the cache for the specified clusters, in 60 minutes intervals (`-a 60`) with a maximum of 24 intervals (`--max-intervals 24`) run:

`SARC_CONFIG=config_file.yaml sarc fetch jobs -c cluster1 cluster2 ... -a 60 --max-intervals 24`

The automatic interval feature will fetch all completed intervals that are not already present in the cache (according to the timestamp recorded in the database for that cluster) for the specified clusters starting with the oldest. If there is no maximum, then it will fetch up to the most recent complete interval.

Note that to import the jobs into the database they must have a matching user, but they will be stored into the cache as raw data, regardless.

# prometheus

To fetch prometheus data for jobs, run:

`SARC_CONFIG=config_file.yaml sarc fetch prometheus -c cluster1 cluster2 ... --max_jobs 123 --after 2025-01-01`

This requires that the jobs are already present in the database and will fetch prometheus data for all jobs from each specified cluster that has no data and was submitted after 2025-01-01 (`--after 2025-01-01`). It will limit the fetch to the 123 (`--max_jobs 123`) oldest jobs. The fetch limit applies per-cluster.

Since sometimes jobs just don't have any data, it will be necessary to increment the after date to avoid trying to fetch data for older jobs repeatedly at the expense of newer jobs.
