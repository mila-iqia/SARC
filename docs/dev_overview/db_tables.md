# Database tables

The shape of the PostgreSQL schema. For the current columns themselves,
read the SQLModel class in `sarc/db/` or `\d <table>` in psql.

The diagram is generated from the SQLModel metadata by
`docs/_generate_db_er.py`; re-run it when the tables change.

```{mermaid} db_schema.mmd
```

`users`, `clusters` and `slurm_jobs` are the three hubs the rest of the schema
hangs off — except `parsedates` and `healthcheckstatedb`, which stand alone.

## Jobs

`slurm_jobs` is one row per job, as scraped from `sacct`. `jobstatisticdb` is
one row per (job, metric name), from Prometheus. `jobstatistics_fetchdate` is
one row per job, written when a fetch is *attempted* and whether or not
Prometheus answered with anything: the next `sarc fetch prometheus` skips every
job that already has one, so a job with no metrics is not retried forever —
those are the rows whose `jobstatistic_id` stays NULL.

`job_series` is the read model: the same jobs with the users, clusters,
`gpurgudb` and statistics joins already resolved and the RGU/cost/waste
arithmetic precomputed, one wide row per job. It is not a cache — triggers
maintain it AFTER ROW in the same transaction as the write to `slurm_jobs`, so
a reader can only see it lag inside an in-flight transaction.
`sarc db backfill-series` repairs or fills it, idempotently.

`job_series_view` is absent from the diagram because it is a view, not a table.
It wraps `job_series` and adds only `member_type`, `supervisors` and the
`usage_metric` alias. Those two user attributes stay read-time subqueries
because they resolve validity ranges that the user scrapers edit
retroactively (see below), which materializing would turn into cascading
updates.

## Users

`users` holds three columns: `id`, `display_name`, `email`. Everything else
about a user is time-varying and lives in its own table — `credentialsdb`
(cluster accounts), `membertypedb`, `user_supervisors` — each carrying a
`valid` TSTZRANGE and a GiST exclusion constraint that forbids two overlapping
rows for the same user. An attribute is therefore a history, not a column, and
asking for its value means asking at a date.

Supervisors need two tables because the list is ordered: `user_supervisors` is
the time-ranged row, `supervisorshelper` holds one supervisor per `pos`.

`matchingid` maps a SARC user to their identifier in each source
(`plugin_name`, `match_id`), unique in both directions. It is what lets LDAP,
DRAC and MyMila records converge on one `users` row.

`user_periods` is not a user attribute like the tables above: a row is one
(user, cluster)'s GPU usage over one notification cycle — the
`usage_cycle_length_weeks` window that `end_date` closes — with the underuse
verdicts already decided. It is derived data on a rolling window: `sarc usage
refresh-store` recomputes the `history_cycles` most recent cycles and prunes
everything older, and `sarc usage notify` reads it by default rather than
replaying those queries over `job_series` (`--ignore-store` recomputes live).
It also feeds PowerBI.

## Clusters

`clusters` also carries the scraping watermarks `end_time_sacct` and
`end_time_prometheus`, which `init_insert()` syncs from the cache.

`gpubillingdb` and `nodegpumappingdb` are `since`-stamped JSONB snapshots
rather than normalized rows: each is a whole mapping as it stood at a date.

`gpurgudb` is keyed by GPU name, which is why `slurm_jobs.harmonized_gpu_type`
is a foreign key on a string rather than on an id.

## Disk usage

A three-level tree: `diskusage_reports` (one per cluster and timestamp) →
`diskusage_groups` → `diskusage_users`. That grain is a convention, not a
constraint: the `(cluster_id, timestamp)` index is not unique and `sarc parse
diskusage` plainly inserts, so re-parsing a window duplicates its reports.

## Allocations

`allocationdb` holds DRAC allocations, one row per cluster, resource, group and
allocation period: `start`/`end` are part of what identifies a row, so a renewed
allocation lands as a new row rather than an update. It is fed by hand —
`sarc fetch allocations --file <csv>` then `sarc parse allocations` — and
nothing in SARC reads it back yet.

## Run state

`parsedates` and `healthcheckstatedb` have no foreign keys, hence their lone
boxes on the diagram. The first is one resume watermark per source name, read
by `sarc parse` when `--start` is omitted. The second is one row per check: its
last result and message, next to a serialized copy of its configuration that
every run rewrites from the config file — never the source of truth, but what
`sarc health list` prints. That stored state is what lets `sarc health run`
work from cron without a daemon, and what it reads to skip a check whose
`depends` did not last come back OK.
