import json
import logging
from datetime import UTC, datetime
from itertools import batched

from sqlalchemy.orm import joinedload
from sqlmodel import Session, col, select, tuple_

from sarc.cache import Cache, CacheEntry
from sarc.config import ClusterConfig, config
from sarc.db.cluster import SlurmClusterDB
from sarc.db.job import JobStatisticDB, JobStatisticsFetchDateDB, SlurmJobDB
from sarc.db.runstate import get_parsed_date, set_parsed_date
from sarc.models.job import SlurmState
from sarc.scraping import series
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def fetch_prometheus(
    sess: Session, cluster: ClusterConfig, after: datetime | None, max_jobs: int | None
) -> None:
    """
    Fetch Prometheus metrics for jobs on the specified cluster.
    """
    if cluster.name is None:
        logger.error("cluster name not set, can't fetch")
        return
    cluster_id = SlurmClusterDB.id_by_name(sess, cluster.name)
    if cluster_id is None:
        logger.error("Unknown cluster %, skipping cluster", cluster.name)
        return
    has_statistics = (
        select(JobStatisticDB.job_id)
        .where(JobStatisticDB.job_id == SlurmJobDB.id)
        .exists()
    )
    query = select(SlurmJobDB).where(
        SlurmJobDB.cluster_id == cluster_id,
        SlurmJobDB.elapsed_time != 0,
        SlurmJobDB.job_state != SlurmState.RUNNING,
        ~has_statistics,
    )
    was_attempted = (
        select(JobStatisticsFetchDateDB.job_id)
        .where(JobStatisticsFetchDateDB.job_id == SlurmJobDB.id)
        .exists()
    )
    query = query.where(~was_attempted)
    if after is not None:
        query = query.where(SlurmJobDB.submit_time >= after)
    query = query.order_by(col(SlurmJobDB.submit_time).desc())
    if max_jobs is not None:
        query = query.limit(max_jobs)
    nb_jobs = 0
    fetch_date_now = datetime.now(UTC)
    cache = Cache("prometheus")
    with cache.create_entry(fetch_date_now) as ce:
        for entry in sess.exec(query):
            assert entry.id is not None
            fetch_record = sess.exec(
                select(JobStatisticsFetchDateDB).where(
                    JobStatisticsFetchDateDB.job_id == entry.id
                )
            ).one_or_none()
            if fetch_record is None:
                sess.add(
                    JobStatisticsFetchDateDB(job_id=entry.id, fetch_date=fetch_date_now)
                )
            else:
                fetch_record.fetch_date = fetch_date_now
                fetch_record.jobstatistic_id = None
            raw_prom_data = series.get_job_time_series_data(
                job=entry, metric=series.JOB_STATISTICS_METRIC_NAMES, max_points=10_000
            )
            if raw_prom_data == []:
                continue
            nb_jobs += 1
            ce.add_value(
                f"{entry.cluster.name}${entry.job_id}${entry.submit_time.isoformat(timespec='seconds')}",
                json.dumps(raw_prom_data).encode("utf-8"),
            )
    sess.commit()
    logger.info(f"Fetched Prometheus metrics for {nb_jobs} jobs.")


@trace_decorator()
def parse_prometheus(since: datetime | None, update_parsed_date: bool) -> None:
    cache = Cache("prometheus")
    with config.db.session() as sess:
        if since is None:
            since = get_parsed_date(sess, "prometheus")
            if since is None:
                since = cache.oldest_year()

        cluster_ids: dict[str, int | None] = {}
        for ce in cache.read_from(from_time=since):
            error = parse_prometheus_ce(sess, ce, cluster_ids)
            if update_parsed_date and not error:
                logger.info(f"Set parsed_dates for jobs to {ce.get_entry_datetime()}.")
                set_parsed_date(sess, "prometheus", ce.get_entry_datetime())
            sess.commit()


PARSE_BATCH_SIZE = 100


def parse_prometheus_ce(
    sess: Session, ce: CacheEntry, cluster_ids: dict[str, int | None]
) -> bool:
    error = False
    nb_jobs = 0

    logger.info(
        f"Parsing prometheus data from cache entry: {ce.get_entry_datetime().isoformat(timespec='milliseconds')}"
    )
    for batch in batched(ce.items(), PARSE_BATCH_SIZE):
        batch_error, batch_nb_jobs = _parse_prometheus_batch(sess, batch, cluster_ids)
        error = error or batch_error
        nb_jobs += batch_nb_jobs

    logger.info(f"Saved Prometheus metrics for {nb_jobs} jobs.")
    return error


def _parse_prometheus_batch(
    sess: Session,
    batch: tuple[tuple[str, bytes], ...],
    cluster_ids: dict[str, int | None],
) -> tuple[bool, int]:
    error = False
    nb_jobs = 0

    # First pass: cheap, in-memory parsing/validation of every key in the batch,
    # so the DB is only hit once (below) for the jobs that are actually usable.
    parsed = []
    for key, value in batch:
        nb_jobs += 1
        cluster_name, job_id_str, submit_time_str = key.split("$")
        cluster = config.clusters.get(cluster_name, None)
        if cluster is None:
            logger.error("Could not find cluster '%s' in config", cluster_name)
            error = True
            continue
        job_id = int(job_id_str)
        submit_time = datetime.fromisoformat(submit_time_str).astimezone(UTC)
        data = json.loads(value.decode("utf-8"))
        if data == []:
            logger.warning(
                f"Empty data found for job {job_id} on cluster {cluster_name} (submit_time {submit_time}), skipping cache entry"
            )
            continue
        if cluster_name not in cluster_ids:
            cluster_ids[cluster_name] = SlurmClusterDB.id_by_name(sess, cluster_name)
        cluster_id = cluster_ids[cluster_name]
        if cluster_id is None:
            logger.error("Unknown cluster name %s in entry key %s", cluster_name, key)
            error = True
            continue
        parsed.append((key, cluster, cluster_id, job_id, submit_time, data))

    if not parsed:
        return error, nb_jobs

    refs = [
        (cluster_id, job_id, submit_time)
        for _, _, cluster_id, job_id, submit_time, _ in parsed
    ]
    entries_by_ref = {
        (job.cluster_id, job.job_id, job.submit_time): job
        for job in sess.exec(
            select(SlurmJobDB)
            .where(
                tuple_(
                    col(SlurmJobDB.cluster_id),
                    col(SlurmJobDB.job_id),
                    col(SlurmJobDB.submit_time),
                ).in_(refs)
            )
            .options(joinedload(SlurmJobDB.statistics))
        )
        .unique()
        .all()
    }

    updated_entries = []
    for key, cluster, cluster_id, job_id, submit_time, data in parsed:
        entry = entries_by_ref.get((cluster_id, job_id, submit_time))
        if entry is None:
            logger.error("Could not find job for %s", key)
            error = True
            continue
        if entry.allocated_gres_gpu is not None:
            # If it's a GPU job, get job GPU type from Prometheus.
            # NB: Will Prometheus even provide a GPU type for a CPU-only job?
            gpu_type = data[0]["metric"].get("gpu_type", None)
            if gpu_type is not None:
                entry.allocated_gpu_type = gpu_type
                entry.harmonized_gpu_type = cluster.harmonize_gpu_from_nodes(
                    entry.nodes, gpu_type
                )
        statistics = series.compute_job_statistics(entry, data)
        if len(statistics) != 0:
            for k, v in statistics.items():
                if (existing := entry.statistics.get(k)) is not None:
                    v.id = existing.id
                    v.job_id = existing.job_id
                    existing.sqlmodel_update(v)
                else:
                    entry.statistics[k] = v
            updated_entries.append(entry)

    if updated_entries:
        sess.flush()
        fetch_records_by_job_id = {
            fetch_record.job_id: fetch_record
            for fetch_record in sess.exec(
                select(JobStatisticsFetchDateDB).where(
                    col(JobStatisticsFetchDateDB.job_id).in_(
                        [entry.id for entry in updated_entries]
                    )
                )
            )
        }
        for entry in updated_entries:
            fetch_record = fetch_records_by_job_id.get(entry.id)
            if fetch_record is not None:
                any_stat = next(iter(entry.statistics.values()))
                fetch_record.jobstatistic_id = any_stat.id

    return error, nb_jobs
