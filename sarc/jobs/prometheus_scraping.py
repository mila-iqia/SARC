import logging
from datetime import datetime
from typing import Optional

from tqdm import tqdm

from sarc.client.job import SlurmJob, _jobs_collection, count_jobs, get_jobs
from sarc.config import ClusterConfig
from sarc.jobs.series import get_job_time_series
from sarc.traces import trace_decorator

logger = logging.getLogger(__name__)


@trace_decorator()
def scrap_prometheus(
    cluster: ClusterConfig,
    start: datetime,
    end: datetime,
) -> None:
    """Scrap Prometheus metrics for jobs fromm start to end and save it to database.

    Parameters
    ----------
    cluster: ClusterConfig
        The configuration of the cluster on which to fetch the jobs.
    start: datetime
        The datetime from which to fetch the jobs. Time matters.
    end: datetime
        The datetime up to which we fetch the jobs. Time matters.
    """
    collection = _jobs_collection()
    logger.info(
        f"Saving into mongodb collection '{collection.Meta.collection_name}'..."
    )
    job_args = {"cluster": cluster.name, "start": start, "end": end}
    nb_jobs = count_jobs(cluster=cluster.name, start=start, end=end)
    for entry in tqdm(
        get_jobs(cluster=cluster.name, start=start, end=end),
        total=nb_jobs,
        desc="get Prometheus metrics",
    ):
        update_allocated_gpu_type_from_prometheus(cluster, entry)
        saved = entry.statistics(recompute=True, save=True) is not None
        if not saved:
            collection.save_job(entry)
    logger.info(
        f"Saved Prometheus metrics for {nb_jobs} jobs on {cluster.name} from {start} to {end}."
    )


@trace_decorator()
def update_allocated_gpu_type_from_prometheus(
    cluster: ClusterConfig, entry: SlurmJob
) -> Optional[str]:
    """
    Try to infer job GPU type from Prometheus
    if cluster have configured a Prometheus connection.

    Parameters
    ----------
    cluster: ClusterConfig
        Cluster configuration for the current job.
    entry: SlurmJob
        Slurm job for which to infer the gpu type.

    Returns
    -------
    str
        String representing the gpu type.
    None
        Unable to infer gpu type.
    """
    gpu_type = None

    if cluster.prometheus_url:
        # Cluster does have prometheus config.
        output = get_job_time_series(
            job=entry,
            metric="slurm_job_utilization_gpu_memory",
            max_points=1,
            dataframe=False,
        )
        if output:
            gpu_type = output[0]["metric"]["gpu_type"]

    # If we found a GPU type, try to infer descriptive GPU name
    if gpu_type is not None:
        entry.allocated.gpu_type = (
            cluster.harmonize_gpu_from_nodes(entry.nodes, gpu_type) or gpu_type
        )

    return entry.allocated.gpu_type
